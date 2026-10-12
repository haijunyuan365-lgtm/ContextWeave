"""Lightweight, bounded Span -> Prometheus projection; no export/network IO here."""
from collections import OrderedDict
from dataclasses import dataclass, replace
import logging
import math
import re
import threading

from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.trace import StatusCode
from prometheus_client import REGISTRY, Counter, Gauge, Histogram

logger = logging.getLogger(__name__)
LATENCY_BUCKETS_MS = (1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000)
RAG_SPANS = {"rag.pipeline", "rag.rewrite", "rag.query", "rag.hybrid", "rag.dense", "rag.bm25",
             "rag.rrf.stage1", "rag.rrf.stage2", "rag.mmr", "rag.rerank"}
OUTCOMES = {"success", "error", "degraded", "skipped"}


@dataclass
class AgentMetricsSnapshot:
    total: int = 0
    success: int = 0
    total_ms: float = 0.0

    def legacy_view(self):
        return {"total": self.total, "success_rate": round(self.success / self.total, 3) if self.total else 1.,
                "avg_ms": round(self.total_ms / self.total, 1) if self.total else 0.}


@dataclass
class ToolMetricsSnapshot:
    total: int = 0
    success: int = 0
    failed: int = 0
    total_ms: float = 0.0
    consecutive_fails: int = 0
    circuit_state: str = "closed"
    version: int = -1

    def legacy_view(self):
        return {"total": self.total, "success_rate": round(self.success / self.total, 3) if self.total else 1.,
                "avg_latency_ms": round(self.total_ms / self.total, 1) if self.total else 0.,
                "consecutive_fails": self.consecutive_fails, "circuit_state": self.circuit_state}


class MetricsProjection:
    def __init__(self, registry=REGISTRY, *, max_owners=64, max_labels=64):
        self._lock = threading.RLock()
        self._max_owners = max_owners
        self._max_labels = max_labels
        self._labels = {}
        self._agents = OrderedDict()
        self._tools = OrderedDict()
        self.failures = 0
        self.capture_enabled = Gauge("contextweave_metrics_capture_enabled", "1 when full local Span capture is available", registry=registry)
        self.capture_enabled.set(1)
        self.compatibility = {
            "requests_total": Counter("requests_total", "Completed accepted chat requests", registry=registry),
            "agent_latency_ms": Histogram("agent_latency_ms", "Each actual Agent Span duration in ms", ["agent"], buckets=LATENCY_BUCKETS_MS, registry=registry),
            "agent_success_rate": Gauge("agent_success_rate", "Agent success rate (legacy definition)", ["agent"], registry=registry),
            "agent_avg_ms": Gauge("agent_avg_ms", "Cumulative Agent average latency in ms", ["agent"], registry=registry),
            "tool_success_rate": Gauge("tool_success_rate", "Tool success rate (legacy definition)", ["tool"], registry=registry),
        }
        self.requests = Counter("contextweave_requests_total", "Completed chat pipelines by business outcome", ["outcome"], registry=registry)
        self.request_duration = Histogram("contextweave_request_duration_ms", "Chat processing including memory persistence", buckets=LATENCY_BUCKETS_MS, registry=registry)
        self.agents = Counter("contextweave_agent_calls_total", "Actual Agent executions", ["agent", "outcome"], registry=registry)
        self.tools = Counter("contextweave_tool_calls_total", "Tool invocations; agent and MCP layers are separate", ["tool", "layer", "outcome"], registry=registry)
        self.tool_duration = Histogram("contextweave_tool_duration_ms", "Actual tool call duration", ["tool", "layer"], buckets=LATENCY_BUCKETS_MS, registry=registry)
        self.tool_results = Counter("contextweave_tool_results_total", "Technical versus business tool results", ["tool", "layer", "dimension", "success"], registry=registry)
        self.cache_hits = Counter("contextweave_tool_cache_hits_total", "Tool cache hits", ["tool", "layer"], registry=registry)
        self.fallbacks = Counter("contextweave_tool_fallbacks_total", "Tool fallback invocations", ["tool", "layer"], registry=registry)
        self.rag_calls = Counter("contextweave_rag_calls_total", "RAG executions by stage", ["stage", "outcome"], registry=registry)
        self.rag_duration = Histogram("contextweave_rag_duration_ms", "RAG stage duration", ["stage"], buckets=LATENCY_BUCKETS_MS, registry=registry)
        self.rag_candidates = Histogram("contextweave_rag_candidates", "Candidate counts per RAG stage", ["stage", "direction"], buckets=(0, 1, 3, 5, 10, 20, 40, 80, 160, 320), registry=registry)
        self.rag_empty = Counter("contextweave_rag_empty_results_total", "Completed stages with zero output candidates", ["stage"], registry=registry)
        self.llm_calls = Counter("contextweave_llm_calls_total", "Actual messages.create invocations", ["provider", "model", "stage", "outcome"], registry=registry)
        self.llm_duration = Histogram("contextweave_llm_duration_ms", "Actual LLM call duration", ["provider", "model", "stage"], buckets=LATENCY_BUCKETS_MS, registry=registry)
        self.tokens = Counter("contextweave_llm_tokens_total", "Known returned usage; cache tokens remain separate", ["provider", "model", "stage", "type"], registry=registry)
        self.usage_known = Counter("contextweave_llm_usage_known_total", "Calls with a known usage value, including known zero", ["provider", "model", "stage", "type"], registry=registry)
        self.context_calls = Counter("contextweave_context_calls_total", "Context source lookups", ["source", "outcome"], registry=registry)
        self.context_duration = Histogram("contextweave_context_duration_ms", "Context source lookup duration", ["source"], buckets=LATENCY_BUCKETS_MS, registry=registry)
        self.projection_errors = Counter("contextweave_metrics_projection_errors_total", "Projection failures, isolated from business code", registry=registry)
        self.export_errors = Counter("contextweave_trace_export_failures_total", "Failed trace batches; local metrics remain available", registry=registry)
        self.reconciliation = Gauge("contextweave_stats_reconciliation_mismatches", "Latest mismatched legacy/projection rows", ["component"], registry=registry)

    def label(self, dimension, value):
        """Bound both label length and the number of unique configured values."""
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,96}", value):
            return "unknown"
        with self._lock:
            values = self._labels.setdefault(dimension, set())
            if value not in values and len(values) >= self._max_labels:
                return "other"
            values.add(value)
        return value

    @staticmethod
    def _number(value):
        return type(value) in (int, float) and math.isfinite(value) and value >= 0

    def _owner(self, table, owner):
        if not isinstance(owner, str) or not owner:
            return None
        if owner not in table:
            if len(table) >= self._max_owners:
                table.popitem(last=False)
            table[owner] = {}
        return table[owner]

    def _row(self, rows, key, factory):
        if key not in rows and len(rows) >= self._max_labels:
            return None
        return rows.setdefault(key, factory())

    def snapshots(self, agent_owner, tool_owner):
        with self._lock:
            agents = {k: replace(v).legacy_view() for k, v in self._agents.get(agent_owner, {}).items()}
            tools = {k: replace(v).legacy_view() for k, v in self._tools.get(tool_owner, {}).items()}
        return agents, tools

    def record(self, span):
        attrs = span.attributes or {}
        if span.end_time is None or span.start_time is None:
            return
        duration = max(0., (span.end_time - span.start_time) / 1_000_000)
        outcome = attrs.get("app.outcome")
        if outcome not in OUTCOMES:
            outcome = "error" if span.status.status_code == StatusCode.ERROR else "success"
        with self._lock:
            if span.name == "chat.pipeline":
                self.compatibility["requests_total"].inc()
                self.requests.labels(outcome).inc()
                self.request_duration.observe(duration)
            elif span.name == "agent.execute":
                agent_key = attrs.get("app.agent.instance") or attrs.get("app.agent.name", "unknown")
                agent = self.label("agent", agent_key)
                self.agents.labels(agent, outcome).inc()
                self.compatibility["agent_latency_ms"].labels(agent).observe(duration)
                rows = self._owner(self._agents, attrs.get("app.agent.monitor_owner"))
                if rows is not None and attrs.get("app.agent.stats_counted") is True:
                    row = self._row(rows, agent_key, AgentMetricsSnapshot)
                    if row is not None:
                        row.total += 1
                        row.success += int(attrs.get("app.agent.success") is True)
                        row.total_ms += attrs.get("app.agent.stats_latency_ms", 0.)
            elif span.name == "tool.execute":
                tool_key = attrs.get("app.tool.name", "unknown")
                tool = self.label("tool", tool_key)
                layer = attrs.get("app.tool.layer", "unknown")
                layer = layer if layer in {"agent", "mcp"} else "unknown"
                self.tools.labels(tool, layer, outcome).inc()
                self.tool_duration.labels(tool, layer).observe(duration)
                for dimension, key in (("technical", "app.tool.success"), ("business", "app.tool.result_success")):
                    if type(attrs.get(key)) is bool:
                        self.tool_results.labels(tool, layer, dimension, str(attrs[key]).lower()).inc()
                if attrs.get("app.cache_hit") is True:
                    self.cache_hits.labels(tool, layer).inc()
                if attrs.get("app.fallback_used") is True:
                    self.fallbacks.labels(tool, layer).inc()
                rows = self._owner(self._tools, attrs.get("app.tool.monitor_owner")) if layer == "mcp" else None
                if rows is not None and attrs.get("app.tool.stats_registered") is True:
                    row = self._row(rows, tool_key, ToolMetricsSnapshot)
                    if row is not None:
                        row.total += attrs.get("app.tool.stats_total", 0)
                        row.success += attrs.get("app.tool.stats_success", 0)
                        row.failed += attrs.get("app.tool.stats_failed", 0)
                        row.total_ms += attrs.get("app.tool.stats_latency_ms", 0.)
                        version = attrs.get("app.tool.stats_version", -1)
                        if version >= row.version:
                            row.version = version
                            row.consecutive_fails = attrs.get("app.tool.stats_consecutive_fails", row.consecutive_fails)
                            row.circuit_state = attrs.get("app.tool.stats_circuit_state", row.circuit_state)
            elif span.name in RAG_SPANS:
                self.rag_calls.labels(span.name, outcome).inc()
                self.rag_duration.labels(span.name).observe(duration)
                for direction in ("input", "output"):
                    value = attrs.get("app.rag." + direction + "_count")
                    if self._number(value):
                        self.rag_candidates.labels(span.name, direction).observe(value)
                if attrs.get("app.rag.output_count") == 0:
                    self.rag_empty.labels(span.name).inc()
            elif span.name == "llm.generate":
                provider = self.label("provider", attrs.get("gen_ai.provider.name", "unknown"))
                model = self.label("model", attrs.get("gen_ai.response.model") or attrs.get("gen_ai.request.model", "unknown"))
                stage = self.label("stage", attrs.get("app.llm.stage", "unknown"))
                self.llm_calls.labels(provider, model, stage, outcome).inc()
                self.llm_duration.labels(provider, model, stage).observe(duration)
                fields = {"input": "gen_ai.usage.input_tokens", "output": "gen_ai.usage.output_tokens",
                          "cache_read": "app.llm.cache_read_tokens", "cache_write": "app.llm.cache_write_tokens"}
                for kind, key in fields.items():
                    value = attrs.get(key)
                    if type(value) is int and value >= 0:
                        self.tokens.labels(provider, model, stage, kind).inc(value)
                        self.usage_known.labels(provider, model, stage, kind).inc()
            elif span.name == "context.source.retrieve":
                source = attrs.get("app.context.source", "unknown")
                if source not in {"memory", "profile", "knowledge", "experience"}:
                    source = "unknown"
                self.context_calls.labels(source, outcome).inc()
                self.context_duration.labels(source).observe(duration)


class MetricsSpanProcessor(SpanProcessor):
    def __init__(self, projection):
        self.projection = projection

    def on_start(self, span, parent_context=None):
        pass

    def on_end(self, span):
        try:
            self.projection.record(span)
        except Exception:
            self.projection.failures += 1
            self.projection.projection_errors.inc()
            logger.warning("metrics_projection_failed")

    def shutdown(self):
        pass

    def force_flush(self, timeout_millis=30000):
        return True


_projection = None
_projection_lock = threading.Lock()


def get_metrics_projection():
    global _projection
    with _projection_lock:
        if _projection is None:
            _projection = MetricsProjection()
        return _projection
