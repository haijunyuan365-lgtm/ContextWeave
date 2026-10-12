"""P2: exact per-completion metrics, legacy reconciliation and real loopback OTLP IO."""
import asyncio
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, StatusCode
from prometheus_client import CollectorRegistry, generate_latest

from api import main
from agents.agent_orchestrator import AgentOrchestrator, AgentType, Request
from core.context_planner import ContextPlanner, LightweightContext
from core.context_runtime import ContextPipeline
from core.intent_recognizer import IntentCategory, IntentResult, UrgencyLevel
from mcp.tool_manager import MCPToolManager, Tool, CircuitState
from monitor.performance_monitor import PerformanceMonitor
from observability import bootstrap, observed, request_context
from observability.bootstrap import SafeSpanExporter
from observability.metrics_projection import MetricsProjection, MetricsSpanProcessor


@pytest.fixture
def metrics(monkeypatch):
    registry = CollectorRegistry()
    projection = MetricsProjection(registry)
    provider = TracerProvider()
    provider.contextweave_metrics_enabled = True
    provider.add_span_processor(MetricsSpanProcessor(projection))
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(bootstrap, "_provider", provider)
    monkeypatch.setattr(bootstrap, "get_metrics_projection", lambda: projection)
    yield SimpleNamespace(registry=registry, projection=projection, provider=provider, exporter=exporter)
    provider.shutdown()


def sample(metrics, name, labels=None):
    return metrics.registry.get_sample_value(name, labels or {}) or 0


def fake_client(text="answer"):
    return SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(return_value=SimpleNamespace(
        content=[{"type": "text", "text": text}], usage={"input_tokens": 12, "output_tokens": 3}))))


def services():
    orch = AgentOrchestrator("test-key", model="test-model")
    for agents in orch._pool.values():
        agents[0]._client = fake_client()
    manager = MCPToolManager("test-key", model="test-model")
    return orch, manager


def test_n_completions_histograms_and_request_counts_are_n(metrics):
    for duration in (100., 200., 500.):
        span = metrics.provider.get_tracer("test").start_span("chat.pipeline", start_time=1_000_000_000)
        span.end(end_time=1_000_000_000 + int(duration * 1_000_000))
    assert sample(metrics, "requests_total") == 3
    assert sample(metrics, "contextweave_requests_total", {"outcome": "success"}) == 3
    assert sample(metrics, "contextweave_request_duration_ms_count") == 3
    assert sample(metrics, "contextweave_request_duration_ms_sum") == 800


def test_histogram_uses_actual_agent_span_duration_not_legacy_average(metrics):
    tracer = metrics.provider.get_tracer("test")
    for duration in (50, 200, 5000):
        span = tracer.start_span("agent.execute", attributes={"app.agent.name": "technical", "app.agent.stats_latency_ms": 1}, start_time=1_000_000_000)
        span.end(end_time=1_000_000_000 + duration * 1_000_000)
    assert sample(metrics, "agent_latency_ms_count", {"agent": "technical"}) == 3
    assert sample(metrics, "agent_latency_ms_sum", {"agent": "technical"}) == 5250
    assert sample(metrics, "agent_latency_ms_bucket", {"agent": "technical", "le": "250.0"}) == 2


def test_agent_and_tool_projection_match_existing_success_and_timer_definitions(metrics):
    orch, manager = services()
    async def handler(params, context):
        return {"success": False} if params.get("business_fail") else ["ok"]
    tool = Tool("test_tool", "", handler, {}, cache_ttl=60)
    manager.register(tool)
    async def run():
        for _ in range(3):
            await orch.run(Request("hello", "u", "c", intent=IntentCategory.GREETING))
        await manager.call("test_tool", {})
        await manager.call("test_tool", {})
        await manager.call("test_tool", {"business_fail": True})
    asyncio.run(run())
    agents, tools = metrics.projection.snapshots(orch._metrics_owner, manager._metrics_owner)
    assert agents["general_0"] == {k: orch.get_stats()["general_0"][k] for k in ("total", "success_rate", "avg_ms")}
    assert tools["test_tool"]["total"] == 3
    for field in ("success_rate", "avg_latency_ms", "consecutive_fails", "circuit_state"):
        assert tools["test_tool"][field] == manager.get_stats()["test_tool"][field]
    assert tools["test_tool"]["success_rate"] == 1.0  # legacy technical success
    assert sample(metrics, "contextweave_tool_calls_total", {"tool": "test_tool", "layer": "mcp", "outcome": "error"}) == 1


def test_monitor_repeated_collection_does_not_observe_more_histogram_samples(metrics):
    orch, manager = services()
    asyncio.run(orch.run(Request("hello", "u", "c", intent=IntentCategory.GREETING)))
    monitor = PerformanceMonitor(orch, manager, metrics_projection=metrics.projection)
    before = sample(metrics, "agent_latency_ms_count", {"agent": "general_0"})
    async def collect():
        for _ in range(10):
            await monitor._collect()
    asyncio.run(collect())
    assert before == 1 and sample(metrics, "agent_latency_ms_count", {"agent": "general_0"}) == 1
    assert monitor.summary()["stats_reconciliation"]["agent"]["general_0"] == {
        "match": True, "source": "span", "span": metrics.projection.snapshots(orch._metrics_owner, manager._metrics_owner)[0]["general_0"]}
    assert sample(metrics, "agent_avg_ms", {"agent": "general_0"}) == orch.get_stats()["general_0"]["avg_ms"]


@pytest.mark.parametrize("mode,selected", [("span", "span"), ("legacy", "legacy"), ("shadow", "legacy")])
def test_monitor_modes_preserve_routing_penalties(metrics, mode, selected):
    orch, manager = services()
    orch._pool[AgentType.GENERAL][0]._client.messages.create.side_effect = RuntimeError("private failure")
    asyncio.run(orch.run(Request("hello", "u", "c", intent=IntentCategory.GREETING)))
    monitor = PerformanceMonitor(orch, manager, metrics_projection=metrics.projection, stats_source=mode)
    old = orch.get_stats()["general_0"]
    asyncio.run(monitor._collect())
    assert orch.get_stats()["general_0"]["monitor_penalty"] == monitor._routing_penalty(old["success_rate"], old["avg_ms"])
    assert monitor.summary()["stats_reconciliation"]["agent"]["general_0"]["source"] == selected
    assert monitor.summary()["active_alerts"]


def test_reconciliation_mismatch_falls_back_without_changing_route_feedback(metrics):
    orch, manager = services()
    orch._pool[AgentType.GENERAL][0].stats.total = 20
    orch._pool[AgentType.GENERAL][0].stats.success = 10
    orch._pool[AgentType.GENERAL][0].stats.total_ms = 100_000
    monitor = PerformanceMonitor(orch, manager, metrics_projection=metrics.projection)
    asyncio.run(monitor._collect())
    report = monitor.summary()["stats_reconciliation"]["agent"]["general_0"]
    assert not report["match"] and report["source"] == "legacy"
    assert orch.get_stats()["general_0"]["monitor_penalty"] == monitor._routing_penalty(.5, 5000)
    assert sample(metrics, "contextweave_stats_reconciliation_mismatches", {"component": "agent"}) == 1
    assert monitor.summary()["suggestions"]


def test_in_flight_request_temporarily_uses_legacy_then_matches_projection(metrics):
    orch, manager = services()
    async def run():
        started, finish = asyncio.Event(), asyncio.Event()
        async def create(**kwargs):
            started.set()
            await finish.wait()
            return SimpleNamespace(content=[{"type": "text", "text": "ok"}])
        orch._pool[AgentType.GENERAL][0]._client.messages.create.side_effect = create
        monitor = PerformanceMonitor(orch, manager, metrics_projection=metrics.projection)
        task = asyncio.create_task(orch.run(Request("hello", "u", "c", intent=IntentCategory.GREETING)))
        await started.wait()
        assert monitor.summary()["stats_reconciliation"]["agent"]["general_0"]["source"] == "legacy"
        finish.set()
        await task
        assert monitor.summary()["stats_reconciliation"]["agent"]["general_0"]["source"] == "span"
    asyncio.run(run())


def test_owner_isolation_and_two_instances_of_same_agent_type(metrics):
    orch, manager = services()
    other, _ = services()
    from agents.agent_orchestrator import GeneralAgent
    extra = GeneralAgent(fake_client(), "other-model")
    orch._pool[AgentType.GENERAL].append(extra)
    async def run():
        req = Request("hello", "u", "c", intent=IntentCategory.GREETING)
        await orch._handle_agent(req, orch._pool[AgentType.GENERAL][0], AgentType.GENERAL)
        await orch._handle_agent(req, extra, AgentType.GENERAL)
        await other.run(Request("hello", "u", "c", intent=IntentCategory.GREETING))
    asyncio.run(run())
    rows, _ = metrics.projection.snapshots(orch._metrics_owner, manager._metrics_owner)
    assert rows["general_0"]["total"] == rows["general_1"]["total"] == 1
    assert metrics.projection.snapshots(other._metrics_owner, None)[0]["general_0"]["total"] == 1
    assert PerformanceMonitor(orch, manager, metrics_projection=metrics.projection).summary()["stats_reconciliation"]["agent"]["general_1"]["match"]


def test_tool_timeout_cache_breaker_and_slow_fallback_reconcile(metrics):
    _, manager = services()
    first_failed = asyncio.Event()
    release_fallback = asyncio.Event()
    async def handler(params, context):
        if params["fail"]:
            raise RuntimeError("private error")
        return "ok"
    async def fallback(*args):
        first_failed.set()
        await release_fallback.wait()
        return "fallback"
    tool = Tool("tool", "", handler, {}, cache_ttl=60, fallback=fallback)
    manager.register(tool)
    async def run():
        first = asyncio.create_task(manager.call("tool", {"fail": True}))
        await first_failed.wait()
        await manager.call("tool", {"fail": False})
        release_fallback.set()
        await first
        await manager.call("tool", {"fail": False})
    asyncio.run(run())
    row = metrics.projection.snapshots(None, manager._metrics_owner)[1]["tool"]
    old = manager.get_stats()["tool"]
    assert row["total"] == tool.stats.total == 3
    assert all(row[k] == old[k] for k in old)
    assert row["consecutive_fails"] == 0
    assert sample(metrics, "contextweave_tool_fallbacks_total", {"tool": "tool", "layer": "mcp"}) == 1
    assert sample(metrics, "contextweave_tool_cache_hits_total", {"tool": "tool", "layer": "mcp"}) == 1


def test_breaker_rejection_does_not_inflate_legacy_tool_total(metrics):
    _, manager = services()
    tool = Tool("tool", "", AsyncMock(side_effect=RuntimeError("down")), {}, fallback=lambda *args: "fallback")
    tool.breaker.threshold = 1
    manager.register(tool)
    async def run():
        await manager.call("tool", {})
        await manager.call("tool", {})
    asyncio.run(run())
    row = metrics.projection.snapshots(None, manager._metrics_owner)[1]["tool"]
    assert row["total"] == tool.stats.total == 1 and row["consecutive_fails"] == 1
    assert row["circuit_state"] == "open"
    assert sample(metrics, "contextweave_tool_calls_total", {"tool": "tool", "layer": "mcp", "outcome": "degraded"}) == 2


def test_llm_unknown_usage_does_not_manufacture_zero_samples(metrics):
    from observability.llm import generate
    model = fake_client()
    model.messages.create.side_effect = [SimpleNamespace(content=[], usage=None),
        SimpleNamespace(content=[], usage={"input_tokens": 0, "output_tokens": 5, "cache_read_input_tokens": 3})]
    async def run():
        for _ in range(2):
            await generate(model, stage="agent", model="configured", messages=[])
    asyncio.run(run())
    labels = {"provider": "anthropic", "model": "configured", "stage": "agent"}
    assert sample(metrics, "contextweave_llm_calls_total", {**labels, "outcome": "success"}) == 2
    assert sample(metrics, "contextweave_llm_usage_known_total", {**labels, "type": "input"}) == 1
    assert sample(metrics, "contextweave_llm_tokens_total", {**labels, "type": "output"}) == 5
    assert metrics.registry.get_sample_value("contextweave_llm_tokens_total", {**labels, "type": "cache_write"}) is None


def test_rag_context_metrics_and_tool_layers_are_separate(metrics):
    from observability.instrumentation import annotate, outcome
    with observed("tool.execute", span_type="tool", **{"app.tool.name": "search", "app.tool.layer": "agent"}):
        with observed("tool.execute", span_type="tool", **{"app.tool.name": "search", "app.tool.layer": "mcp"}):
            with observed("rag.rerank", span_type="retrieval"):
                annotate(**{"app.rag.input_count": 10, "app.rag.output_count": 0})
                outcome("degraded", fallback=True)
    with observed("context.source.retrieve", **{"app.context.source": "knowledge"}):
        pass
    assert sample(metrics, "contextweave_tool_calls_total", {"tool": "search", "layer": "agent", "outcome": "success"}) == 1
    assert sample(metrics, "contextweave_tool_calls_total", {"tool": "search", "layer": "mcp", "outcome": "success"}) == 1
    assert sample(metrics, "contextweave_rag_candidates_sum", {"stage": "rag.rerank", "direction": "input"}) == 10
    assert sample(metrics, "contextweave_rag_empty_results_total", {"stage": "rag.rerank"}) == 1
    assert sample(metrics, "contextweave_context_calls_total", {"source": "knowledge", "outcome": "success"}) == 1


def test_projection_error_is_isolated_from_business(metrics):
    metrics.projection.record = Mock(side_effect=RuntimeError("private error"))
    with observed("chat.pipeline"):
        result = "business completed"
    assert result == "business completed"
    assert metrics.projection.failures == 1
    assert sample(metrics, "contextweave_metrics_projection_errors_total") == 1


def test_labels_exclude_request_identity_and_are_bounded(metrics):
    projection = MetricsProjection(CollectorRegistry(), max_labels=2, max_owners=2)
    for value in ("one", "two", "three", "user@example.com", "private question"):
        with request_context("private-request"), observed("llm.generate", **{
            "gen_ai.request.model": value, "app.request_id": "private-request", "app.user_id": "private-user"}) as span:
            pass
        projection.record(metrics.exporter.get_finished_spans()[-1])
    output = generate_latest(metrics.registry).decode()
    assert "private-request" not in output and "private-user" not in output
    assert projection.label("model", "four") == "other"
    assert projection.label("model", "user@example.com") == "unknown"


def test_unsampled_upstream_and_sampler_env_do_not_sample_local_metrics(metrics, monkeypatch):
    monkeypatch.setattr(bootstrap, "_provider", None)
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")
    monkeypatch.setenv("OTEL_TRACES_SAMPLER", "always_off")
    upstream = NonRecordingSpan(SpanContext(123, 456, True, TraceFlags(0)))
    with trace.use_span(upstream), observed("chat.pipeline") as span:
        assert span.get_span_context().trace_flags.sampled
    assert sample(metrics, "requests_total") == 1
    bootstrap.shutdown_tracing()


@contextmanager
def otlp_backend(status=200):
    received = []
    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest, ExportTraceServiceResponse
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            request = ExportTraceServiceRequest.FromString(payload)
            received.append((self.path, request))
            response = ExportTraceServiceResponse().SerializeToString()
            self.send_response(status)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/traces", received
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize("status", [200, 503])
def test_actual_otlp_http_export_success_and_failure_keep_metrics(metrics, status):
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    with otlp_backend(status) as (endpoint, received):
        exporter = OTLPSpanExporter(endpoint=endpoint, timeout=.1)
        exporter._session.trust_env = False
        metrics.provider.add_span_processor(BatchSpanProcessor(SafeSpanExporter(exporter)))
        with request_context("otlp-request"), observed("chat.pipeline") as root:
            with observed("agent.execute", **{"app.agent.name": "general"}):
                pass
        assert sample(metrics, "requests_total") == 1
        assert metrics.provider.force_flush(timeout_millis=5000)
        assert received and received[0][0] == "/v1/traces"
        remote = [s for _, batch in received for resource in batch.resource_spans for scope in resource.scope_spans for s in scope.spans]
        assert {s.name for s in remote} == {"chat.pipeline", "agent.execute"}
        assert {s.trace_id.hex() for s in remote} == {format(root.get_span_context().trace_id, "032x")}
        agent = next(s for s in remote if s.name == "agent.execute")
        assert agent.parent_span_id.hex() == format(root.get_span_context().span_id, "016x")
        if status == 503:
            assert sample(metrics, "contextweave_trace_export_failures_total") == 1
        metrics.provider.shutdown()


def test_default_metrics_endpoint_and_legacy_monitor_tool_trace_api(metrics, monkeypatch):
    orch, manager = services()
    orch.recognize_intent = AsyncMock(return_value=IntentResult(IntentCategory.GREETING, .99, UrgencyLevel.LOW, "other", {}, "", 0))
    memory = SimpleNamespace(begin_turn=AsyncMock(), add_message=AsyncMock(), get_lightweight_context=AsyncMock(return_value=LightweightContext()))
    pipeline = ContextPipeline(orch, ContextPlanner(None, "test"), memory)
    monitor = PerformanceMonitor(orch, manager, metrics_projection=metrics.projection)
    for name, value in (("_orchestrator", orch), ("_tool_manager", manager), ("_memory", memory), ("_context_pipeline", pipeline), ("_monitor", monitor)):
        monkeypatch.setattr(main, name, value)
    monkeypatch.setattr(main, "generate_latest", lambda: generate_latest(metrics.registry))
    client = TestClient(main.app)
    bodies = [client.post("/chat", json={"message": "hello"}).json() for _ in range(3)]
    assert client.get("/metrics").status_code == 200
    assert sample(metrics, "requests_total") == 3
    assert sample(metrics, "agent_latency_ms_count", {"agent": "general_0"}) == 3
    assert client.get("/trace/tool/" + bodies[0]["request_id"]).json()["found"]
    summary = client.get("/monitor").json()
    assert {"agent_stats", "tool_stats", "active_alerts", "suggestions"} <= summary.keys()
    assert summary["stats_reconciliation"]["agent"]["general_0"]["match"]


def test_reusable_smoke_script_reaches_actual_local_otlp_receiver():
    root = Path(__file__).resolve().parents[1]
    with otlp_backend() as (endpoint, received):
        result = subprocess.run([sys.executable, str(root / "scripts/otlp-smoke.py"), "--endpoint", endpoint],
                                capture_output=True, text=True, timeout=15)
        assert result.returncode == 0, result.stderr
        output = json.loads(result.stdout)
        assert output["ok"] and output["synthetic"] and output["projected_requests"] == 1
        assert received
        exported = [s for _, batch in received for resource in batch.resource_spans for scope in resource.scope_spans for s in scope.spans]
        assert len(exported) == 2 and {s.trace_id.hex() for s in exported} == {output["trace_id"]}


def test_sdk_disabled_uses_legacy_monitor_and_exposes_capture_unavailable(metrics, monkeypatch):
    monkeypatch.setattr(bootstrap, "_provider", None)
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    orch, manager = services()
    asyncio.run(orch.run(Request("hello", "u", "c", intent=IntentCategory.GREETING)))
    monitor = PerformanceMonitor(orch, manager, metrics_projection=metrics.projection)
    report = monitor.summary()["stats_reconciliation"]
    assert not report["capture_available"] and report["agent"]["general_0"]["source"] == "legacy"
    assert sample(metrics, "requests_total") == 0
    assert sample(metrics, "contextweave_metrics_capture_enabled") == 0
    bootstrap.shutdown_tracing()


def test_metrics_survive_invalid_trace_export_configuration(metrics, monkeypatch):
    monkeypatch.setattr(bootstrap, "_provider", None)
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "unsupported")
    with observed("chat.pipeline"):
        pass
    assert bootstrap.metrics_available() and sample(metrics, "requests_total") == 1
    bootstrap.shutdown_tracing()


def test_registered_metric_families_are_reused_when_monitors_share_registry(metrics, monkeypatch):
    orch, manager = services()
    listener = Mock()
    monkeypatch.setattr("monitor.performance_monitor.start_http_server", listener)
    one = PerformanceMonitor(orch, manager, metrics_projection=metrics.projection, prometheus_port=9999)
    two = PerformanceMonitor(orch, manager, metrics_projection=metrics.projection)
    listener.assert_called_once_with(9999)
    assert one._prom["requests_total"] is two._prom["requests_total"]
    assert one.summary()["stats_reconciliation"]["capture_available"]


def test_unknown_tool_is_counted_as_error_but_not_added_to_legacy_snapshot(metrics):
    _, manager = services()
    assert not asyncio.run(manager.call("missing_tool", {})).success
    assert not metrics.projection.snapshots(None, manager._metrics_owner)[1]
    assert sample(metrics, "contextweave_tool_calls_total", {"tool": "missing_tool", "layer": "mcp", "outcome": "error"}) == 1


def test_cancelled_agent_preserves_legacy_counter_definition(metrics):
    orch, manager = services()
    async def run():
        started = asyncio.Event()
        async def create(**kwargs):
            started.set()
            await asyncio.Event().wait()
        orch._pool[AgentType.GENERAL][0]._client.messages.create.side_effect = create
        task = asyncio.create_task(orch.run(Request("hello", "u", "c", intent=IntentCategory.GREETING)))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(run())
    row = metrics.projection.snapshots(orch._metrics_owner, manager._metrics_owner)[0]["general_0"]
    assert row["total"] == 1 and row["avg_ms"] == orch.get_stats()["general_0"]["avg_ms"] == 0
    assert sample(metrics, "agent_latency_ms_count", {"agent": "general_0"}) == 1


def test_snapshot_retention_is_bounded_and_monitor_falls_back_after_eviction(metrics):
    projection = MetricsProjection(CollectorRegistry(), max_owners=1, max_labels=1)
    for owner, agent in (("one", "general_0"), ("two", "general_1"), ("two", "general_2")):
        with observed("agent.execute", **{"app.agent.name": "general", "app.agent.instance": agent,
                      "app.agent.monitor_owner": owner, "app.agent.stats_counted": True, "app.agent.success": True}):
            pass
        projection.record(metrics.exporter.get_finished_spans()[-1])
    assert not projection.snapshots("one", None)[0]
    assert len(projection.snapshots("two", None)[0]) == 1
