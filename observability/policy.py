"""Validated local retention, export sampling and explicitly configured pricing."""
from dataclasses import dataclass, field
import math
import os
import re
from pathlib import Path

import yaml
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


@dataclass(frozen=True)
class TracePolicy:
    history_enabled: bool = False
    history_path: str = "data/observability/traces.sqlite3"
    retention_days: int = 7
    max_traces: int = 2000
    max_spans_per_trace: int = 500
    export_ratio: float = 1.0
    prices: dict = field(default_factory=dict)

    @classmethod
    def load(cls):
        path = Path(os.getenv("TRACE_POLICY_PATH", "config/observability.yml"))
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
        raw = raw or {}
        history = raw.get("history", {})
        enabled = os.getenv("TRACE_HISTORY_ENABLED", str(history.get("enabled", False))).lower() in {"true", "1", "yes"}
        ratio = float(os.getenv("TRACE_EXPORT_RATIO", raw.get("export_ratio", 1.0)))
        if not math.isfinite(ratio) or not 0 <= ratio <= 1:
            raise ValueError("invalid export ratio")
        limits = [history.get("retention_days", 7), history.get("max_traces", 2000), history.get("max_spans_per_trace", 500)]
        if any(type(v) is not int or v < 1 for v in limits):
            raise ValueError("invalid retention limits")
        prices = raw.get("prices", {})
        if not isinstance(prices, dict) or len(prices) > 64:
            raise ValueError("invalid prices")
        for model, price in prices.items():
            if not isinstance(model, str) or not isinstance(price, dict):
                raise ValueError("invalid price entry")
            for key in ("version", "currency"):
                if not isinstance(price.get(key), str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,32}", price[key]):
                    raise ValueError("price requires version and currency")
            for key in ("input", "output", "cache_read", "cache_write"):
                value = price.get(key)
                if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                    raise ValueError("price requires all four finite nonnegative rates")
        return cls(enabled, os.getenv("TRACE_HISTORY_PATH", history.get("path", cls.history_path)),
                   *limits, ratio, prices)


class SamplingExporter(SpanExporter):
    """Deterministic whole-trace decision AFTER the full metrics processor."""
    def __init__(self, delegate, ratio):
        self.delegate = delegate
        self.threshold = int(ratio * (1 << 64))

    def export(self, spans):
        selected = [s for s in spans if (s.context.trace_id & ((1 << 64) - 1)) < self.threshold]
        return self.delegate.export(selected) if selected else SpanExportResult.SUCCESS

    def shutdown(self):
        return self.delegate.shutdown()

    def force_flush(self, timeout_millis=30000):
        return self.delegate.force_flush(timeout_millis)


USAGE_FIELDS = {"input": "gen_ai.usage.input_tokens", "output": "gen_ai.usage.output_tokens",
                "cache_read": "app.llm.cache_read_tokens", "cache_write": "app.llm.cache_write_tokens"}


def estimate_cost(attributes, prices):
    """No configured price or incomplete usage means unknown, never zero cost."""
    model = attributes.get("gen_ai.response.model") or attributes.get("gen_ai.request.model")
    price = prices.get(model)
    if not price:
        return None
    usage = {kind: attributes.get(key) for kind, key in USAGE_FIELDS.items()}
    if any(type(v) is not int or v < 0 for v in usage.values()):
        return None
    amount = sum(usage[k] * price[k] for k in usage) / 1_000_000
    return (amount, price["currency"], price["version"]) if math.isfinite(amount) else None
