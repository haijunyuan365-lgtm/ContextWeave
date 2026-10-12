"""Send two explicitly synthetic spans to an OTLP HTTP backend, without a model key."""
import argparse
import json
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CollectorRegistry

from observability.bootstrap import SafeSpanExporter
from observability.metrics_projection import MetricsProjection, MetricsSpanProcessor


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:4318/v1/traces", help="Full OTLP HTTP traces endpoint")
    args = parser.parse_args()
    registry = CollectorRegistry()
    projection = MetricsProjection(registry)
    exporter = SafeSpanExporter(OTLPSpanExporter(endpoint=args.endpoint, timeout=3))
    provider = TracerProvider(resource=Resource.create({"service.name": "contextweave-smoke"}))
    provider.add_span_processor(MetricsSpanProcessor(projection))
    provider.add_span_processor(BatchSpanProcessor(exporter))
    request_id = str(uuid.uuid4())
    tracer = provider.get_tracer("contextweave-smoke")
    try:
        attributes = {"app.request_id": request_id, "app.operation": "chat", "app.smoke_test": True, "app.outcome": "success"}
        with tracer.start_as_current_span("chat.pipeline", attributes=attributes) as root:
            with tracer.start_as_current_span("agent.execute", attributes={**attributes, "app.agent.name": "smoke"}):
                pass
        flushed = provider.force_flush(timeout_millis=5000)
        ok = flushed and exporter.failed_batches == 0
        print(json.dumps({"ok": ok, "synthetic": True, "request_id": request_id,
                          "trace_id": format(root.get_span_context().trace_id, "032x"),
                          "projected_requests": registry.get_sample_value("requests_total"),
                          "failed_batches": exporter.failed_batches}))
        return 0 if ok else 1
    finally:
        provider.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
