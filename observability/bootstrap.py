"""One explicitly shared SDK provider, isolated from third-party global providers."""
import logging
import os
import threading

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider, SpanLimits, ReadableSpan, Event
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ALWAYS_OFF
from opentelemetry.trace import NoOpTracerProvider, Status

from observability.sanitization import safe_attributes
from observability.metrics_projection import get_metrics_projection, MetricsSpanProcessor
from observability.policy import TracePolicy, SamplingExporter

logger = logging.getLogger(__name__)
_provider = None
_lock = threading.Lock()


class SafeSpanExporter(SpanExporter):
    """Defensive scrub also covers exceptions created by automatic HTTP instrumentation."""
    def __init__(self, delegate):
        self.delegate = delegate
        self.failed_batches = 0

    def export(self, spans):
        try:
            clean = [ReadableSpan(
                name=s.name, context=s.context, parent=s.parent, resource=s.resource,
                attributes=safe_attributes(s.attributes),
                events=[Event(e.name, safe_attributes(e.attributes), e.timestamp) for e in s.events],
                links=s.links, kind=s.kind, status=Status(s.status.status_code),
                start_time=s.start_time, end_time=s.end_time,
                instrumentation_scope=s.instrumentation_scope,
            ) for s in spans]
            result = self.delegate.export(clean)
        except Exception:
            result = SpanExportResult.FAILURE
        if result == SpanExportResult.FAILURE:
            self.failed_batches += 1
            get_metrics_projection().export_errors.inc()
            logger.warning("trace_export_failed batches=%d", self.failed_batches)
        return result

    def shutdown(self):
        try:
            self.delegate.shutdown()
        except Exception:
            logger.warning("trace_export_shutdown_failed")

    def force_flush(self, timeout_millis=30000):
        try:
            return self.delegate.force_flush(timeout_millis)
        except Exception:
            logger.warning("trace_export_flush_failed")
            return False


def configure_tracing():
    global _provider
    with _lock:
        if _provider is not None:
            return _provider
        provider = None
        try:
            enabled = os.getenv("OTEL_SDK_DISABLED", "false").lower() not in {"1", "true", "yes"}
            provider = TracerProvider(
                resource=Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", "contextweave")}),
                sampler=ALWAYS_ON if enabled else ALWAYS_OFF,
                span_limits=SpanLimits(max_attributes=64, max_events=16, max_attribute_length=256),
                shutdown_on_exit=False,
            )
            # ALWAYS_ON feeds the local projector before the asynchronous trace export queue.
            projection = get_metrics_projection()
            provider.add_span_processor(MetricsSpanProcessor(projection))
            provider.contextweave_metrics_enabled = enabled
            projection.capture_enabled.set_function(metrics_available)
            try:
                policy = TracePolicy.load()
            except Exception:
                policy = TracePolicy(export_ratio=0)
                logger.warning("trace_policy_invalid; detailed export disabled")
            projection.prices = policy.prices
            if enabled and policy.history_enabled:
                try:
                    from observability import history
                    store = history.TraceHistory(policy)
                    provider.add_span_processor(BatchSpanProcessor(SafeSpanExporter(SamplingExporter(store, policy.export_ratio))))
                    history._history = store
                except Exception:
                    logger.warning("trace_history_setup_failed")
            mode = os.getenv("OTEL_TRACES_EXPORTER", "console").lower().strip()
            if enabled and mode != "none":
                if mode == "console":
                    exporter = ConsoleSpanExporter()
                elif mode == "otlp":
                    # HTTP/protobuf: standard endpoint, headers and timeout env vars are honoured.
                    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
                    exporter = OTLPSpanExporter()
                else:
                    raise ValueError("unsupported trace exporter")
                provider.add_span_processor(BatchSpanProcessor(SafeSpanExporter(SamplingExporter(exporter, policy.export_ratio))))
            _provider = provider
        except Exception:
            # A broken exporter/configuration must not discard full local metrics.
            logger.warning("trace_setup_failed; trace export disabled")
            _provider = provider if provider is not None else NoOpTracerProvider()
        return _provider


def get_provider():
    return configure_tracing()


def metrics_available():
    return getattr(get_provider(), "contextweave_metrics_enabled", False)


def shutdown_tracing():
    if _provider is None:
        return
    try:
        if hasattr(_provider, "force_flush") and not _provider.force_flush(timeout_millis=5000):
            logger.warning("trace_flush_incomplete")
    except Exception:
        logger.warning("trace_flush_failed")
    finally:
        try:
            if hasattr(_provider, "shutdown"):
                _provider.shutdown()
        except Exception:
            logger.warning("trace_shutdown_failed")


class CorrelationFilter(logging.Filter):
    def filter(self, record):
        from observability.context import correlation_ids
        for key, value in correlation_ids().items():
            setattr(record, key, value or "-")
        return True


def configure_logging():
    for handler in logging.getLogger().handlers:
        if not any(isinstance(f, CorrelationFilter) for f in handler.filters):
            handler.addFilter(CorrelationFilter())
