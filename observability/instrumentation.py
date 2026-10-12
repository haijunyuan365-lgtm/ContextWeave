"""Business code uses this package instead of exporter/platform APIs."""
from contextlib import contextmanager
from functools import wraps
import inspect

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode, SpanKind

from observability.bootstrap import get_provider
from observability.context import correlation_ids, request_context
from observability.sanitization import error_code, safe_attributes
from observability import semantics as sem


def set_outcome(span, outcome: str, *, fallback: bool = False, code: str = ""):
    span.set_attribute(sem.OUTCOME, outcome)
    span.set_attribute(sem.FALLBACK_USED, fallback)
    if code:
        span.set_attribute(sem.ERROR_CODE, code)
    if outcome == sem.ERROR:
        span.set_status(Status(StatusCode.ERROR))


@contextmanager
def observed(name: str, *, span_type: str = "context", parent_context=None,
             links=(), kind=SpanKind.INTERNAL, reuse: bool = False, **attributes):
    current = trace.get_current_span()
    if reuse and getattr(current, "name", None) == name:
        yield current
        return
    tracer = get_provider().get_tracer("contextweave", "1.0.0")
    inherited = {key: value for key, value in getattr(current, "attributes", {}).items()
                 if key in {"app.agent.name", "app.agent.role", "app.rag.query_index", sem.OPERATION}}
    attrs = {sem.REQUEST_ID: correlation_ids()["request_id"], sem.OPERATION: "chat", **inherited,
             sem.SPAN_TYPE: span_type, sem.OUTCOME: sem.SUCCESS, **attributes}
    with tracer.start_as_current_span(name, context=parent_context, links=links, kind=kind,
                                     attributes=safe_attributes(attrs),
                                     record_exception=False, set_status_on_exception=False) as span:
        try:
            yield span
        except BaseException as exc:
            code = error_code(exc)
            set_outcome(span, sem.ERROR, code=code)
            span.add_event("exception", {"exception.type": type(exc).__name__, sem.ERROR_CODE: code})
            raise


def annotate(**attributes):
    span = trace.get_current_span()
    for key, value in safe_attributes(attributes).items():
        span.set_attribute(key, value)


def outcome(value: str, *, fallback: bool = False, code: str = ""):
    set_outcome(trace.get_current_span(), value, fallback=fallback, code=code)


def instrumented(name: str, *, span_type: str = "context", reuse: bool = False,
                 result_count: str | None = None, propagate_degradation: bool = False):
    """Wrap sync/async functions without capturing arguments or returned content."""
    def decorate(function):
        if inspect.iscoroutinefunction(function):
            @wraps(function)
            async def wrapped(*args, **kwargs):
                with observed(name, span_type=span_type, reuse=reuse) as span:
                    result = await function(*args, **kwargs)
                    if result_count and isinstance(result, (list, dict)):
                        annotate(**{result_count: len(result)})
                if propagate_degradation:
                    _propagate_degradation(span)
                return result
        else:
            @wraps(function)
            def wrapped(*args, **kwargs):
                with observed(name, span_type=span_type, reuse=reuse) as span:
                    result = function(*args, **kwargs)
                    if result_count and isinstance(result, (list, dict)):
                        annotate(**{result_count: len(result)})
                if propagate_degradation:
                    _propagate_degradation(span)
                return result
        return wrapped
    return decorate


def _propagate_degradation(span):
    attributes = getattr(span, "attributes", {})
    parent = trace.get_current_span()
    if attributes.get(sem.OUTCOME) == sem.DEGRADED and getattr(parent, "attributes", {}).get(sem.OUTCOME) != sem.ERROR:
        set_outcome(parent, sem.DEGRADED, fallback=True, code=attributes.get(sem.ERROR_CODE, "STAGE_FALLBACK"))


def traced(name: str, *, span_type: str = "context", reuse: bool = False):
    """Trace async methods without capturing inputs or outputs."""
    def decorate(function):
        @wraps(function)
        async def wrapped(self, req, *args, **kwargs):
            with request_context(req.request_id), observed(name, span_type=span_type, reuse=reuse):
                return await function(self, req, *args, **kwargs)
        return wrapped
    return decorate
