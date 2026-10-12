"""Business code uses this package instead of exporter/platform APIs."""
from contextlib import contextmanager
from functools import wraps

from opentelemetry.trace import Status, StatusCode

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
def observed(name: str, *, span_type: str = "context", **attributes):
    tracer = get_provider().get_tracer("contextweave", "1.0.0")
    attrs = {sem.REQUEST_ID: correlation_ids()["request_id"], sem.OPERATION: "chat",
             sem.SPAN_TYPE: span_type, sem.OUTCOME: sem.SUCCESS, **attributes}
    with tracer.start_as_current_span(name, attributes=safe_attributes(attrs),
                                     record_exception=False, set_status_on_exception=False) as span:
        try:
            yield span
        except BaseException as exc:
            code = error_code(exc)
            set_outcome(span, sem.ERROR, code=code)
            span.add_event("exception", {"exception.type": type(exc).__name__, sem.ERROR_CODE: code})
            raise


def traced(name: str, *, span_type: str = "context"):
    """Trace async methods without capturing inputs or outputs."""
    def decorate(function):
        @wraps(function)
        async def wrapped(self, req, *args, **kwargs):
            with request_context(req.request_id), observed(name, span_type=span_type):
                return await function(self, req, *args, **kwargs)
        return wrapped
    return decorate
