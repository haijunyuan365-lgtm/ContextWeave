"""Request-local business identity alongside the current OTel context."""
from contextlib import contextmanager
from contextvars import ContextVar

from opentelemetry import trace
from opentelemetry.propagate import inject, extract

_request_id = ContextVar("contextweave_request_id", default="")


@contextmanager
def request_context(request_id: str):
    token = _request_id.set(request_id)
    try:
        yield
    finally:
        _request_id.reset(token)


def correlation_ids() -> dict[str, str]:
    context = trace.get_current_span().get_span_context()
    return {
        "request_id": _request_id.get(),
        "trace_id": format(context.trace_id, "032x") if context.is_valid else "",
        "span_id": format(context.span_id, "016x") if context.is_valid else "",
    }


def inject_headers(headers: dict[str, str]) -> None:
    """Inject W3C traceparent/tracestate into an outgoing carrier."""
    inject(headers)


def extract_headers(headers: dict[str, str]):
    return extract(headers)
