"""Request-local business identity alongside the current OTel context."""
from contextlib import contextmanager
from contextvars import ContextVar
import contextvars
import asyncio

from opentelemetry import trace
from opentelemetry.propagate import inject, extract
from opentelemetry.context import Context, attach, detach
from opentelemetry.trace import SpanContext, TraceFlags, Link

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


def detached_task(coroutine):
    """Python 3.10-compatible task creation without the initiating request's context."""
    return contextvars.Context().run(asyncio.create_task, coroutine)


@contextmanager
def detached_context():
    token = attach(Context())
    try:
        with request_context(""):
            yield
    finally:
        detach(token)


def source_link(trace_id: str, span_id: str, flags: int = 1):
    try:
        if len(trace_id) != 32 or len(span_id) != 16:
            return ()
        context = SpanContext(int(trace_id, 16), int(span_id, 16), True, TraceFlags(flags))
        return (Link(context),) if context.is_valid else ()
    except (ValueError, TypeError):
        return ()
