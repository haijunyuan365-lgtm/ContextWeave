from memory_fakes import atomic_http_stub
import asyncio
import io
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from api import main
from agents.agent_orchestrator import AgentOrchestrator
from core.context_planner import ContextPlanner, LightweightContext
from core.context_runtime import ContextPipeline, RetrievalSession
from core.intent_recognizer import IntentCategory, IntentResult, UrgencyLevel
from observability import observed, request_context, correlation_ids
from observability import bootstrap
from observability.bootstrap import SafeSpanExporter, CorrelationFilter
from observability.context import inject_headers, extract_headers
from observability.sanitization import safe_attributes


@pytest.fixture(scope="module")
def collector():
    exporter = InMemorySpanExporter()
    bootstrap.get_provider().add_span_processor(SimpleSpanProcessor(SafeSpanExporter(exporter)))
    return exporter


@pytest.fixture
def spans(collector):
    collector.clear()
    return collector


@pytest.fixture
def service(monkeypatch):
    orch = AgentOrchestrator("test-key", model="test")
    orch.recognize_intent = AsyncMock(return_value=IntentResult(
        IntentCategory.GREETING, .99, UrgencyLevel.LOW, "other", {}, "", 0))
    async def create(**kwargs):
        await asyncio.sleep(.002)
        return SimpleNamespace(content=[{"type": "text", "text": "mock response"}])
    for agents in orch._pool.values():
        agents[0]._client = SimpleNamespace(messages=SimpleNamespace(create=create))
    async def begin(*args):
        await asyncio.sleep(.01)
        return "test-source"
    async def persist(*args, **kwargs):
        await asyncio.sleep(.01)
    mem = SimpleNamespace(
        get_lightweight_context=AsyncMock(return_value=LightweightContext()),
        begin_turn=AsyncMock(side_effect=begin), add_message=AsyncMock(side_effect=persist))
    pipeline = ContextPipeline(orch, ContextPlanner(None, "test"), mem)
    monkeypatch.setattr(main, "_orchestrator", orch)
    monkeypatch.setattr(main, "_memory", atomic_http_stub(mem))
    monkeypatch.setattr(main, "_context_pipeline", pipeline)
    return orch, mem, pipeline


def by_name(exporter, name):
    return [s for s in exporter.get_finished_spans() if s.name == name]


def test_chat_complete_tree_duration_and_legacy_trace(service, spans):
    body = TestClient(main.app).post("/chat", json={"message": "private question", "conv_id": "private-conv"}).json()
    finished = spans.get_finished_spans()
    tree = {s.name: s for s in finished}
    required = {"POST /chat", "chat.pipeline", "memory.begin_turn", "context.prepare",
                "memory.load_lightweight", "intent.recognize", "context.plan", "context.retrieve",
                "context.skills", "agent.orchestrate", "agent.execute", "experience.submit",
                "context.cleanup", "memory.persist"}
    assert required <= tree.keys()
    assert len({s.context.trace_id for s in finished}) == 1
    assert all(s.attributes["app.request_id"] == body["request_id"] for s in finished)
    assert body["trace_id"] == format(tree["POST /chat"].context.trace_id, "032x")
    assert tree["POST /chat"].kind == SpanKind.SERVER
    assert tree["chat.pipeline"].parent.span_id == tree["POST /chat"].context.span_id
    for name in ("memory.begin_turn", "context.prepare", "agent.orchestrate", "experience.submit",
                 "context.cleanup", "memory.persist"):
        assert tree[name].parent.span_id == tree["chat.pipeline"].context.span_id
    assert tree["context.plan"].parent.span_id == tree["context.prepare"].context.span_id
    assert tree["agent.execute"].parent.span_id == tree["agent.orchestrate"].context.span_id
    assert body["request_duration_ms"] >= body["latency_ms"] + 20
    root = tree["chat.pipeline"]
    assert root.start_time <= min(s.start_time for s in finished if s.name != "POST /chat")
    assert root.end_time >= max(s.end_time for s in finished if s.name != "POST /chat")
    metadata = str([(dict(s.attributes), [(e.name, dict(e.attributes)) for e in s.events]) for s in finished])
    assert "private question" not in metadata and "private-conv" not in metadata
    legacy = TestClient(main.app).get("/trace/tool/" + body["request_id"]).json()
    assert legacy["found"] and legacy["trace"]["request_id"] == body["request_id"]


def test_concurrent_http_requests_do_not_share_identity(service, spans):
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
            return await asyncio.gather(*(client.post("/chat", json={"message": str(i)}) for i in range(2)))
    responses = asyncio.run(run())
    bodies = [r.json() for r in responses]
    assert len({b["trace_id"] for b in bodies}) == 2
    for body in bodies:
        branch = [s for s in spans.get_finished_spans() if s.attributes.get("app.request_id") == body["request_id"]]
        assert len(branch) >= 10
        assert {format(s.context.trace_id, "032x") for s in branch} == {body["trace_id"]}
    assert correlation_ids() == {"request_id": "", "trace_id": "", "span_id": ""}


def test_parallel_agent_spans_are_siblings(service, spans):
    orch, _, _ = service
    orch.recognize_intent.return_value = IntentResult(
        IntentCategory.TECHNICAL_LOGIN, .99, UrgencyLevel.LOW, "technical", {}, "", 0)
    response = TestClient(main.app).post("/chat", json={"message": "登录报错同时被重复扣款"})
    assert response.status_code == 200
    agents = by_name(spans, "agent.execute")
    parent = by_name(spans, "agent.orchestrate")[0]
    assert {s.attributes["app.agent.name"] for s in agents} == {"technical", "billing"}
    assert all(s.parent.span_id == parent.context.span_id for s in agents)
    composer = by_name(spans, "agent.compose")[0]
    assert composer.parent.span_id == parent.context.span_id
    assert composer.start_time >= max(s.end_time for s in agents)


@pytest.mark.parametrize("stage", ["begin", "prepare", "prepare_allocated", "agent", "persist", "cleanup"])
def test_chat_failure_ends_trace_and_closes_session(service, spans, monkeypatch, stage):
    orch, mem, pipeline = service
    secret = "secret-user-input-token"
    error = RuntimeError(secret)
    closed = []
    original_close = RetrievalSession.close
    async def close(session):
        closed.append(session)
        await original_close(session)
        if stage == "cleanup":
            raise error
    monkeypatch.setattr(RetrievalSession, "close", close)
    if stage == "begin":
        mem.begin_turn.side_effect = error
    elif stage == "prepare":
        pipeline.planner.plan = AsyncMock(side_effect=error)
    elif stage == "prepare_allocated":
        original_prepare = pipeline.prepare
        async def prepare(req):
            await original_prepare(req)
            raise error
        pipeline.prepare = prepare
    elif stage == "agent":
        orch._execute_agent = AsyncMock(side_effect=error)
    elif stage == "persist":
        mem.add_message.side_effect = error
    response = TestClient(main.app, raise_server_exceptions=False).post("/chat", json={"message": "hello"})
    assert response.status_code == 500
    root = by_name(spans, "chat.pipeline")[0]
    assert root.end_time and root.status.status_code == StatusCode.ERROR
    assert root.attributes["app.outcome"] == "error"
    assert bool(closed) == (stage not in {"begin", "prepare"})
    for s in spans.get_finished_spans():
        assert secret not in str(s.attributes) + str([(e.name, dict(e.attributes)) for e in s.events])
        assert not s.status.description
    assert correlation_ids()["request_id"] == ""


def test_cleanup_error_does_not_replace_primary_failure(service, spans, monkeypatch):
    orch, _, _ = service
    orch._execute_agent = AsyncMock(side_effect=ValueError("primary"))
    monkeypatch.setattr(RetrievalSession, "close", AsyncMock(side_effect=RuntimeError("cleanup")))
    with pytest.raises(ValueError, match="primary"):
        TestClient(main.app).post("/chat", json={"message": "hello"})
    assert by_name(spans, "context.cleanup")[0].status.status_code == StatusCode.ERROR


def test_memory_and_experience_fallbacks_are_observable(service, spans):
    _, mem, pipeline = service
    mem.get_lightweight_context.side_effect = RuntimeError("private")
    pipeline.experience = SimpleNamespace(submit=AsyncMock(side_effect=RuntimeError("private")))
    response = TestClient(main.app).post("/chat", json={"message": "hello"})
    assert response.status_code == 200
    assert by_name(spans, "memory.load_lightweight")[0].attributes["app.outcome"] == "degraded"
    assert by_name(spans, "experience.submit")[0].attributes["app.outcome"] == "degraded"


def test_gather_create_task_to_thread_and_log_context_propagate(spans):
    async def run():
        with request_context("r1"), observed("parent") as parent:
            async def child(name):
                with observed(name):
                    await asyncio.sleep(0)
                    assert correlation_ids()["request_id"] == "r1"
            def threaded():
                with observed("thread"):
                    assert correlation_ids()["request_id"] == "r1"
                    record = logging.LogRecord("test", logging.INFO, "", 0, "ok", (), None)
                    CorrelationFilter().filter(record)
                    assert record.request_id == "r1" and record.trace_id == correlation_ids()["trace_id"]
            await asyncio.gather(child("gather"), asyncio.create_task(child("task")), asyncio.to_thread(threaded))
            return parent.get_span_context().span_id
    parent_id = asyncio.run(run())
    assert all(s.parent.span_id == parent_id for s in spans.get_finished_spans() if s.name != "parent")
    assert correlation_ids()["request_id"] == ""


def test_w3c_incoming_parent_and_outgoing_headers(service, spans):
    incoming_trace = "1234567890abcdef1234567890abcdef"
    response = TestClient(main.app).post("/chat", json={"message": "hello"},
        headers={"traceparent": f"00-{incoming_trace}-1234567890abcdef-01"})
    assert response.json()["trace_id"] == incoming_trace
    server = by_name(spans, "POST /chat")[0]
    assert server.parent.span_id == int("1234567890abcdef", 16)
    with observed("outgoing") as span:
        headers = {}
        inject_headers(headers)
        extracted = trace.get_current_span(extract_headers(headers)).get_span_context()
        assert extracted.trace_id == span.get_span_context().trace_id


def test_cancelled_task_ends_span_and_restores_context(spans):
    async def run():
        started = asyncio.Event()
        async def work():
            with request_context("cancel"), observed("cancelled"):
                started.set()
                await asyncio.Event().wait()
        task = asyncio.create_task(work())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert correlation_ids()["request_id"] == ""
    asyncio.run(run())
    assert by_name(spans, "cancelled")[0].attributes["app.error_code"] == "CANCELLED"


def test_safe_attributes_are_bounded_and_drop_payloads():
    cleaned = safe_attributes({"app.prompt": "secret", "exception.stacktrace": "secret",
        "http.url": "http://test/chat?question=secret", "app.tool.arguments": {"password": "secret"},
        "app.agent.name": "x" * 1000, "gen_ai.usage.input_tokens": None})
    assert "secret" not in str(cleaned)
    assert len(cleaned["app.agent.name"]) == 256
    assert "gen_ai.usage.input_tokens" not in cleaned


@pytest.mark.parametrize("mode", ["console", "otlp", "none", "invalid"])
def test_bootstrap_is_idempotent_and_shutdown_flushes(monkeypatch, mode):
    monkeypatch.setattr(bootstrap, "_provider", None)
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", mode)
    monkeypatch.setenv("OTEL_SDK_DISABLED", "false")
    provider = bootstrap.configure_tracing()
    assert bootstrap.configure_tracing() is provider
    if isinstance(provider, TracerProvider):
        assert provider.resource.attributes["service.name"] == "contextweave"
        provider.force_flush = Mock(return_value=True)
        provider.shutdown = Mock()
        bootstrap.shutdown_tracing()
        provider.force_flush.assert_called_once_with(timeout_millis=5000)
        provider.shutdown.assert_called_once()
        # Close real processors too; the fake shutdown only checks the public lifecycle.
        provider._active_span_processor.shutdown()


def test_disabled_sdk_and_bad_queue_config_do_not_break_business(monkeypatch):
    monkeypatch.setattr(bootstrap, "_provider", None)
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    with observed("disabled") as span:
        assert not span.is_recording()
    bootstrap.shutdown_tracing()
    monkeypatch.setattr(bootstrap, "_provider", None)
    monkeypatch.setenv("OTEL_SDK_DISABLED", "false")
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "console")
    monkeypatch.setenv("OTEL_BSP_MAX_QUEUE_SIZE", "0")
    with observed("invalid-config") as span:
        # A failed trace exporter must preserve the full local metrics channel.
        assert span.is_recording()
        assert bootstrap.metrics_available()


def test_exporter_failure_in_batch_worker_does_not_break_chat(service, spans):
    class BrokenExporter(SpanExporter):
        def export(self, batch):
            raise RuntimeError("secret backend credentials")
        def shutdown(self):
            raise RuntimeError("secret backend credentials")
    safe = SafeSpanExporter(BrokenExporter())
    processor = BatchSpanProcessor(safe, max_queue_size=32, max_export_batch_size=16)
    bootstrap.get_provider().add_span_processor(processor)
    try:
        assert TestClient(main.app).post("/chat", json={"message": "hello"}).status_code == 200
        assert processor.force_flush(timeout_millis=5000)
        assert safe.failed_batches > 0
    finally:
        processor.shutdown()


def test_console_exporter_produces_readable_trace(spans):
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter
    with request_context("console-request"), observed("console-test"):
        pass
    stream = io.StringIO()
    SafeSpanExporter(ConsoleSpanExporter(out=stream)).export(by_name(spans, "console-test"))
    output = stream.getvalue()
    assert '"name": "console-test"' in output and '"app.request_id": "console-request"' in output


@pytest.mark.parametrize("failure", ["none", "startup", "shutdown"])
def test_application_lifespan_always_shuts_down_tracing(monkeypatch, failure):
    @asynccontextmanager
    async def fake_service(app):
        if failure == "startup":
            raise RuntimeError("startup failed")
        yield
        if failure == "shutdown":
            raise RuntimeError("shutdown failed")
    shutdown = Mock()
    monkeypatch.setattr(main, "_service_lifespan", fake_service)
    monkeypatch.setattr(main, "shutdown_tracing", shutdown)
    async def run():
        async with main.lifespan(main.app):
            pass
    if failure == "none":
        asyncio.run(run())
    else:
        with pytest.raises(RuntimeError):
            asyncio.run(run())
    shutdown.assert_called_once()


def test_otlp_http_serialization_preserves_ids_without_network(spans):
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
    with request_context("otlp-request"), observed("otlp-test"):
        pass
    sample = by_name(spans, "otlp-test")
    exporter = OTLPSpanExporter(endpoint="http://collector.invalid:4318/v1/traces")
    exporter._session.post = Mock(return_value=SimpleNamespace(ok=True))
    try:
        assert SafeSpanExporter(exporter).export(sample) == SpanExportResult.SUCCESS
        payload = exporter._session.post.call_args.kwargs["data"]
        decoded = ExportTraceServiceRequest.FromString(payload)
        exported = decoded.resource_spans[0].scope_spans[0].spans[0]
        assert exported.name == "otlp-test"
        assert exported.trace_id.hex() == format(sample[0].context.trace_id, "032x")
        assert exported.span_id.hex() == format(sample[0].context.span_id, "016x")
    finally:
        exporter.shutdown()
