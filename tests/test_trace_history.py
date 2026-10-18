"""P3: real durable history, authorized replay, whole-trace sampling and pricing."""
import asyncio
from dataclasses import replace
import json
import sqlite3
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, use_span
from prometheus_client import CollectorRegistry
import pytest
import yaml

from api import main
from evaluation.evaluator import EndToEndEvaluator, QualityScores
from observability import bootstrap, history, observed, request_context, correlation_ids
from observability.bootstrap import SafeSpanExporter
from observability.evaluation import record_scores
from observability.history import TraceHistory
from observability.metrics_projection import MetricsProjection, MetricsSpanProcessor
from observability.policy import TracePolicy, SamplingExporter, estimate_cost
from observability.rag_snapshot import candidates


@pytest.fixture
def backend(tmp_path, monkeypatch):
    policy = TracePolicy(history_enabled=True, history_path=str(tmp_path / "trace.sqlite3"))
    store = TraceHistory(policy)
    registry = CollectorRegistry()
    projection = MetricsProjection(registry)
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(MetricsSpanProcessor(projection))
    provider.add_span_processor(BatchSpanProcessor(SafeSpanExporter(SamplingExporter(store, 1)), schedule_delay_millis=60000))
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(bootstrap, "_provider", provider)
    monkeypatch.setattr(bootstrap, "get_metrics_projection", lambda: projection)
    monkeypatch.setattr(history, "_history", store)
    monkeypatch.setenv("TRACE_ADMIN_TOKEN", "test-admin-token")
    yield SimpleNamespace(store=store, provider=provider, projection=projection, registry=registry, exporter=exporter)
    provider.shutdown()


def scored_trace(backend, score=.5, failed=False):
    with request_context("request-123"), observed("evaluation.dialog"):
        trace_id = correlation_ids()["trace_id"]
        with observed("agent.execute", **{"app.agent.name": "general"}):
            pass
        record_scores(QualityScores(score, score, score, score, judge_failed=failed), test_id="dialog_0", passed=score >= .75)
    assert backend.provider.force_flush(5000)
    return trace_id


def auth():
    return {"Authorization": "Bearer test-admin-token"}


def test_background_sqlite_persists_relationships_and_metadata_after_restart(backend):
    with request_context("r1"), observed("chat.pipeline", prompt="PRIVATE", **{"app.conversation.hash": "hashed"}):
        ids = correlation_ids()
        with observed("rag.rerank"):
            candidates([{"chunk_id": "private-ref", "content": "SECRET", "title": "secret", "score": .5}])
    assert backend.provider.force_flush(5000)
    reopened = TraceHistory(backend.store.policy)
    detail = reopened.detail(ids["trace_id"])
    assert detail["request_id"] == "r1" and len(detail["spans"]) == 2
    assert detail["name"] == "chat.pipeline"
    child = next(s for s in detail["spans"] if s["name"] == "rag.rerank")
    assert child["parent_span_id"] == ids["span_id"]
    raw = json.dumps(detail)
    assert all(value not in raw for value in ("PRIVATE", "SECRET", "secret", "private-ref"))
    assert json.loads(child["events"][0]["attributes"]["app.rag.candidate_refs"][0])[1:] == [1, .5]
    reopened.shutdown()


def test_history_search_filters_paging_and_parameterized_input(backend):
    trace_id = scored_trace(backend)
    assert backend.store.search(request_id="request-123", limit=1)["total"] == 1
    assert backend.store.search(request_id="' OR 1=1 --")["total"] == 0
    assert backend.store.search(since_ns=time.time_ns()+1000)["total"] == 0
    assert backend.store.search(outcome="error")["total"] == 0
    assert backend.store.search(offset=1)["items"] == []
    assert backend.store.search(low_score=True)["items"][0]["trace_id"] == trace_id


def test_judge_failure_does_not_become_regression_candidate(backend):
    trace_id = scored_trace(backend, failed=True)
    assert backend.store.detail(trace_id)["judge_failed"] == 1
    assert backend.store.search(low_score=True)["total"] == 0


def test_retention_cascades_scores_spans_and_regression_inputs(backend):
    trace_id = scored_trace(backend)
    assert backend.store.save_regression(trace_id, ["reviewed fixture"])
    with sqlite3.connect(backend.store.path) as db:
        db.execute("UPDATE traces SET start_ns=1 WHERE trace_id=?", (trace_id,))
    assert backend.store.detail(trace_id) is None
    with sqlite3.connect(backend.store.path) as db:
        for table in ("spans", "evaluations", "regressions"):
            assert db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0] == 0


def test_storage_limits_and_explicit_truncation(backend):
    backend.store.policy = replace(backend.store.policy, max_traces=1, max_spans_per_trace=2)
    first = scored_trace(backend)
    with observed("chat.pipeline"):
        second = correlation_ids()["trace_id"]
        for i in range(4):
            with observed("tool.execute", **{"app.tool.name": str(i)}):
                pass
    backend.provider.force_flush(5000)
    assert backend.store.detail(first) is None
    detail = backend.store.detail(second)
    assert len(detail["spans"]) == 2 and detail["possibly_truncated"] is True
    assert backend.store.search()["total"] == 1


def test_delete_blocks_late_queued_span_reinsertion(backend):
    trace_id = scored_trace(backend)
    backend.store.save_regression(trace_id, ["fixture"])
    saved = backend.exporter.get_finished_spans()
    assert backend.store.delete(trace_id)
    backend.store.export(saved)
    assert backend.store.detail(trace_id) is None
    assert backend.store.regression(trace_id) is None


def test_all_historical_routes_require_token_and_validate_inputs(backend, monkeypatch):
    trace_id = scored_trace(backend)
    client = TestClient(main.app)
    paths = ["/trace/history", f"/trace/history/{trace_id}", "/trace/regressions"]
    for path in paths:
        assert client.get(path).status_code == 401
        assert client.get(path, headers={"Authorization": "Bearer wrong"}).status_code == 401
        assert client.get(path, headers=auth()).status_code == 200
    assert client.delete(f"/trace/history/{trace_id}").status_code == 401
    assert client.put(f"/trace/regressions/{trace_id}", json={"turns": ["test"]}).status_code == 401
    assert client.post(f"/trace/regressions/{trace_id}/run").status_code == 401
    assert client.get("/trace/history?limit=101", headers=auth()).status_code == 422
    assert client.get("/trace/history?since_ns=999999999999999999999999", headers=auth()).status_code == 422
    assert client.get("/trace/history/invalid", headers=auth()).status_code == 422
    assert client.get("/trace/history/" + "0"*32, headers=auth()).status_code == 422
    monkeypatch.delenv("TRACE_ADMIN_TOKEN")
    assert client.get("/trace/history", headers=auth()).status_code == 503


def test_disabled_or_faulty_store_returns_fixed_unavailable(backend, monkeypatch):
    client = TestClient(main.app)
    monkeypatch.setattr(history, "_history", None)
    assert client.get("/trace/history", headers=auth()).status_code == 503
    monkeypatch.setattr(history, "_history", backend.store)
    monkeypatch.setattr(backend.store, "search", lambda **kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("SECRET PATH")))
    response = client.get("/trace/history", headers=auth())
    assert response.status_code == 503 and "SECRET" not in response.text


def test_regression_requires_reviewed_fixture_and_replays_with_source(backend, monkeypatch):
    trace_id = scored_trace(backend)
    client = TestClient(main.app)
    assert client.post(f"/trace/regressions/{trace_id}/run", headers=auth()).status_code == 409
    for turns in ([], [" "], ["x"*2001], ["x"]*11):
        assert client.put(f"/trace/regressions/{trace_id}", headers=auth(), json={"turns": turns}).status_code == 422
    assert client.put(f"/trace/regressions/{trace_id}", headers=auth(), json={"turns": ["reviewed question"]}).status_code == 200
    assert client.get("/trace/regressions", headers=auth()).json()["items"][0]["replay_ready"] == 1
    evaluator = SimpleNamespace(run=AsyncMock(return_value=QualityScores(.9,.9,.9,.9)))
    monkeypatch.setattr(main, "_evaluator", evaluator)
    response = client.post(f"/trace/regressions/{trace_id}/run", headers=auth())
    assert response.status_code == 200
    case = evaluator.run.call_args.kwargs["dialog_cases"][0]
    assert case["turns"] == ["reviewed question"] and case["source_trace_id"] == trace_id
    assert len(case["source_span_id"]) == 16


def test_evaluation_and_replay_create_associated_trace_scores(backend, tmp_path):
    evaluator = EndToEndEvaluator(SimpleNamespace(run=AsyncMock(return_value=SimpleNamespace(
        response="answer", agent_type=SimpleNamespace(value="general"), intent=None))),
        SimpleNamespace(), "test-key", model="test-model", baseline_path=str(tmp_path / "baseline.json"))
    evaluator._judge = SimpleNamespace(judge=AsyncMock(return_value=QualityScores(.6,.6,.6,.6)))
    original = asyncio.run(evaluator._evaluate_dialog_case({"question": "PRIVATE QUESTION"}, 0))[0]
    backend.provider.force_flush(5000)
    trace_id = original.metadata["trace_id"]
    detail = backend.store.detail(trace_id)
    assert detail["score"] == .6 and detail["passed"] == 0
    assert detail["evaluations"][0]["scores"]["overall"] == .6
    assert "PRIVATE QUESTION" not in json.dumps(detail)
    source = next(s for s in detail["spans"] if s["name"] == "evaluation.dialog")
    replay = asyncio.run(evaluator._evaluate_dialog_case({"turns": ["fixture"], "source_trace_id": trace_id,
        "source_span_id": source["span_id"]}, 0))[0]
    backend.provider.force_flush(5000)
    assert replay.metadata["trace_id"] != trace_id
    span = next(s for s in backend.store.detail(replay.metadata["trace_id"])["spans"] if s["name"] == "evaluation.dialog")
    assert span["links"][0] == {"trace_id": trace_id, "span_id": source["span_id"]}


def test_background_experience_score_is_written_back_to_source(backend):
    from test_experience import manager, request, result
    async def run():
        m, req = manager(), request()
        m.judge.judge = AsyncMock(return_value=QualityScores(.6,.6,.6,.6))
        with request_context(req.request_id), observed("chat.pipeline"):
            trace_id = correlation_ids()["trace_id"]
            cid = await m.submit(req, result())
        backend.provider.force_flush(5000)
        await m.process(req.user_id, cid)
        return trace_id
    trace_id = asyncio.run(run())
    backend.provider.force_flush(5000)
    detail = backend.store.detail(trace_id)
    assert detail["score"] == .6 and detail["passed"] == 0
    assert detail["evaluations"][0]["evaluator_trace_id"] != trace_id
    assert backend.store.search(low_score=True)["total"] == 1


def test_linked_score_can_arrive_before_source_batch(backend):
    with observed("chat.pipeline"):
        source_id = correlation_ids()["trace_id"]
    with observed("experience.process"):
        record_scores(QualityScores(.6,.6,.6,.6), test_id="experience", passed=False, source_trace_id=source_id)
    source, evaluation = backend.exporter.get_finished_spans()
    backend.store.export([evaluation])
    backend.store.export([source])
    assert backend.store.detail(source_id)["score"] == .6


def test_deleting_evaluator_removes_source_score_and_expiring_source_removes_association(backend):
    with observed("chat.pipeline"):
        source_id = correlation_ids()["trace_id"]
    with observed("experience.process"):
        evaluator_id = correlation_ids()["trace_id"]
        record_scores(QualityScores(.6,.6,.6,.6), test_id="experience", passed=False, source_trace_id=source_id)
    backend.provider.force_flush(5000)
    assert backend.store.delete(evaluator_id)
    detail = backend.store.detail(source_id)
    assert detail["score"] is None and detail["evaluations"] == []
    with observed("experience.process"):
        record_scores(QualityScores(.6,.6,.6,.6), test_id="experience", passed=False, source_trace_id=source_id)
    backend.provider.force_flush(5000)
    with sqlite3.connect(backend.store.path) as db:
        db.execute("UPDATE traces SET start_ns=1 WHERE trace_id=?", (source_id,))
    assert backend.store.detail(source_id) is None
    with sqlite3.connect(backend.store.path) as db:
        assert db.execute("SELECT COUNT(*) FROM evaluations WHERE trace_id=?", (source_id,)).fetchone()[0] == 0


def test_bootstrap_enables_real_history_even_without_remote_export(backend, tmp_path, monkeypatch):
    monkeypatch.setattr(bootstrap, "_provider", None)
    monkeypatch.setattr(history, "_history", None)
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")
    monkeypatch.setenv("TRACE_HISTORY_ENABLED", "true")
    monkeypatch.setenv("TRACE_HISTORY_PATH", str(tmp_path / "configured.sqlite3"))
    monkeypatch.setenv("TRACE_EXPORT_RATIO", "0")
    configured = bootstrap.configure_tracing()
    with observed("chat.pipeline"):
        pass
    configured.force_flush(5000)
    assert history.get_history() is not None
    assert history.get_history().search()["total"] == 0
    assert backend.registry.get_sample_value("requests_total") == 1
    configured.shutdown()


def test_history_setup_failure_preserves_provider_and_metrics(backend, tmp_path, monkeypatch):
    monkeypatch.setattr(bootstrap, "_provider", None)
    monkeypatch.setattr(history, "_history", None)
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")
    monkeypatch.setenv("TRACE_HISTORY_ENABLED", "true")
    obstruction = tmp_path / "file"
    obstruction.write_text("obstruction")
    monkeypatch.setenv("TRACE_HISTORY_PATH", str(obstruction / "db.sqlite3"))
    configured = bootstrap.configure_tracing()
    with observed("chat.pipeline"):
        pass
    assert history.get_history() is None
    assert backend.registry.get_sample_value("requests_total") == 1
    configured.shutdown()


@pytest.mark.parametrize("ratio", [0, .5, 1])
def test_sampling_keeps_whole_trace_and_full_metrics(ratio):
    registry = CollectorRegistry()
    projection = MetricsProjection(registry)
    sink = InMemorySpanExporter()
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(MetricsSpanProcessor(projection))
    provider.add_span_processor(SimpleSpanProcessor(SamplingExporter(sink, ratio)))
    tracer = provider.get_tracer("test")
    for trace_id in (1, (1 << 64)-1):
        parent = NonRecordingSpan(SpanContext(trace_id, 42, False, TraceFlags(1)))
        with use_span(parent), tracer.start_as_current_span("chat.pipeline"):
            with tracer.start_as_current_span("agent.execute", attributes={"app.agent.name": "general"}):
                pass
    spans = sink.get_finished_spans()
    assert len(spans) == {0:0, .5:2, 1:4}[ratio]
    assert registry.get_sample_value("requests_total") == 2
    assert registry.get_sample_value("agent_latency_ms_count", {"agent":"general"}) == 2
    provider.shutdown()


def test_snapshot_bound_preserves_rank_without_text(backend):
    with observed("rag.rerank"):
        candidates([{"chunk_id":str(i), "score": float("nan"), "content":"PRIVATE"} for i in range(30)], "input")
    event = backend.exporter.get_finished_spans()[0].events[0]
    assert len(event.attributes["app.rag.candidate_refs"]) == 16
    assert event.attributes["app.rag.snapshot_truncated"] is True
    assert json.loads(event.attributes["app.rag.candidate_refs"][0])[2] is None


def test_malformed_snapshot_reference_and_old_source_id_do_not_break_business(backend):
    items = [{"chunk_id":"\ud800", "score":.5}, {"chunk_id":"valid", "score":.4}]
    with observed("rag.rerank"):
        assert candidates(items) is items
        record_scores(QualityScores(.8,.8,.8,.8), test_id="old-case", passed=True, source_trace_id=None)
    span = backend.exporter.get_finished_spans()[0]
    assert len(span.events[0].attributes["app.rag.candidate_refs"]) == 1
    assert json.loads(span.events[0].attributes["app.rag.candidate_refs"][0])[1] == 2


def test_actual_mmr_and_rerank_fallbacks_keep_input_and_output_snapshots(backend):
    from tooling.tool_manager import MCPToolManager
    manager = MCPToolManager("test-key", model="test-model")
    manager._client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(side_effect=TimeoutError())))
    items = [{"chunk_id":"a","score":.5,"content":"PRIVATE"}, {"chunk_id":"b","score":.4,"content":"PRIVATE"}]
    tool = SimpleNamespace(embedding_provider=AsyncMock(side_effect=TimeoutError()))
    assert asyncio.run(manager._mmr_select(tool, items, 1, .7)) == items[:1]
    assert asyncio.run(manager._rerank("PRIVATE QUESTION", items, 1)) == items[:1]
    for name in ("rag.mmr", "rag.rerank"):
        span = next(s for s in backend.exporter.get_finished_spans() if s.name == name)
        assert [e.attributes["app.rag.direction"] for e in span.events] == ["input", "output"]
        assert span.attributes["app.outcome"] == "degraded"
        assert len(span.events[0].attributes["app.rag.candidate_refs"]) == 2
        assert len(span.events[1].attributes["app.rag.candidate_refs"]) == 1


def test_configured_cost_uses_all_four_known_usage_fields_and_version(backend):
    prices = {"m": {"input":1., "output":2., "cache_read":.5, "cache_write":1.25, "currency":"USD", "version":"test-v1"}}
    backend.projection.prices = prices
    backend.store.policy = replace(backend.store.policy, prices=prices)
    attrs = {"gen_ai.request.model":"m", "gen_ai.provider.name":"anthropic", "app.llm.stage":"test",
        "gen_ai.usage.input_tokens":100, "gen_ai.usage.output_tokens":10,
        "app.llm.cache_read_tokens":20, "app.llm.cache_write_tokens":4}
    with observed("llm.generate", **attrs):
        trace_id = correlation_ids()["trace_id"]
    backend.provider.force_flush(5000)
    cost = backend.store.detail(trace_id)["spans"][0]["estimated_cost"]
    assert cost == {"amount": pytest.approx(.000135), "currency":"USD", "price_version":"test-v1"}
    assert backend.registry.get_sample_value("contextweave_llm_estimated_cost_total",
        {"provider":"anthropic","model":"m","stage":"test","currency":"USD","price_version":"test-v1"}) == pytest.approx(.000135)
    for key in ("gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens", "app.llm.cache_read_tokens", "app.llm.cache_write_tokens"):
        assert estimate_cost({k:v for k,v in attrs.items() if k != key}, prices) is None
    assert estimate_cost(attrs, {}) is None
    assert estimate_cost({**attrs,"gen_ai.usage.output_tokens":True}, prices) is None
    zeros = {k:(0 if "tokens" in k else v) for k,v in attrs.items()}
    assert estimate_cost(zeros, prices)[0] == 0


@pytest.mark.parametrize("document", ["export_ratio: .nan", "export_ratio: -0.1", "history: {max_traces: 0}", "prices: {m: {input: 1}}"])
def test_invalid_policy_rejected(tmp_path, monkeypatch, document):
    path = tmp_path / "policy.yml"
    path.write_text(document, encoding="utf-8")
    monkeypatch.setenv("TRACE_POLICY_PATH", str(path))
    with pytest.raises(ValueError):
        TracePolicy.load()


def test_export_failure_isolated_from_full_metrics(backend, monkeypatch):
    monkeypatch.setattr(backend.store, "export", lambda spans: (_ for _ in ()).throw(sqlite3.OperationalError("locked")))
    with observed("chat.pipeline"):
        pass
    assert backend.provider.force_flush(5000)
    assert backend.registry.get_sample_value("requests_total") == 1
    assert backend.registry.get_sample_value("contextweave_trace_export_failures_total") == 1


def test_invalid_policy_disables_detail_export_but_keeps_full_metrics(backend, tmp_path, monkeypatch):
    path = tmp_path / "invalid.yml"
    path.write_text("export_ratio: .nan", encoding="utf-8")
    sink = InMemorySpanExporter()
    monkeypatch.setenv("TRACE_POLICY_PATH", str(path))
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "console")
    monkeypatch.setenv("TRACE_HISTORY_ENABLED", "true")
    monkeypatch.setattr(bootstrap, "ConsoleSpanExporter", lambda: sink)
    monkeypatch.setattr(bootstrap, "_provider", None)
    monkeypatch.setattr(history, "_history", None)
    configured = bootstrap.configure_tracing()
    with observed("chat.pipeline"):
        pass
    configured.force_flush(5000)
    assert sink.get_finished_spans() == ()
    assert history.get_history() is None
    assert backend.registry.get_sample_value("requests_total") == 1
    configured.shutdown()


def test_provisioning_and_alert_mounts_are_consistent():
    from pathlib import Path
    compose = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    assert "./config/alerts:/etc/prometheus/alerts:ro" in compose["services"]["prometheus"]["volumes"]
    dashboard = json.loads(Path("config/grafana/dashboards/trace.json").read_text(encoding="utf-8"))
    assert len(dashboard["panels"]) == 12
    assert all(p["datasource"]["uid"] == "echomind-prometheus" for p in dashboard["panels"])
    rules = yaml.safe_load(Path("config/alerts/trace.yml").read_text(encoding="utf-8"))
    assert len(rules["groups"][0]["rules"]) == 5
