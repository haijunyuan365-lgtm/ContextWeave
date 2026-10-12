"""P1 integration contracts using real pipeline methods and offline providers."""
import ast
import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode, SpanKind

from agents.agent_orchestrator import AgentOrchestrator, AgentType, Request, TechnicalAgent, ResponseComposer, AgentResponse
from agents.tools import make_tool
from core.context_runtime import ContextBundle, RetrievalSession
from core.intent_recognizer import IntentRecognizer, IntentCategory, UrgencyLevel
from core.context_planner import ContextPlanner, LightweightContext
from mcp.knowledge_base import KnowledgeBase
from mcp.rag_config import RAGConfig
from mcp.tool_manager import MCPToolManager, Tool, CircuitState
from observability import observed, request_context, correlation_ids
from observability.bootstrap import get_provider, SafeSpanExporter
from observability.llm import generate
from observability.sanitization import safe_attributes


@pytest.fixture(scope="module")
def collector():
    exporter = InMemorySpanExporter()
    get_provider().add_span_processor(SimpleSpanProcessor(SafeSpanExporter(exporter)))
    return exporter


@pytest.fixture
def spans(collector):
    collector.clear()
    return collector


def named(spans, name):
    return [s for s in spans.get_finished_spans() if s.name == name]


def response(text, usage=None):
    return SimpleNamespace(content=[{"type": "text", "text": text}], usage=usage,
                           model="actual-model", stop_reason="end_turn")


def client(text, usage=None):
    return SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(return_value=response(text, usage))))


def document(key):
    return {"chunk_id": key, "title": "private title", "content": "private document",
            "score": .5, "retrieval_sources": ["vector"]}


def rag_service():
    kb = object.__new__(KnowledgeBase)
    kb.rag_config = RAGConfig(rewrite_count=1, rerank_candidates=3, mmr_candidates=4, final_top_k=2)
    kb._vector_search = Mock(return_value=[document(k) for k in "abc"])
    kb._bm25_search = Mock(return_value=[document("b")])
    manager = MCPToolManager("test-key", model="test-model", rag_config=kb.rag_config)
    async def create(**kwargs):
        await asyncio.sleep(.001)
        text = '["expanded"]' if "将以下用户查询改写" in kwargs["messages"][0]["content"] else "[2, 0, 1]"
        return response(text, {"input_tokens": 20, "output_tokens": 4})
    manager._client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(side_effect=create)))
    tool = Tool("knowledge_search", "", kb.search_handler, {}, cache_ttl=60,
                embedding_provider=AsyncMock(return_value={"a": [1., 0.], "b": [.99, .01], "c": [0., 1.]}))
    manager.register(tool)
    return manager, kb, tool


def test_real_intent_branches_are_siblings_and_cache_does_not_fake_calls(spans):
    recognizer = IntentRecognizer("test-key", model="test-model")
    recognizer.client = client('{"intent":"refund","confidence":1,"reasoning":"private reason"}',
                               {"input_tokens": 50, "output_tokens": 7})
    async def run():
        with request_context("intent-1"), observed("intent.recognize", span_type="intent"):
            await recognizer.recognize("退款 private query")
        with request_context("intent-2"):
            await recognizer.recognize("退款 private query")
    asyncio.run(run())
    roots = named(spans, "intent.recognize")
    assert len(roots) == 2
    original = next(s for s in roots if s.attributes["app.request_id"] == "intent-1")
    cached = next(s for s in roots if s.attributes["app.request_id"] == "intent-2")
    for name in ("intent.llm", "intent.embedding", "intent.pattern"):
        branch = named(spans, name)
        assert len(branch) == 1 and branch[0].parent.span_id == original.context.span_id
        assert "app.intent.name" in branch[0].attributes
    assert cached.attributes["app.cache_hit"] is True
    assert original.context.trace_id != cached.context.trace_id
    assert recognizer.client.messages.create.await_count == 1
    llm = named(spans, "llm.generate")[0]
    assert llm.parent.span_id == named(spans, "intent.llm")[0].context.span_id
    assert llm.attributes["gen_ai.usage.input_tokens"] == 50


def test_intent_llm_failure_keeps_local_fallback_visible(spans):
    recognizer = IntentRecognizer("test-key")
    recognizer.client = client("")
    recognizer.client.messages.create.side_effect = RuntimeError("private exception")
    result = asyncio.run(recognizer.recognize("退款"))
    assert result.intent != IntentCategory.OTHER
    assert named(spans, "intent.llm")[0].attributes["app.fallback_used"] is True
    assert named(spans, "intent.recognize")[0].attributes["app.outcome"] == "degraded"
    assert named(spans, "llm.generate")[0].status.status_code == StatusCode.ERROR


def test_parallel_context_sources_and_cached_calls_keep_correct_parent(spans):
    async def load(*args, **kwargs):
        await asyncio.sleep(.005)
        return []
    mem = SimpleNamespace(search_episodic=AsyncMock(side_effect=load),
                          get_profile=AsyncMock(return_value={}))
    rag = SimpleNamespace(search_with_rewrite=AsyncMock(return_value=SimpleNamespace(success=True, data=[], error=None)))
    session = RetrievalSession("private user", "private conv", ContextBundle(), mem, rag)
    async def run():
        with request_context("context"), observed("context.retrieve") as parent:
            await asyncio.gather(*(session.retrieve(source, "private query") for source in ("memory", "profile", "knowledge")))
            await session.retrieve("knowledge", "private query")
            await session.close()
            return parent.get_span_context().span_id
    parent_id = asyncio.run(run())
    children = named(spans, "context.source.retrieve")
    assert len(children) == 4 and all(s.parent.span_id == parent_id for s in children)
    assert sum(s.attributes["app.cache_hit"] for s in children) == 1
    assert rag.search_with_rewrite.await_count == 1
    assert {s.attributes["app.context.source"] for s in children} == {"memory", "profile", "knowledge"}


def test_dense_bm25_execute_in_threads_as_siblings_and_rrf_counts(spans):
    kb = object.__new__(KnowledgeBase)
    kb.rag_config = RAGConfig()
    barrier = threading.Barrier(2)
    def search(*args):
        barrier.wait(timeout=3)
        return [document("a"), document("b")]
    kb._vector_search = search
    kb._bm25_search = search
    async def run():
        with request_context("hybrid"):
            return await kb.search_async("private query", 3)
    assert len(asyncio.run(run())) == 2
    root = named(spans, "rag.hybrid")[0]
    for name in ("rag.dense", "rag.bm25", "rag.rrf.stage1"):
        child = named(spans, name)[0]
        assert child.parent.span_id == root.context.span_id
        assert child.attributes["app.request_id"] == "hybrid"
    fusion = named(spans, "rag.rrf.stage1")[0]
    assert fusion.attributes["app.rag.input_count"] == 4 and fusion.attributes["app.rag.output_count"] == 2


@pytest.mark.parametrize("both_fail", [False, True])
def test_hybrid_failure_is_located_at_failed_branch(spans, both_fail):
    manager, kb, _ = rag_service()
    kb._vector_search.side_effect = RuntimeError("private exception")
    if both_fail:
        kb._bm25_search.side_effect = RuntimeError("private exception")
        with pytest.raises(RuntimeError):
            asyncio.run(kb.search_async("private query", 3))
    else:
        assert asyncio.run(kb.search_async("private query", 3))
    assert named(spans, "rag.dense")[0].status.status_code == StatusCode.ERROR
    root = named(spans, "rag.hybrid")[0]
    assert root.attributes["app.outcome"] == ("error" if both_fail else "degraded")
    assert "vector" in root.attributes["app.rag.failed_sources"]


def test_rag_pipeline_counts_query_indexes_llm_usage_and_cache(spans):
    manager, kb, _ = rag_service()
    async def run(request_id):
        with request_context(request_id):
            return await manager.search_with_rewrite("knowledge_search", "private original", top_k=2)
    assert len(asyncio.run(run("rag-1")).data) == 2
    root = named(spans, "rag.pipeline")[0]
    assert root.attributes["app.rag.subquery_count"] == 2
    assert root.attributes["app.rag.output_count"] == 2
    queries = named(spans, "rag.query")
    assert {s.attributes["app.rag.query_index"] for s in queries} == {0, 1}
    assert all(s.parent.span_id == root.context.span_id for s in queries)
    assert len(named(spans, "rag.hybrid")) == 2
    assert named(spans, "rag.rrf.stage2")[0].attributes["app.rag.input_count"] == 6
    assert named(spans, "rag.rrf.stage2")[0].attributes["app.rag.output_count"] == 3
    assert named(spans, "rag.mmr")[0].attributes["app.rag.input_count"] == 3
    assert named(spans, "rag.rerank")[0].attributes["app.rag.output_count"] == 2
    calls = named(spans, "llm.generate")
    assert {s.attributes["app.llm.stage"] for s in calls} == {"rag.rewrite", "rag.rerank"}
    assert sum(s.attributes["gen_ai.usage.input_tokens"] for s in calls) == 40
    spans.clear()
    asyncio.run(run("rag-2"))
    assert not named(spans, "rag.hybrid") and not named(spans, "rag.dense")
    assert all(s.attributes["app.cache_hit"] for s in named(spans, "tool.execute"))


def test_concurrent_rag_requests_do_not_mix_query_context(spans):
    manager, _, _ = rag_service()
    async def run():
        async def request(value):
            with request_context(value), observed("request"):
                await manager.search_with_rewrite("knowledge_search", value, 2)
        await asyncio.gather(request("r1"), request("r2"))
    asyncio.run(run())
    for value in ("r1", "r2"):
        tree = [s for s in spans.get_finished_spans() if s.attributes.get("app.request_id") == value]
        assert len(tree) >= 12 and len({s.context.trace_id for s in tree}) == 1
    assert len({s.context.trace_id for s in named(spans, "rag.pipeline")}) == 2


def test_mmr_and_rerank_fallbacks_degrade_pipeline_without_changing_api(spans):
    manager, _, tool = rag_service()
    tool.embedding_provider.side_effect = RuntimeError("private embedding failure")
    original_create = manager._client.messages.create.side_effect
    async def create(**kwargs):
        if "按相关性打分" in kwargs["messages"][0]["content"]:
            raise RuntimeError("private rerank failure")
        return await original_create(**kwargs)
    manager._client.messages.create.side_effect = create
    result = asyncio.run(manager.search_with_rewrite("knowledge_search", "query", 2))
    assert result.success and len(result.data) == 2
    for name in ("rag.mmr", "rag.rerank", "rag.pipeline"):
        assert named(spans, name)[0].attributes["app.outcome"] == "degraded"
        assert named(spans, name)[0].attributes["app.fallback_used"] is True


def test_empty_retrieval_does_not_invent_mmr_rerank_llm_steps(spans):
    manager, kb, _ = rag_service()
    kb._vector_search.return_value = []
    kb._bm25_search.return_value = []
    result = asyncio.run(manager.search_with_rewrite("knowledge_search", "query", 2))
    assert result.success and result.data == []
    assert not named(spans, "rag.mmr") and not named(spans, "rag.rerank")
    assert len(named(spans, "llm.generate")) == 1


@pytest.mark.parametrize("mode,expected,technical,business", [
    ("business", "error", True, False), ("timeout", "error", False, False),
    ("fallback", "degraded", False, True), ("breaker", "degraded", False, True),
    ("fallback_business", "error", False, False),
])
def test_mcp_tool_success_dimensions_timeout_and_breaker(spans, mode, expected, technical, business):
    manager = MCPToolManager("test-key")
    async def handler(*args):
        if mode in {"timeout", "fallback", "fallback_business"}:
            await asyncio.sleep(1)
        return {"success": False}
    tool = Tool("test_tool", "", handler, {}, timeout_s=.001,
                fallback=(lambda *args: {"success": mode != "fallback_business"}) if mode in {"fallback", "breaker", "fallback_business"} else None)
    if mode == "breaker":
        tool.breaker.state = CircuitState.OPEN
        import time
        tool.breaker.opened_at = time.monotonic()
    manager.register(tool)
    result = asyncio.run(manager.call("test_tool", {}))
    span = named(spans, "tool.execute")[0]
    assert span.attributes["app.outcome"] == expected
    assert span.attributes["app.tool.success"] is technical
    assert span.attributes["app.tool.result_success"] is business
    assert result.success is (mode != "timeout")  # legacy technical-call semantics
    if mode in {"timeout", "fallback", "fallback_business"}:
        assert span.attributes["app.error_code"] == "TIMEOUT"
    if mode == "breaker":
        assert span.attributes["app.error_code"] == "CIRCUIT_OPEN" and span.attributes["app.tool.circuit_open"]


@pytest.mark.parametrize("mode", ["business", "exception", "cached"])
def test_agent_tool_spans_match_legacy_trace_without_exporting_arguments(spans, mode):
    model = client("")
    model.messages.create.side_effect = [
        SimpleNamespace(content=[{"type": "tool_use", "id": "call", "name": "test_tool", "input": {"query": "private input"}}]),
        response("final answer")]
    def handler(req, args):
        if mode == "exception":
            raise RuntimeError("private exception")
        return {"success": mode != "business", "cached": mode == "cached"}
    agent = TechnicalAgent(model, "test-model")
    agent.get_tools = Mock(return_value={"test_tool": make_tool("test_tool", "", {"query": {"type": "string"}}, handler)})
    req = Request("private question", "private user", "private conv", intent=IntentCategory.TECHNICAL_LOGIN)
    async def run():
        with request_context(req.request_id), observed("agent.execute", span_type="agent", **{"app.agent.name": "technical"}):
            return await agent.handle(req)
    result = asyncio.run(run())
    assert result.success
    span = named(spans, "tool.execute")[0]
    legacy = result.tool_traces[0]
    assert span.attributes["app.tool.success"] == legacy["success"]
    assert span.attributes["app.tool.result_success"] is (mode == "cached")
    assert span.attributes["app.cache_hit"] == legacy["cached"]
    assert span.attributes["app.outcome"] == ("success" if mode == "cached" else "error")
    assert "private input" in str(legacy["input"]) and "private input" not in str(span.attributes)
    assert [s.attributes["app.llm.round"] for s in named(spans, "llm.generate")] == [1, 2]


def test_agent_fallback_names_actual_attempts_and_llm_models(spans):
    orch = AgentOrchestrator("test-key", model="test")
    orch._pool[AgentType.TECHNICAL][0]._client = client("")
    orch._pool[AgentType.TECHNICAL][0]._client.messages.create.side_effect = RuntimeError("private exception")
    orch._pool[AgentType.GENERAL][0]._client = client("answer", {"input_tokens": 10, "output_tokens": 5})
    req = Request("登录失败", "u", "c", intent=IntentCategory.TECHNICAL_LOGIN, urgency=UrgencyLevel.LOW)
    asyncio.run(orch.run(req))
    agents = named(spans, "agent.execute")
    assert [s.attributes["app.agent.name"] for s in agents] == ["technical", "general"]
    assert len({s.parent.span_id for s in agents}) == 1
    assert agents[0].attributes["app.outcome"] == "error"
    assert agents[1].attributes["app.fallback_used"] is True
    assert [s.attributes["app.agent.name"] for s in named(spans, "llm.generate")] == ["technical", "general"]


def test_composer_fallback_is_explicit_and_preserves_responses(spans):
    model = client("")
    model.messages.create.side_effect = RuntimeError("private exception")
    composer = ResponseComposer(model, "test")
    req = Request("question", "u", "c")
    result = asyncio.run(composer.compose(req, [AgentResponse(AgentType.TECHNICAL, "tech", True),
                                               AgentResponse(AgentType.BILLING, "billing", True)]))
    assert "tech" in result and "billing" in result
    assert named(spans, "agent.compose")[0].attributes["app.fallback_used"] is True


@pytest.mark.parametrize("usage,expected", [
    (None, {}), ({"input_tokens": 0}, {"gen_ai.usage.input_tokens": 0}),
    (SimpleNamespace(input_tokens=12, output_tokens=3, cache_read_input_tokens=7, cache_creation_input_tokens=2),
     {"gen_ai.usage.input_tokens": 12, "gen_ai.usage.output_tokens": 3, "app.llm.cache_read_tokens": 7, "app.llm.cache_write_tokens": 2}),
    ({"input_tokens": None, "output_tokens": -1, "cache_read_input_tokens": True}, {}),
])
def test_llm_usage_uses_returned_values_and_keeps_missing_unknown(spans, usage, expected):
    model = client("private answer", usage)
    asyncio.run(generate(model, stage="test", model="requested-model", messages=[{"role": "user", "content": "private prompt"}]))
    span = named(spans, "llm.generate")[0]
    assert span.kind == SpanKind.CLIENT
    assert span.attributes["gen_ai.request.model"] == "requested-model"
    assert span.attributes["gen_ai.response.model"] == "actual-model"
    actual = {k: v for k, v in span.attributes.items() if k.startswith("gen_ai.usage.") or k in {"app.llm.cache_read_tokens", "app.llm.cache_write_tokens"}}
    assert actual == expected
    assert "private" not in str(span.attributes)


def test_planner_timeout_is_distinct_from_llm_task_cancellation(spans):
    model = client("")
    async def slow(**kwargs):
        await asyncio.sleep(1)
    model.messages.create.side_effect = slow
    planner = ContextPlanner(model, "test")
    planner.timeout = .002
    from core.intent_recognizer import IntentResult
    intent = IntentResult(IntentCategory.OTHER, .1, UrgencyLevel.LOW, "other", {}, "", 0)
    async def run():
        with observed("context.plan"):
            return await planner.plan("uncertain", intent, LightweightContext())
    assert asyncio.run(run()).planner_source == "fallback"
    assert named(spans, "context.plan")[0].attributes["app.error_code"] == "TIMEOUT"
    assert named(spans, "llm.generate")[0].attributes["app.error_code"] == "CANCELLED"


def test_every_existing_model_call_uses_the_shared_wrapper():
    root = Path(__file__).resolve().parents[1]
    stages = set()
    for directory in ("agents", "core", "memory", "evaluation", "mcp"):
        for path in (root / directory).glob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.Call):
                    assert not (isinstance(node.func, ast.Attribute) and node.func.attr == "create"
                                and isinstance(node.func.value, ast.Attribute) and node.func.value.attr == "messages"), path
                    if isinstance(node.func, ast.Name) and node.func.id == "generate":
                        stages.update(k.value.value for k in node.keywords if k.arg == "stage" and isinstance(k.value, ast.Constant))
    assert stages == {"agent", "agent.compose", "intent", "context.plan", "rag.rewrite", "rag.rerank",
                      "memory.compress", "memory.merge_summary", "memory.extract", "evaluation.judge"}


def test_index_metadata_is_allowed_but_private_query_is_filtered():
    assert safe_attributes({"app.rag.query_index": 1, "app.rag.subquery_count": 3, "app.query": "secret"}) == {
        "app.rag.query_index": 1, "app.rag.subquery_count": 3}


def test_experience_process_uses_new_trace_link_and_restores_calling_context(spans):
    from test_experience import manager, request, result
    m, req = manager(), request()
    async def run():
        with request_context(req.request_id), observed("chat.pipeline") as parent:
            cid = await m.submit(req, result())
            source = await m.store.read(req.user_id, cid)
            await m.process(req.user_id, cid)
            assert correlation_ids()["trace_id"] == format(parent.get_span_context().trace_id, "032x")
            assert correlation_ids()["request_id"] == req.request_id
            return source
    source = asyncio.run(run())
    worker = named(spans, "experience.process")[0]
    submission = named(spans, "experience.submit")[0]
    assert worker.parent is None and worker.context.trace_id != submission.context.trace_id
    assert worker.attributes["app.request_id"] == req.request_id
    assert worker.attributes["app.operation"] == "experience"
    assert len(worker.links) == 1 and worker.links[0].context.span_id == submission.context.span_id
    assert source["source_trace_id"] == format(submission.context.trace_id, "032x")
    assert source["source_span_id"] == format(submission.context.span_id, "016x")


def test_experience_worker_task_starts_without_request_context(spans):
    from test_experience import manager
    m = manager()
    async def run():
        done = asyncio.Event()
        async def worker():
            assert correlation_ids() == {"request_id": "", "trace_id": "", "span_id": ""}
            done.set()
        m._run = worker
        with request_context("calling-request"), observed("chat.pipeline"):
            await m.start()
            await asyncio.wait_for(done.wait(), 1)
            await m.close()
    asyncio.run(run())


def test_legacy_experience_case_without_link_is_still_processed(spans):
    from test_experience import manager, request, result
    m, req = manager(), request()
    async def run():
        cid = await m.submit(req, result())
        case = await m.store.read(req.user_id, cid)
        case.pop("source_trace_id")
        case.pop("source_span_id")
        await m.store.write(case)
        await m.process(req.user_id, cid)
        return await m.store.read(req.user_id, cid)
    assert asyncio.run(run())["status"] == "active"
    assert not named(spans, "experience.process")[0].links


def test_experience_storage_failure_is_traced(spans):
    from test_experience import manager
    m = manager()
    m.store.read = AsyncMock(side_effect=RuntimeError("private storage failure"))
    with pytest.raises(RuntimeError):
        asyncio.run(m.process("private user", "private case"))
    worker = named(spans, "experience.process")[0]
    assert worker.parent is None and worker.status.status_code == StatusCode.ERROR
    assert "private user" not in str(worker.attributes)


def test_context_single_flight_has_one_backend_branch_and_three_call_spans(spans):
    async def search(*args, **kwargs):
        await asyncio.sleep(.005)
        with observed("backend"):
            return SimpleNamespace(success=True, data=[], error=None)
    rag = SimpleNamespace(search_with_rewrite=AsyncMock(side_effect=search))
    session = RetrievalSession("u", "c", ContextBundle(), knowledge=rag)
    async def run():
        with request_context("single-flight"), observed("context.retrieve"):
            await asyncio.gather(*(session.retrieve("knowledge", "same") for _ in range(3)))
    asyncio.run(run())
    calls = named(spans, "context.source.retrieve")
    assert len(calls) == 3 and sum(s.attributes["app.cache_hit"] for s in calls) == 2
    assert len(named(spans, "backend")) == 1
    assert named(spans, "backend")[0].parent.span_id in {s.context.span_id for s in calls}
    assert rag.search_with_rewrite.await_count == 1


def test_context_provider_timeout_is_not_reported_as_generic_failure(spans):
    memory = SimpleNamespace(get_profile=AsyncMock(side_effect=asyncio.TimeoutError()))
    session = RetrievalSession("u", "c", ContextBundle(), memory=memory)
    result = asyncio.run(session.retrieve("profile"))
    assert result.status == "failed"
    assert named(spans, "context.source.retrieve")[0].attributes["app.error_code"] == "TIMEOUT"


def test_direct_agent_handle_binds_request_without_duplicate_span(spans):
    req = Request("登录失败", "u", "c", intent=IntentCategory.TECHNICAL_LOGIN)
    agent = TechnicalAgent(client("answer"), "test")
    asyncio.run(agent.handle(req))
    roots = named(spans, "agent.execute")
    assert len(roots) == 1 and roots[0].parent is None
    assert roots[0].attributes["app.request_id"] == req.request_id
    assert named(spans, "llm.generate")[0].parent.span_id == roots[0].context.span_id


def test_direct_parallel_orchestration_has_one_root_and_correct_request(spans):
    from agents.agent_orchestrator import RoutingDecision
    orch = AgentOrchestrator("test-key", model="test")
    for agents in orch._pool.values():
        agents[0]._client = client("answer")
    orch._composer._client = client("combined")
    req = Request("登录失败且重复扣款", "u", "c", intent=IntentCategory.TECHNICAL_LOGIN)
    decision = orch._route_decision(req)
    assert isinstance(decision, RoutingDecision) and decision.multi_agent
    asyncio.run(orch.run_parallel(req, decision))
    roots = named(spans, "agent.orchestrate")
    assert len(roots) == 1 and roots[0].parent is None
    assert all(s.attributes["app.request_id"] == req.request_id for s in spans.get_finished_spans())
    assert len(named(spans, "agent.execute")) == 2


def test_experience_judge_usage_is_in_worker_trace_and_retries_use_new_traces(spans):
    from test_experience import manager, request, result
    from evaluation.evaluator import LLMJudge
    m, req = manager(), request()
    model = client('{"relevance":0.9,"accuracy":0.9,"completeness":0.9,"helpfulness":0.9}',
                   {"input_tokens": 80, "output_tokens": 12})
    model.messages.create.side_effect = [RuntimeError("private failure"), model.messages.create.return_value]
    m.judge = LLMJudge(model, "test")
    async def run():
        cid = await m.submit(req, result())
        await m.process(req.user_id, cid)
        await m.process(req.user_id, cid)
        return await m.store.read(req.user_id, cid)
    assert asyncio.run(run())["status"] == "active"
    workers = named(spans, "experience.process")
    assert len(workers) == 2 and len({s.context.trace_id for s in workers}) == 2
    assert all(s.parent is None and len(s.links) == 1 for s in workers)
    assert workers[0].attributes["app.outcome"] == "error"
    assert workers[1].attributes["app.experience.attempt"] == 2
    calls = named(spans, "llm.generate")
    assert all(s.attributes["app.request_id"] == req.request_id for s in calls)
    assert {s.parent.span_id for s in calls} == {s.context.span_id for s in workers}
    assert calls[1].attributes["gen_ai.usage.input_tokens"] == 80
    assert calls[1].attributes["app.operation"] == "experience"
