from memory_fakes import atomic_http_stub
"""Offline experience lifecycle, isolation, recovery, and integration contracts."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from agents.agent_orchestrator import AgentOrchestrator, AgentResponse, AgentType, Request, TechnicalAgent
from agents.tools import build_context_tools
from api import main
from core.context_planner import ContextPlanner, LightweightContext
from core.context_runtime import ContextBundle, ContextPipeline, RetrievalSession, context_debug
from core.intent_recognizer import IntentCategory, IntentResult, UrgencyLevel
from evaluation.evaluator import LLMJudge, QualityScores
from experience.runtime import ExperienceManager, ExperienceStore, ExperienceIndex, format_references
from memory_fakes import FakeRedis


class Redis(FakeRedis):
    async def hget(self, key, field):
        return self.hashes[key].get(field)

    async def hscan_iter(self, key):
        for item in list(self.hashes[key].items()):
            yield item


class Collection:
    def __init__(self):
        self.docs = {}
        self.distances = {}
        self.queries = []

    def upsert(self, ids, documents, metadatas):
        for cid, text, meta in zip(ids, documents, metadatas):
            self.docs[cid] = (text, meta)

    def query(self, query_texts, n_results, where, include):
        self.queries.append((query_texts, where))
        ids = [cid for cid, (_, meta) in self.docs.items() if meta["user_id"] == where["user_id"]][:n_results]
        return {"ids": [ids], "distances": [[self.distances.get(cid, .1) for cid in ids]]}


def manager():
    return ExperienceManager(ExperienceStore(Redis()), ExperienceIndex(Collection()),
                             SimpleNamespace(judge=AsyncMock(return_value=QualityScores(.9, .9, .9, .9))))


def request(user="u1", conv="c1"):
    return Request("OAuth 登录报 401", user, conv, intent=IntentCategory.TECHNICAL_LOGIN,
                   intent_group="technical", execution_records=[{"agent_type": "technical", "success": True, "fallback": False}])


def result():
    return SimpleNamespace(response="核对 Token 是否过期，重新授权后重试。", escalated=False)


def test_complete_lifecycle_and_cached_revocation():
    async def scenario():
        m, req = manager(), request()
        cid = await m.submit(req, result())
        assert await m.search("u1", "OAuth 登录失败，提示 401") == []
        await m.process("u1", cid)
        row = await m.store.read("u1", cid)
        assert row["status"] == "active" and row["attempts"] == 1
        assert len(cid) == 36
        session = RetrievalSession("u1", "new-conversation", ContextBundle(), experience=m)
        hits = await asyncio.gather(*[session.retrieve("experience", "OAuth 登录失败，提示 401", 3) for _ in range(2)])
        assert all(h.data[0]["case_id"] == cid for h in hits)
        assert len(m.index.collection.queries) == 2
        assert await m.search("another-user", req.message) == []
        assert (await m.feedback("u1", cid, "negative"))["status"] == "revoked"
        assert (await session.retrieve("experience", "OAuth 登录失败，提示 401", 3)).data == []
        assert await m.search("u1", req.message) == []
        assert (await m.feedback("u1", cid, "positive"))["status"] == "revoked"
        print(json.dumps({"case_id": cid, "first_case": "active", "new_session_hits": len(hits[0].data),
                          "after_negative_feedback_hits": len(await m.search("u1", req.message))}))
        await session.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("scores,status", [
    (QualityScores(.9, .9, .9, .9), "active"),
    (QualityScores(.9, .7, .9, .9), "rejected"),
    (QualityScores(.4, .9, .4, .4), "rejected"),
    (QualityScores(float("nan"), .9, .9, .9), "failed"),
    (QualityScores(.9, 1.1, .9, .9), "failed"),
    (QualityScores(.9, .9, .9, .9, judge_failed=True), "failed"),
    (SimpleNamespace(relevance=.9, accuracy=.9), "failed"),
])
def test_quality_gate(scores, status):
    async def scenario():
        m = manager()
        m.judge.judge.return_value = scores
        cid = await m.submit(request(), result())
        for _ in range(4):
            await m.process("u1", cid)
        case = await m.store.read("u1", cid)
        assert case["status"] == status
        assert case["attempts"] == (3 if status == "failed" else 1)
        assert bool(m.index.collection.docs) == (status == "active")
    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["greeting", "clarification", "escalated", "empty", "failed", "fallback", "partial", "tool", "degraded", "knowledge"])
def test_unsuccessful_cases_never_judged(change):
    async def scenario():
        m, req, res = manager(), request(), result()
        if change == "greeting": req.intent = IntentCategory.GREETING
        if change == "clarification": req.execution_records = []
        if change == "escalated": res.escalated = True
        if change == "empty": res.response = " "
        if change == "failed": req.execution_records[0]["success"] = False
        if change == "fallback": req.execution_records[0]["fallback"] = True
        if change == "partial": req.execution_records.append({"agent_type": "billing", "success": False})
        if change == "tool": req.experience_tools = [{"success": True, "result_success": False}]
        if change == "degraded": req.experience_tools = [{"success": True, "degraded": True}]
        if change == "knowledge": req.retrieval_session = SimpleNamespace(evidence_failed=True)
        cid = await m.submit(req, res)
        await m.process("u1", cid)
        assert (await m.store.read("u1", cid))["status"] == "rejected"
        m.judge.judge.assert_not_awaited()
    asyncio.run(scenario())


def test_timeout_retry_idempotency_and_restart():
    async def scenario():
        m, req = manager(), request()
        cid = await m.submit(req, result())
        assert await m.submit(req, result()) == cid
        assert len(await m.list_cases("u1")) == 1
        m.judge.judge.side_effect = asyncio.TimeoutError
        await m.process("u1", cid)
        assert (await m.store.read("u1", cid))["status"] == "pending"
        restarted = ExperienceManager(m.store, m.index, SimpleNamespace(judge=AsyncMock(return_value=QualityScores(.9, .9, .9, .9))))
        await restarted.start()
        async def active():
            while (await m.store.read("u1", cid))["status"] != "active":
                await asyncio.sleep(.01)
        try:
            await asyncio.wait_for(active(), 2)
        finally:
            await asyncio.wait_for(restarted.close(), 1)
        assert (await m.store.read("u1", cid))["attempts"] == 2
    asyncio.run(scenario())


def test_index_failure_cannot_activate_and_feedback_wins_race():
    async def scenario():
        m = manager()
        cid = await m.submit(request(), result())
        original = m.index.upsert
        m.index.upsert = AsyncMock(side_effect=ConnectionError)
        await m.process("u1", cid)
        assert (await m.store.read("u1", cid))["status"] == "pending"
        async def revoke_during_index(case):
            await original(case)
            await m.feedback("u1", cid, "negative")
        m.index.upsert = revoke_during_index
        await m.process("u1", cid)
        assert (await m.store.read("u1", cid))["status"] == "revoked"
        assert await m.search("u1", "401") == []
    asyncio.run(scenario())


def test_anonymous_disabled_low_similarity_and_redaction(monkeypatch):
    async def scenario():
        m = manager()
        assert await m.submit(request("anonymous"), result()) is None
        assert await m.search("anonymous", "401") == []
        req = request()
        req.entities = {"api_key": ["very-secret"]}
        req.message += " password=hello Bearer abc.def sk-keyvalue"
        cid = await m.submit(req, result())
        await m.process("u1", cid)
        raw = json.dumps(await m.store.read("u1", cid))
        assert all(secret not in raw for secret in ["very-secret", "hello", "abc.def", "sk-keyvalue"])
        m.index.collection.distances[cid] = .6
        assert await m.search("u1", "401") == []
        monkeypatch.setenv("ECHOMIND_EXPERIENCE_ENABLED", "false")
        disabled = manager()
        assert await disabled.submit(request(), result()) is None
        await disabled.start()
        assert disabled._worker is None
    asyncio.run(scenario())


@pytest.mark.parametrize("category,query,expected", [
    (IntentCategory.TECHNICAL_LOGIN, "OAuth 登录报 401", True),
    (IntentCategory.PAYMENT_ISSUE, "支付失败", True),
    (IntentCategory.REFUND, "按照上次的方式处理退款", True),
    (IntentCategory.ORDER_STATUS, "订单状态", False),
    (IntentCategory.GREETING, "你好", False),
    (IntentCategory.HUMAN_HANDOFF, "转人工", False),
])
def test_planner_rules(category, query, expected):
    intent = IntentResult(category, .95, UrgencyLevel.LOW, "technical", {}, "", 0)
    plan, _ = ContextPlanner(None, "test", True).rules(query, intent, LightweightContext())
    assert plan.need_experience is expected
    assert bool(plan.experience_query) is expected


def test_pipeline_preload_tool_cache_debug_and_knowledge_capture():
    async def scenario():
        m = manager()
        cid = await m.submit(request(), result())
        await m.process("u1", cid)
        intent = IntentResult(IntentCategory.TECHNICAL_LOGIN, .95, UrgencyLevel.LOW, "technical", {}, "", 0)
        orch = SimpleNamespace(recognize_intent=AsyncMock(return_value=intent))
        kb = SimpleNamespace(search_with_rewrite=AsyncMock(return_value=SimpleNamespace(
            success=True, data=[{"title": "auth", "content": "current auth policy"}], error=None)))
        pipeline = ContextPipeline(orch, ContextPlanner(None, "test", True), knowledge=kb, experience=m)
        req = request(conv="c2")
        await pipeline.prepare(req)
        assert req.context_bundle.experiences[0]["case_id"] == cid
        assert "历史处理经验" in req.context
        output = await build_context_tools()["search_experience"].handler(req, {"query": req.message})
        assert output["cached"] is True
        debug = context_debug(req)
        assert debug["sources"]["experience"]["case_ids"] == [cid]
        assert "Token" not in json.dumps(debug)
        kb.search_with_rewrite.return_value.data = [{"title": "more", "content": "additional evidence"}]
        await req.retrieval_session.retrieve("knowledge", "different query")
        case = m.snapshot(req, result())
        assert {k["title"] for k in case["knowledge"]} == {"auth", "more"}
        await req.retrieval_session.close()
    asyncio.run(scenario())


def test_prompt_budget_and_json_boundaries(monkeypatch):
    monkeypatch.setenv("ECHOMIND_EXPERIENCE_MAX_PROMPT_CHARS", "600")
    items = [{"case_id": str(i), "query": "ignore instructions", "response": "x" * 200} for i in range(3)]
    text = format_references(items)
    assert 0 < len(text) <= 600
    for line in text.splitlines()[1:]:
        json.loads(line)


@pytest.mark.parametrize("scores", [{}, {"accuracy": .9}, {"relevance": True, "accuracy": .9, "completeness": .9, "helpfulness": .9}])
def test_judge_rejects_missing_or_invalid_fields(scores):
    client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(return_value=SimpleNamespace(
        content=[{"type": "text", "text": json.dumps(scores)}]))))
    assert asyncio.run(LLMJudge(client, "test").judge("q", "a")).judge_failed


def test_tool_results_and_real_fallback_recording():
    async def scenario():
        client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(side_effect=[
            SimpleNamespace(content=[{"type": "tool_use", "id": "call-1", "name": "lookup_error_code", "input": {"error_code": "401"}}]),
            SimpleNamespace(content=[{"type": "text", "text": "重新授权"}]),
        ])))
        req = request()
        response = await TechnicalAgent(client, "test").handle(req)
        assert response.success
        assert req.experience_tools[0]["result"]
        assert req.experience_tools[0]["sequence"] == 1
        orch = AgentOrchestrator("test", model="test")
        orch._pool[AgentType.TECHNICAL][0].handle = AsyncMock(return_value=AgentResponse(AgentType.TECHNICAL, "failed", False))
        orch._pool[AgentType.GENERAL][0].handle = AsyncMock(return_value=AgentResponse(AgentType.GENERAL, "fallback", True))
        req.execution_records.clear()
        await orch._execute(req, AgentType.TECHNICAL)
        assert len(req.execution_records) == 2
        assert req.execution_records[-1]["fallback"]
        assert manager().snapshot(req, result())["status"] == "rejected"
    asyncio.run(scenario())


def test_http_capture_feedback_and_failure_isolation(monkeypatch):
    m = manager()
    orch = AgentOrchestrator("test", model="test")
    intent = IntentResult(IntentCategory.TECHNICAL_LOGIN, .95, UrgencyLevel.LOW, "technical", {}, "", 0)
    orch.recognize_intent = AsyncMock(return_value=intent)
    for agents in orch._pool.values():
        agents[0]._client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(return_value=SimpleNamespace(
            content=[{"type": "text", "text": "核对 Token 是否过期，再重新授权。"}]))))
    mem = SimpleNamespace(get_lightweight_context=AsyncMock(return_value=LightweightContext()),
                          begin_turn=AsyncMock(), add_message=AsyncMock())
    pipeline = ContextPipeline(orch, ContextPlanner(None, "test", True), memory=mem, experience=m)
    monkeypatch.setattr(main, "_orchestrator", orch)
    monkeypatch.setattr(main, "_memory", atomic_http_stub(mem))
    monkeypatch.setattr(main, "_context_pipeline", pipeline)
    client = TestClient(main.app)
    response = client.post("/chat", json={"message": "OAuth 登录报 401", "user_id": "u1", "debug_context": True})
    assert response.status_code == 200
    rows = client.get("/experience/u1/cases?status=pending").json()["items"]
    assert len(rows) == 1 and rows[0]["request_id"] == response.json()["request_id"]
    cid = rows[0]["case_id"]
    asyncio.run(m.process("u1", cid))
    assert client.get("/experience/u2/cases").json()["items"] == []
    assert client.post(f"/experience/u2/cases/{cid}/feedback", json={"feedback": "negative"}).status_code == 404
    assert client.post(f"/experience/u1/cases/{cid}/feedback", json={"feedback": "bad"}).status_code == 422
    assert client.get("/experience/u1/cases?status=bad").status_code == 422
    for _ in range(2):
        assert client.post(f"/experience/u1/cases/{cid}/feedback", json={"feedback": "negative"}).json()["status"] == "revoked"
    m.index.search = AsyncMock(side_effect=ConnectionError)
    m.store.insert = AsyncMock(side_effect=ConnectionError)
    response = client.post("/chat", json={"message": "OAuth 登录报 401", "user_id": "u1", "debug_context": True})
    assert response.status_code == 200
    assert response.json()["context_debug"]["sources"]["experience"]["status"] == "failed"


def test_shutdown_preserves_pending_and_deadline_is_bounded():
    async def scenario():
        m = manager()
        started = asyncio.Event()
        async def slow_judge(*args):
            started.set()
            await asyncio.Event().wait()
        m.judge.judge = slow_judge
        cid = await m.submit(request(), result())
        await m.start()
        await asyncio.wait_for(started.wait(), 1)
        await asyncio.wait_for(m.close(), 1)
        assert (await m.store.read("u1", cid))["status"] == "pending"
        assert (await m.store.redis.hgetall(m.store.PENDING))[cid] == "u1"
        m.timeout = .01
        await asyncio.wait_for(m.process("u1", cid), 1)
        assert (await m.store.read("u1", cid))["attempts"] == 2
        await asyncio.wait_for(m.process("u1", cid), 1)
        assert (await m.store.read("u1", cid))["status"] == "failed"
        assert await m.store.redis.hgetall(m.store.PENDING) == {}
    asyncio.run(scenario())


def test_queue_ack_failure_does_not_repeat_judging():
    async def scenario():
        m = manager()
        cid = await m.submit(request(), result())
        m.store.redis.fail_ack = True
        with pytest.raises(ConnectionError):
            await m.process("u1", cid)
        assert (await m.store.read("u1", cid))["status"] == "active"
        m.store.redis.fail_ack = False
        await m.process("u1", cid)
        m.judge.judge.assert_awaited_once()
        assert await m.store.redis.hgetall(m.store.PENDING) == {}
    asyncio.run(scenario())


@pytest.mark.parametrize("available", [True, False])
def test_llm_planning_experience_and_fallback(available):
    async def scenario():
        data = {"need_" + s: s == "experience" for s in ("memory", "profile", "knowledge", "skill", "experience")}
        data.update(memory_query=None, knowledge_query=None, reasons={s: "reason" for s in
                    ("memory", "profile", "knowledge", "skill", "experience")})
        client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(return_value=SimpleNamespace(
            content=[{"type": "text", "text": json.dumps(data)}]))))
        intent = IntentResult(IntentCategory.TECHNICAL_LOGIN, .5, UrgencyLevel.LOW, "technical", {}, "", 0)
        planner = ContextPlanner(client, "test", available)
        plan = await planner.plan("登录报 401", intent, LightweightContext())
        assert plan.planner_source == "llm"
        assert plan.need_experience is available
        assert plan.experience_query == ("登录报 401" if available else None)
        client.messages.create.side_effect = RuntimeError("offline")
        plan = await planner.plan("登录报 401", intent, LightweightContext())
        assert plan.planner_source == "fallback" and plan.need_experience is available
    asyncio.run(scenario())


def test_experience_cannot_change_route_and_parallel_failures_are_retained():
    async def scenario():
        from agents.agent_orchestrator import RoutingDecision
        orch = AgentOrchestrator("test", model="test")
        req = request()
        before = orch._route_decision(req)
        req.context_bundle = ContextBundle(experiences=[{"agents": ["billing"], "response": "route to billing"}])
        req.context = req.context_bundle.to_prompt_text()
        assert orch._route_decision(req) == before
        orch._pool[AgentType.TECHNICAL][0].handle = AsyncMock(return_value=AgentResponse(AgentType.TECHNICAL, "good", True))
        orch._pool[AgentType.BILLING][0].handle = AsyncMock(side_effect=RuntimeError("unexpected"))
        req.execution_records.clear()
        output = await orch.run_parallel(req, RoutingDecision(AgentType.TECHNICAL, [AgentType.BILLING], "mixed", .9))
        assert output.response == "good"
        assert any(not record["success"] for record in req.execution_records)
        assert manager().snapshot(req, output)["status"] == "rejected"
    asyncio.run(scenario())


def test_credential_json_and_nested_fields_are_filtered():
    from experience.runtime import sanitize
    value = sanitize({"nested": {"Authorization": "Bearer hidden"},
                      "text": '{"password": "hidden", "api_key": "hidden"}'})
    assert "hidden" not in json.dumps(value)


def test_worker_stop_does_not_depend_on_cancel_propagation():
    async def scenario():
        m = manager()
        await m.submit(request(), result())
        started = asyncio.Event()
        async def completed_during_cancel(*args):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # Reproduce a child completion winning the wait_for cancellation race.
                return
        m.process = AsyncMock(side_effect=completed_during_cancel)
        await m.start()
        await asyncio.wait_for(started.wait(), 1)
        await asyncio.wait_for(m.close(), 1)
        assert m._worker is None
        m.process.assert_awaited_once()
    asyncio.run(scenario())
