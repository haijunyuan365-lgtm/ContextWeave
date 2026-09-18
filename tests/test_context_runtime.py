import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agents.agent_orchestrator import Request, TechnicalAgent, AgentOrchestrator, BillingAgent
from agents.tools import build_context_tools, build_shared_rag_tools
from core.context_planner import ContextPlanner, LightweightContext
from core.context_runtime import ContextBundle, RetrievalSession, ContextPipeline, context_debug
from core.intent_recognizer import IntentCategory, IntentResult, UrgencyLevel
from core.skill_loader import Skill, SkillManager


def rag_result(data=None, success=True, error=None):
    return SimpleNamespace(success=success, data=data if data is not None else [{"title": "政策", "content": "退款规则"}],
                           reranked=True, error=error)


def make_intent(category=IntentCategory.REFUND):
    return IntentResult(category, .95, UrgencyLevel.LOW, "billing", {}, "", 0)


def test_preload_order_selectivity_reactive_tools_and_cache():
    async def scenario():
        events = []
        async def light(*args):
            events.append("light")
            return LightweightContext(summary="已有摘要")
        async def recognize(*args, **kwargs):
            assert kwargs["summary"] == "已有摘要"
            events.append("intent")
            return make_intent()
        async def knowledge(*args, **kwargs):
            events.append("knowledge")
            return rag_result()
        mem = SimpleNamespace(get_lightweight_context=light, search_episodic=AsyncMock(return_value=["旧偏好"]),
                              get_profile=AsyncMock(return_value={"preference": "alipay"}))
        rag = SimpleNamespace(search_with_rewrite=AsyncMock(side_effect=knowledge))
        pipeline = ContextPipeline(SimpleNamespace(recognize_intent=recognize), ContextPlanner(None, "test"), mem, rag)
        req = Request("退款多久到账", "u1", "c1")
        await pipeline.prepare(req)
        assert events == ["light", "intent", "knowledge"]
        mem.search_episodic.assert_not_called()
        mem.get_profile.assert_not_called()
        assert req.context_bundle.knowledge and not req.retrieval_session.knowledge_used
        tools = {**build_shared_rag_tools(rag), **build_context_tools()}
        answer = await tools["search_knowledge_base"].handler(req, {"query": req.message})
        assert answer["cached"] and rag.search_with_rewrite.await_count == 1
        assert req.retrieval_session.knowledge_used
        await tools["search_conversation_memory"].handler(req, {"query": "上次退款"})
        await tools["get_user_profile"].handler(req, {})
        mem.search_episodic.assert_awaited_once_with("u1", "c1", "上次退款", top_k=5)
        mem.get_profile.assert_awaited_once_with("u1")
        debug = context_debug(req)
        assert "旧偏好" not in str(debug) and "alipay" not in str(debug) and "退款规则" not in str(debug)
        await req.retrieval_session.close()
        assert not req.retrieval_session._cache
    asyncio.run(scenario())


def test_same_request_single_flight_and_distinct_arguments():
    async def scenario():
        async def slow(*args, **kwargs):
            await asyncio.sleep(.01)
            return rag_result()
        rag = SimpleNamespace(search_with_rewrite=AsyncMock(side_effect=slow))
        session = RetrievalSession("u", "c", ContextBundle(), knowledge=rag)
        results = await asyncio.gather(*(session.retrieve("knowledge", "refund   policy") for _ in range(3)))
        assert rag.search_with_rewrite.await_count == 1
        assert sum(r.cached for r in results) == 2
        await session.retrieve("knowledge", "refund policy", 3)
        await session.retrieve("knowledge", "another query")
        assert rag.search_with_rewrite.await_count == 2
        results[0].data[0]["content"] = "mutated"
        assert (await session.retrieve("knowledge", "refund policy")).data[0]["content"] == "退款规则"
        await session.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("first", [rag_result(success=False), rag_result(data=[{"fallback": True, "content": "unavailable"}]),
                                   RuntimeError("private error")])
def test_failed_results_are_retryable(first):
    async def scenario():
        rag = SimpleNamespace(search_with_rewrite=AsyncMock(side_effect=[first, rag_result()]))
        session = RetrievalSession("u", "c", ContextBundle(), knowledge=rag)
        result = await session.retrieve("knowledge", "refund")
        assert result.status == "failed" and not session.knowledge_used
        assert "private" not in result.error
        result = await session.retrieve("knowledge", "refund")
        assert result.status == "success" and not result.cached
        assert rag.search_with_rewrite.await_count == 2
    asyncio.run(scenario())


def test_empty_results_cached_and_requests_are_isolated():
    async def scenario():
        mem = SimpleNamespace(get_profile=AsyncMock(return_value={}))
        rag = SimpleNamespace(search_with_rewrite=AsyncMock(return_value=rag_result(data=[])))
        for user in ("u1", "u2", "u1"):
            session = RetrievalSession(user, "c", ContextBundle(), memory=mem, knowledge=rag)
            for source in ("profile", "knowledge"):
                assert (await session.retrieve(source)).status == "empty"
                assert (await session.retrieve(source)).cached
        assert mem.get_profile.await_count == 3 and rag.search_with_rewrite.await_count == 3
        assert [c.args[0] for c in mem.get_profile.call_args_list] == ["u1", "u2", "u1"]
    asyncio.run(scenario())


def test_selective_failure_and_role_skills():
    async def scenario():
        manager = SkillManager(".")
        manager._skills = [Skill("billing", "", "账单专用", "", ["退款"], ["billing"]),
                           Skill("general", "", "通用专用", "", ["退款"], ["general"])]
        mem = SimpleNamespace(get_lightweight_context=AsyncMock(return_value=LightweightContext()),
                              search_episodic=AsyncMock(side_effect=RuntimeError("failed")),
                              get_profile=AsyncMock(return_value={"preference": "alipay"}))
        orch = SimpleNamespace(recognize_intent=AsyncMock(return_value=make_intent()))
        req = Request("还是按照我上次那个退款方式", "u", "c")
        await ContextPipeline(orch, ContextPlanner(None, "test"), mem, None, manager).prepare(req)
        assert req.context_bundle.sources["memory"]["status"] == "failed"
        assert req.context_bundle.sources["profile"]["status"] == "success"
        assert req.context_bundle.sources["knowledge"]["status"] == "unconfigured"
        billing = BillingAgent(None, "test", manager)._build_system_prompt(req)
        technical = TechnicalAgent(None, "test", manager)._build_system_prompt(req)
        assert "账单专用" in billing and "通用专用" not in billing
        assert "账单专用" not in technical
        assert req.context_bundle.skill_prompt_for("general").endswith("通用专用")
    asyncio.run(scenario())


def test_tools_cannot_select_another_user_and_are_available_in_orchestrator():
    tools = build_context_tools()
    agent = TechnicalAgent(None, "test")
    with pytest.raises(ValueError):
        agent._validate_tool_input(tools["get_user_profile"], {"user_id": "someone_else"})
    orch = AgentOrchestrator("test-key", model="test")
    for agents in orch._pool.values():
        assert set(tools) <= set(agents[0].get_tools())


def test_lightweight_failure_does_not_block_independent_knowledge():
    async def scenario():
        mem = SimpleNamespace(get_lightweight_context=AsyncMock(side_effect=RuntimeError("redis down")))
        rag = SimpleNamespace(search_with_rewrite=AsyncMock(return_value=rag_result()))
        orch = SimpleNamespace(recognize_intent=AsyncMock(return_value=make_intent()))
        req = Request("退款政策", "u", "c")
        await ContextPipeline(orch, ContextPlanner(None, "test"), mem, rag).prepare(req)
        assert req.context_bundle.sources["recent_history"]["status"] == "failed"
        assert req.context_bundle.sources["knowledge"]["status"] == "success"
    asyncio.run(scenario())


def test_partial_knowledge_evidence_is_usable_but_retryable():
    async def scenario():
        rag = SimpleNamespace(search_with_rewrite=AsyncMock(return_value=rag_result(error="partial failure")))
        session = RetrievalSession("u", "c", ContextBundle(), knowledge=rag)
        for _ in range(2):
            result = await session.retrieve("knowledge", "refund")
            assert result.status == "degraded" and result.data and not result.cached
        assert rag.search_with_rewrite.await_count == 2
    asyncio.run(scenario())


def test_concurrent_calls_on_same_agent_keep_local_tool_records():
    async def scenario():
        async def create(**kwargs):
            messages = kwargs["messages"]
            if isinstance(messages[-1]["content"], list):
                await asyncio.sleep(.01)
                return SimpleNamespace(content=[{"type": "text", "text": "done"}])
            query = messages[-1]["content"]
            name = "lookup_error_code" if "401" in query else "build_diagnostic_plan"
            args = {"error_code": "401"} if "401" in query else {"environment": "App", "reproduced": True}
            return SimpleNamespace(content=[{"type": "tool_use", "id": query, "name": name, "input": args}])
        agent = TechnicalAgent(SimpleNamespace(messages=SimpleNamespace(create=create)), "test")
        a, b = await asyncio.gather(agent.handle(Request("401", "u1", "c")),
                                    agent.handle(Request("crash", "u2", "c")))
        assert a.tools_used == ["lookup_error_code"] and b.tools_used == ["build_diagnostic_plan"]
        assert a.tool_traces[0]["tool_use_id"] == "401"
        assert b.tool_traces[0]["tool_use_id"] == "crash"
    asyncio.run(scenario())
