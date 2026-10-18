import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient

from api import main
from agents.agent_orchestrator import AgentOrchestrator, AgentType
from core.context_planner import ContextPlanner, ContextPlan, LightweightContext
from core.context_runtime import ContextPipeline
from core.intent_recognizer import IntentCategory, IntentResult, UrgencyLevel
from memory.conversation_memory import MemoryManager
from tooling.tool_manager import MCPToolManager, ToolResult


def test_lightweight_memory_reads_only_five_messages_and_summary():
    mem = object.__new__(MemoryManager)
    records = [json.dumps({"role": "user", "content": str(i), "ts": "2026-09-16T12:00:00"}) for i in range(5)]
    mem._redis = SimpleNamespace(lrange=AsyncMock(return_value=records), get=AsyncMock(return_value="摘" * 1000))
    mem._episodic = Mock()
    mem._profile = Mock()
    light = asyncio.run(mem.get_lightweight_context("u", "c"))
    assert [m["content"] for m in light.recent_history] == ["4", "3", "2", "1", "0"]
    assert len(light.summary) == 800
    mem._redis.lrange.assert_awaited_once_with("wm:u:c", 0, 4)
    assert not mem._episodic.mock_calls and not mem._profile.mock_calls


def test_episodic_retrieval_uses_user_scope_and_deduplicates_fallback():
    mem = object.__new__(MemoryManager)
    mem._query_episodic = AsyncMock(side_effect=[{"documents": [["same"]]}, {"documents": [["same", "other"]]}])
    result = asyncio.run(mem.search_episodic("u", "c", "退款", top_k=3))
    assert result == ["same", "other"]
    first, second = mem._query_episodic.call_args_list
    assert first.kwargs["where"] == {"$and": [{"user_id": "u"}, {"conv_id": "c"}]}
    assert second.kwargs["where"] == {"user_id": "u"}


def test_public_memory_errors_propagate_but_legacy_helpers_keep_fallback():
    async def scenario():
        mem = object.__new__(MemoryManager)
        mem._query_episodic = AsyncMock(side_effect=RuntimeError("down"))
        mem._profile = SimpleNamespace(get=Mock(side_effect=RuntimeError("down")))
        with pytest.raises(RuntimeError):
            await mem.search_episodic("u", "c", "退款")
        with pytest.raises(RuntimeError):
            await mem.get_profile("u")
        assert await mem._search_episodic("u", "c", "退款") == []
        assert await mem._get_profile("u") == {}
    asyncio.run(scenario())


@pytest.mark.parametrize("data,success,error,expected", [
    ([], True, None, "empty"),
    ([{"content": "fallback", "fallback": True}], True, "down", "failed"),
    ([], False, "down", "failed"),
])
def test_rag_pipeline_distinguishes_empty_from_failure(data, success, error, expected):
    async def scenario():
        manager = object.__new__(MCPToolManager)
        manager.rewrite_query = AsyncMock(return_value=["refund"])
        manager.call = AsyncMock(return_value=ToolResult(success, data, "knowledge_search", error=error))
        manager._rerank = AsyncMock(return_value=data)
        result = await manager.search_with_rewrite("knowledge_search", "refund")
        assert result.data == []
        assert result.success == (expected == "empty")
        manager._rerank.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize("mode,expected_used", [("valid", True), ("empty", False), ("fallback", False), ("error", False)])
def test_chat_default_compatibility_and_opt_in_debug(monkeypatch, mode, expected_used):
    intent = IntentResult(IntentCategory.REFUND, .95, UrgencyLevel.LOW, "billing", {}, "", 0)
    orch = AgentOrchestrator("test-key", model="test")
    orch.recognize_intent = AsyncMock(return_value=intent)
    for agents in orch._pool.values():
        agents[0]._client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(
            return_value=SimpleNamespace(content=[{"type": "text", "text": "请参考退款政策。"}]))))
    mem = SimpleNamespace(get_lightweight_context=AsyncMock(return_value=LightweightContext()),
                          search_episodic=AsyncMock(), get_profile=AsyncMock(),
                          add_message=AsyncMock(), update_profile=AsyncMock(), begin_turn=AsyncMock())
    docs = {"valid": [{"title": "policy", "content": "sensitive knowledge"}], "empty": [],
            "fallback": [{"content": "unavailable", "fallback": True}], "error": []}[mode]
    rag = SimpleNamespace(search_with_rewrite=AsyncMock(return_value=ToolResult(mode != "error", docs, "knowledge_search")))
    pipeline = ContextPipeline(orch, ContextPlanner(None, "test"), mem, rag)
    monkeypatch.setattr(main, "_orchestrator", orch)
    monkeypatch.setattr(main, "_memory", mem)
    monkeypatch.setattr(main, "_context_pipeline", pipeline)
    client = TestClient(main.app)
    for debug in (False, True):
        response = client.post("/chat", json={"message": "退款多久到账", "user_id": "u", "debug_context": debug})
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["knowledge_used"] is expected_used
        assert body["tools_used"] == []
        assert ("context_debug" in body) is debug
        if debug:
            assert body["context_debug"]["planner_source"] == "rule"
            assert "sensitive knowledge" not in json.dumps(body["context_debug"])
        assert {"conv_id", "request_id", "response", "intent", "intent_group", "agent_type", "agent_types",
                "primary_agent", "supporting_agents", "tools_used", "routing_reason", "routing_confidence",
                "escalated", "latency_ms", "knowledge_used", "entities", "intent_confidence", "intent_source_scores"} <= body.keys()
    assert mem.add_message.await_count == 4
    mem.search_episodic.assert_not_called()
    mem.get_profile.assert_not_called()


def test_chat_reports_reactive_knowledge_even_without_preloading(monkeypatch):
    orch = AgentOrchestrator("test-key", model="test")
    orch.recognize_intent = AsyncMock(return_value=IntentResult(
        IntentCategory.REFUND, .95, UrgencyLevel.LOW, "billing", {}, "", 0))
    async def create(**kwargs):
        if isinstance(kwargs["messages"][-1]["content"], list):
            return SimpleNamespace(content=[{"type": "text", "text": "根据知识库政策回复。"}])
        return SimpleNamespace(content=[{"type": "tool_use", "id": "rag", "name": "search_knowledge_base",
                                         "input": {"query": "退款政策"}}])
    for agents in orch._pool.values():
        agents[0]._client = SimpleNamespace(messages=SimpleNamespace(create=create))
    mem = SimpleNamespace(get_lightweight_context=AsyncMock(return_value=LightweightContext()),
                          add_message=AsyncMock(), update_profile=AsyncMock(), begin_turn=AsyncMock())
    rag = SimpleNamespace(search_with_rewrite=AsyncMock(return_value=ToolResult(
        True, [{"title": "退款", "content": "参考政策"}], "knowledge_search")))
    planner = SimpleNamespace(plan=AsyncMock(return_value=ContextPlan(need_skill=False)))
    monkeypatch.setattr(main, "_orchestrator", orch)
    monkeypatch.setattr(main, "_memory", mem)
    monkeypatch.setattr(main, "_context_pipeline", ContextPipeline(orch, planner, mem, rag))
    body = TestClient(main.app).post("/chat", json={"message": "退款政策", "debug_context": True}).json()
    assert body["knowledge_used"] is True
    assert body["tools_used"] == ["search_knowledge_base"]
    assert body["context_debug"]["plan"]["need_knowledge"] is False
    assert body["context_debug"]["sources"]["knowledge"]["calls"] == 1
    assert rag.search_with_rewrite.await_count == 1
