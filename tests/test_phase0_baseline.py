import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agents.agent_orchestrator import AgentOrchestrator
from api import main
from core.context_planner import ContextPlanner, LightweightContext
from core.context_runtime import ContextPipeline
from core.intent_recognizer import IntentCategory, IntentResult, UrgencyLevel
from mcp.tool_manager import ToolResult


CASES_PATH = Path(__file__).parent / "fixtures" / "phase0_demo_cases.json"
DEMO_CASES = json.loads(CASES_PATH.read_text(encoding="utf-8"))

CHAT_RESPONSE_FIELDS = {
    "conv_id", "request_id", "response", "intent", "intent_group", "agent_type",
    "agent_types", "primary_agent", "supporting_agents", "tools_used",
    "routing_reason", "routing_confidence", "escalated", "latency_ms",
    "knowledge_used", "entities", "intent_confidence", "intent_source_scores",
    "context_debug",
}


class OfflineLLM:
    """Deterministic Agent/Composer response; no provider network is used."""

    def __init__(self):
        self.messages = SimpleNamespace(create=self.create)

    async def create(self, **kwargs):
        return SimpleNamespace(content=[{"type": "text", "text": "Phase 0 离线基线回复"}])


def _intent(case):
    recognized = case["recognized"]
    return IntentResult(
        intent=IntentCategory(recognized["intent"]),
        confidence=recognized["confidence"],
        urgency=UrgencyLevel[recognized["urgency"]],
        intent_group=recognized["intent_group"],
        entities=recognized["entities"],
        reasoning="offline baseline fixture",
        latency_ms=0,
        source_scores=recognized["source_scores"],
    )


@pytest.mark.parametrize("case", DEMO_CASES, ids=[case["id"] for case in DEMO_CASES])
def test_phase0_demo_case_through_chat(monkeypatch, case):
    monkeypatch.setenv("ECHOMIND_PLANNER_LLM_ENABLED", "false")
    orchestrator = AgentOrchestrator("offline-test-key", model="offline-test-model")
    orchestrator.recognize_intent = AsyncMock(return_value=_intent(case))
    offline_llm = OfflineLLM()
    orchestrator._composer._client = offline_llm
    for agents in orchestrator._pool.values():
        agents[0]._client = offline_llm

    memory = SimpleNamespace(
        get_lightweight_context=AsyncMock(return_value=LightweightContext()),
        begin_turn=AsyncMock(return_value=1),
        add_message=AsyncMock(),
    )
    knowledge = SimpleNamespace(search_with_rewrite=AsyncMock(return_value=ToolResult(
        True,
        [{"title": "离线基线知识", "content": "用于验证知识预加载状态"}],
        "knowledge_search",
    )))
    pipeline = ContextPipeline(orchestrator, ContextPlanner(None, "offline-test-model"), memory, knowledge)
    monkeypatch.setattr(main, "_orchestrator", orchestrator)
    monkeypatch.setattr(main, "_memory", memory)
    monkeypatch.setattr(main, "_context_pipeline", pipeline)

    response = asyncio.run(main.chat(main.ChatRequest(
        message=case["message"],
        user_id="phase0-demo-user",
        conv_id="phase0-demo-conversation",
        debug_context=True,
    )))
    body = response.model_dump(exclude_unset=True)
    expected = case["expected"]

    assert set(body) == CHAT_RESPONSE_FIELDS
    assert body["intent"] == case["recognized"]["intent"]
    assert body["intent_group"] == case["recognized"]["intent_group"]
    assert body["intent_source_scores"] == case["recognized"]["source_scores"]
    assert body["primary_agent"] == expected["primary_agent"]
    assert body["supporting_agents"] == expected["supporting_agents"]
    assert body["escalated"] is expected["escalated"]
    assert body["context_debug"]["plan"] == expected["plan"]
    assert body["context_debug"]["routing"]["primary_agent"] == expected["primary_agent"]
    assert body["context_debug"]["routing"]["supporting_agents"] == expected["supporting_agents"]
    assert body["knowledge_used"] is expected["plan"]["need_knowledge"]
    assert memory.add_message.await_count == 2
