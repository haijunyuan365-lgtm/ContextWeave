import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.context_planner import ContextPlanner, LightweightContext, SOURCES
from core.intent_recognizer import IntentCategory, IntentResult, UrgencyLevel, IntentRecognizer


def intent(category=IntentCategory.REFUND, confidence=.95):
    return IntentResult(category, confidence, UrgencyLevel.LOW, "billing", {}, "", 0)


def client_for(data):
    text = data if isinstance(data, str) else json.dumps(data)
    return SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(
        return_value=SimpleNamespace(content=[SimpleNamespace(type="text", text=text)]))))


def valid_plan():
    return {**{"need_" + s: s in {"knowledge", "skill"} for s in SOURCES},
            "memory_query": None, "knowledge_query": "invented order 123",
            "reasons": {s: "private value that must not leak" for s in SOURCES}}


@pytest.mark.parametrize("query,category,flags", [
    ("退款多久到账？", IntentCategory.REFUND, (False, False, True)),
    ("还是按照我上次那个退款方式", IntentCategory.REFUND, (True, True, True)),
    ("你好", IntentCategory.GREETING, (False, False, False)),
    ("谢谢", IntentCategory.FEEDBACK, (False, False, False)),
    ("转人工", IntentCategory.HUMAN_HANDOFF, (False, False, False)),
])
def test_rules(query, category, flags):
    planner = ContextPlanner(None, "test")
    plan = asyncio.run(planner.plan(query, intent(category), LightweightContext()))
    assert (plan.need_memory, plan.need_profile, plan.need_knowledge) == flags
    assert not plan.need_experience


def test_clear_query_does_not_call_planner_llm():
    client = client_for(valid_plan())
    plan = asyncio.run(ContextPlanner(client, "test").plan("退款多久到账", intent(), LightweightContext()))
    assert plan.planner_source == "rule"
    client.messages.create.assert_not_called()
    plan = asyncio.run(ContextPlanner(client, "test").plan("还是按照我上次那个退款方式", intent(), LightweightContext()))
    assert plan.planner_source == "rule" and plan.need_memory and plan.need_profile
    client.messages.create.assert_not_called()


def test_recent_reference_does_not_trigger_long_term_retrieval():
    client = client_for(valid_plan())
    light = LightweightContext([{"role": "user", "content": "退款怎么申请"}])
    plan = asyncio.run(ContextPlanner(client, "test").plan("这个需要多久", intent(), light))
    assert not plan.need_memory
    assert "退款怎么申请" in plan.knowledge_query
    client.messages.create.assert_not_called()


def test_llm_planning_is_once_and_does_not_accept_invented_query_or_private_reasons():
    client = client_for(valid_plan())
    plan = asyncio.run(ContextPlanner(client, "test").plan("登录失败又重复扣款", intent(), LightweightContext()))
    assert plan.planner_source == "llm"
    assert plan.knowledge_query == "登录失败又重复扣款"
    assert "private" not in str(plan.reasons)
    assert client.messages.create.await_count == 1


@pytest.mark.parametrize("data", ["not json", {**valid_plan(), "need_memory": "false"},
                                  {**valid_plan(), "extra": 1}, {**valid_plan(), "reasons": []}])
def test_invalid_planner_response_falls_back(data):
    client = client_for(data)
    plan = asyncio.run(ContextPlanner(client, "test").plan("上次退款", intent(confidence=.3), LightweightContext()))
    assert plan.planner_source == "fallback" and plan.need_memory and plan.need_knowledge
    assert not plan.need_profile
    assert client.messages.create.await_count == 1


def test_timeout_and_disabled_planner(monkeypatch):
    async def slow(**kwargs):
        await asyncio.sleep(1)
    client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(side_effect=slow)))
    monkeypatch.setenv("ECHOMIND_PLANNER_TIMEOUT_SECONDS", ".01")
    planner = ContextPlanner(client, "test")
    result = asyncio.run(planner.plan("那个", intent(confidence=.2), LightweightContext()))
    assert result.planner_source == "fallback"
    assert client.messages.create.await_count == 1
    monkeypatch.setenv("ECHOMIND_PLANNER_LLM_ENABLED", "false")
    asyncio.run(ContextPlanner(client, "test").plan("那个", intent(), LightweightContext()))
    assert client.messages.create.await_count == 1


def test_intent_summary_reaches_prompt_and_cache_key():
    recognizer = IntentRecognizer("test-key", model="test")
    recognizer.client = client_for({"intent": "refund", "confidence": .9})
    assert recognizer._cache_key("多久", summary="退款") != recognizer._cache_key("多久", summary="物流")
    asyncio.run(recognizer._llm_recognize("多久", None, "正在申请退款"))
    prompt = recognizer.client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "正在申请退款" in prompt


def test_entities_and_intent_group_inform_conservative_fallback():
    item = intent(IntentCategory.OTHER, .2)
    item.intent_group = "other"
    item.entities = {"error_code": ["401"]}
    planner = ContextPlanner(None, "test")
    result = asyncio.run(planner.plan("401怎么办", item, LightweightContext()))
    assert result.need_knowledge and not result.need_memory
    item.entities, item.intent_group = {}, "billing"
    result = asyncio.run(planner.plan("处理一下", item, LightweightContext()))
    assert result.need_knowledge and not result.need_profile
