import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient

from api import main
from agents.agent_orchestrator import Request
from agents.tools import build_context_tools
from core.context_planner import ContextPlanner, LightweightContext
from core.context_runtime import ContextPipeline
from core.intent_recognizer import IntentCategory, IntentResult, UrgencyLevel
from memory.conversation_memory import MemoryManager, MsgRole
from memory_fakes import FakeCollection, engine, enqueue, idle, source


def manager():
    evo, redis, collection, model = engine()
    mem = object.__new__(MemoryManager)
    mem._redis, mem._evolution, mem._memory_store = redis, evo, evo.store
    mem._profile, mem._episodic = FakeCollection(), FakeCollection()
    mem._conversation_locks = {}
    mem._client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(
        return_value=SimpleNamespace(content=[{"type": "text", "text": "已有对话摘要"}]))), close=AsyncMock())
    mem._model = "test"
    return mem, evo, redis, collection, model


def test_source_order_reserved_before_response_completion():
    async def scenario():
        mem, evo, _, _, model = manager()
        older = await mem.begin_turn("default", "u", "c1", "以后退款优先支付宝")
        newer = await mem.begin_turn("default", "u", "c2", "以后退款优先支付宝")
        assert older.sequence < newer.sequence and older.message_id != newer.message_id
        await mem.add_message("default", "u", "c2", MsgRole.USER, newer.content, source=newer)
        await idle(evo)
        await mem.add_message("default", "u", "c1", MsgRole.USER, older.content, source=older)
        await idle(evo)
        snapshot = await evo.store.read("default", "u")
        assert snapshot["records"]["refund_method"]["source"]["message_id"] == newer.message_id
        assert snapshot["events"][-1]["reason"] == "stale_source"
        with pytest.raises(ValueError):
            await mem.add_message("other-project", "u", "c1", MsgRole.USER, older.content, source=older)
    asyncio.run(scenario())


def test_compression_preserves_ids_order_and_pending_evidence():
    async def scenario():
        mem, evo, redis, _, _ = manager()
        evo.enabled = False
        mem.COMPRESS_AT = 8
        added = []
        for i in range(8):
            added.append(await mem.add_message("default", "u", "c", MsgRole.USER, f"message-{i}"))
        recent = await mem._get_working_memory("default", "u", "c")
        assert [m.content for m in recent] == [f"message-{i}" for i in range(3, 8)]
        assert [m.message_id for m in recent] == [m.message_id for m in added[3:]]
        pending = await evo.pending("default", "u")
        assert [p.message_id for p in pending] == [m.message_id for m in added]
        stored = mem._episodic.writes[0][2]["metadatas"][0]
        assert json.loads(stored["source_message_ids"]) == [m.message_id for m in added[:3]]
        assert all(p.content.startswith("message-") for p in pending)
    asyncio.run(scenario())


def test_append_during_compression_is_not_deleted():
    async def scenario():
        mem, evo, redis, _, _ = manager()
        evo.enabled = False
        mem.COMPRESS_AT = 8
        for i in range(7):
            await mem.add_message("default", "u", "c", MsgRole.USER, str(i))
        entered, release = asyncio.Event(), asyncio.Event()
        async def summarize(**kwargs):
            entered.set()
            await release.wait()
            return SimpleNamespace(content=[{"type": "text", "text": "summary"}])
        mem._client.messages.create = summarize
        compressing = asyncio.create_task(mem.add_message("default", "u", "c", MsgRole.USER, "7"))
        await entered.wait()
        appended = asyncio.create_task(mem.add_message("default", "u", "c", MsgRole.USER, "8"))
        await asyncio.sleep(0)
        assert not appended.done()
        release.set()
        await asyncio.gather(compressing, appended)
        assert [m.content for m in await mem._get_working_memory("default", "u", "c")] == ["3", "4", "5", "6", "7", "8"]
        assert len(await evo.pending("default", "u")) == 9
    asyncio.run(scenario())


def test_archive_failures_do_not_discard_messages_older_than_working_max():
    async def scenario():
        mem, evo, redis, _, _ = manager()
        evo.enabled = False
        mem._episodic.fail_write = True
        added = []
        for i in range(25):
            added.append(await mem.add_message("default", "u", "c", MsgRole.USER, f"message-{i}"))
        assert await redis.llen("wm:default:u:c") == 25
        mem._episodic.fail_write = False
        await mem._compress("default", "u", "c")
        stored = mem._episodic.writes[0][2]["metadatas"][0]
        assert json.loads(stored["source_message_ids"]) == [m.message_id for m in added[:20]]
        assert [m.content for m in await mem._get_working_memory("default", "u", "c")] == [f"message-{i}" for i in range(20, 25)]
    asyncio.run(scenario())


def test_legacy_working_messages_remain_readable_with_stable_synthetic_ids():
    async def scenario():
        mem, _, redis, _, _ = manager()
        await redis.lpush("wm:default:u:c", json.dumps({"role": "user", "content": "旧消息", "ts": "2026-01-01T00:00:00"}))
        a = (await mem._get_working_memory("default", "u", "c"))[0]
        b = (await mem._get_working_memory("default", "u", "c"))[0]
        assert a.message_id == b.message_id and a.message_id.startswith("legacy:")
    asyncio.run(scenario())


def test_background_completion_visible_to_new_planner_request_and_reactive_tool():
    async def scenario():
        mem, evo, _, _, _ = manager()
        await enqueue(evo, source(1))
        await idle(evo)
        intent = IntentResult(IntentCategory.REFUND, .95, UrgencyLevel.LOW, "billing", {}, "", 0)
        orch = SimpleNamespace(recognize_intent=AsyncMock(return_value=intent))
        pipeline = ContextPipeline(orch, ContextPlanner(None, "test"), mem)
        req = Request("按照我习惯的退款方式", "u", "c")
        await pipeline.prepare(req)
        assert req.context_bundle.profile["preferences"]["refund_method"] == "alipay"
        before = req.context
        # Request-local results stay a snapshot even as later records evolve.
        await enqueue(evo, source(2, "请忘记我的退款方式偏好"))
        evo.extractor.client.factory = lambda src: {
            "type": "preference", "key": "refund_method", "value": None, "operation": "retract",
            "confidence": .95, "source_message_id": src.message_id, "quote": src.content}
        await idle(evo)
        result = await build_context_tools()["get_user_profile"].handler(req, {})
        assert result["cached"] and result["profile"]["preferences"]["refund_method"] == "alipay"
        assert req.context == before
        new_req = Request("退款政策", "u", "c")
        await pipeline.prepare(new_req)
        result = await build_context_tools()["get_user_profile"].handler(new_req, {})
        assert result["success"] and result["profile"] == {}
        await req.retrieval_session.close()
        await new_req.retrieval_session.close()
    asyncio.run(scenario())


def test_read_only_endpoints_preserve_legacy_and_bound_event_results(monkeypatch):
    mem, evo, _, _, _ = manager()
    async def setup():
        await enqueue(evo, source(1))
        await idle(evo)
    asyncio.run(setup())
    mem._profile.documents[mem._profile_doc_id("default", "u")] = json.dumps({"preferences": ["旧画像原文"]})
    monkeypatch.setattr(main, "_memory", mem)
    client = TestClient(main.app)
    body = client.get("/memory/u").json()
    assert body["version"] == 1 and body["records"][0]["source"]["message_id"] == "u-1"
    assert body["legacy_profile"] == {"preferences": ["旧画像原文"]}
    assert "旧画像原文" not in json.dumps(body["profile"], ensure_ascii=False)
    events = client.get("/memory/u/events?limit=1").json()["events"]
    assert len(events) == 1 and events[0]["action"] == "CREATE"
    assert client.get("/memory/u/events?limit=201").status_code == 422
    assert client.get("/memory/u/events?limit=0").status_code == 422
    status = client.get("/memory/u/status").json()
    assert status["pending_count"] == 0 and status["last_success_at"]
    assert client.get("/memory/other").json()["records"] == []
    assert client.post("/memory/u", json={}).status_code == 405


def test_update_profile_compatibility_only_schedules_pending_messages():
    async def scenario():
        mem, evo, _, _, model = manager()
        await enqueue(evo, source(1), schedule=False)
        await mem.update_profile("default", "u", "c")
        await idle(evo)
        assert len(model.calls) == 1
        assert not mem._profile.writes
        assert (await mem.get_profile("default", "u"))["preferences"]["refund_method"] == "alipay"
    asyncio.run(scenario())


def test_cli_reserves_source_before_prepare_and_uses_same_source_for_both_messages(monkeypatch):
    from agents import agent_orchestrator
    from memory import conversation_memory
    from core import context_runtime, skill_loader
    calls = []
    src = source(1)
    async def begin(*args):
        calls.append("begin")
        return src
    async def prepare(req):
        calls.append("prepare")
        req.retrieval_session = SimpleNamespace(close=AsyncMock())
    mem = SimpleNamespace(begin_turn=begin, start=AsyncMock(), close=AsyncMock(), add_message=AsyncMock(),
                          create_experience=AsyncMock(return_value=None))
    orch = SimpleNamespace(run=AsyncMock(return_value=SimpleNamespace(
        response="回复", agent_type=agent_orchestrator.AgentType.GENERAL)))
    pipeline = SimpleNamespace(prepare=prepare, close=AsyncMock(), capture=AsyncMock())
    answers = iter([src.content, "quit"])
    monkeypatch.setattr("builtins.input", lambda prompt: next(answers))
    monkeypatch.setattr(main, "_anthropic_cfg", lambda: {"api_key": "test", "model": "test"})
    monkeypatch.setattr(main, "_make_context_pipeline", lambda *args: pipeline)
    monkeypatch.setattr(conversation_memory, "MemoryManager", lambda **kwargs: mem)
    monkeypatch.setattr(agent_orchestrator, "AgentOrchestrator", lambda **kwargs: orch)
    monkeypatch.setattr(skill_loader, "SkillManager", lambda **kwargs: SimpleNamespace(load=Mock()))
    monkeypatch.setattr(context_runtime, "log_context", Mock())
    asyncio.run(main._cli())
    assert calls == ["begin", "prepare"]
    assert mem.add_message.await_count == 2
    assert all(call.kwargs["source"] is src for call in mem.add_message.call_args_list)
    mem.start.assert_awaited_once()
    pipeline.capture.assert_awaited_once()
    mem.close.assert_awaited_once()
