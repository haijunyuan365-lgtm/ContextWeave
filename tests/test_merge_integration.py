"""Cross-branch regressions: durable evidence, receipts, privacy and authorization."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import fakeredis.aioredis
import httpx
import pytest
from fastapi.testclient import TestClient

from api import main
from agents.agent_orchestrator import AgentOrchestrator
from core.concurrency import KeyedLocks
from core.context_planner import ContextPlanner
from core.context_runtime import ContextPipeline
from core.intent_recognizer import IntentResult, IntentCategory, UrgencyLevel
from core.redaction import diagnostic_trace
from memory.conversation_memory import MemoryManager
from memory.evolution_runtime import MemoryEvolution, MemoryStore
from memory_fakes import FakeCollection, FakeModel


@pytest.mark.asyncio
async def test_receipt_replay_queues_user_evidence_and_captures_experience_once(monkeypatch):
    monkeypatch.delenv("ECHOMIND_API_TOKENS", raising=False)
    monkeypatch.setenv("ECHOMIND_ENV", "development")
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    evo = MemoryEvolution(redis, MemoryStore(FakeCollection()), FakeModel(), "test")
    evo.enabled = False  # Inspect durable pending evidence before a worker consumes it.
    evo.schedule = Mock()
    mem = MemoryManager.__new__(MemoryManager)
    mem._redis, mem._evolution = redis, evo
    mem._request_locks, mem._conversation_locks = KeyedLocks(), {}
    mem._get_profile = AsyncMock(return_value={})
    mem._query_episodic = AsyncMock(return_value={})
    orch = AgentOrchestrator("test-key", model="test")
    orch.recognize_intent = AsyncMock(return_value=IntentResult(
        IntentCategory.GREETING, .99, UrgencyLevel.LOW, "other", {}, "test", 0))
    create = AsyncMock(return_value=SimpleNamespace(content=[{"type":"text", "text":"reply"}]))
    for agents in orch._pool.values():
        agents[0]._client = SimpleNamespace(messages=SimpleNamespace(create=create))
    pipeline = ContextPipeline(orch, ContextPlanner(None, "test"), mem)
    pipeline.capture = AsyncMock()
    for name, value in [("_memory", mem), ("_orchestrator", orch), ("_context_pipeline", pipeline),
                        ("_profile_updater", None), ("_monitor", None)]:
        monkeypatch.setattr(main, name, value)
    payload = {"message":"hello", "user_id":"u", "conv_id":"c", "client_request_id":"one"}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as http:
            a, b = await asyncio.gather(*(http.post("/chat", json=payload) for _ in range(2)))
            assert a.status_code == b.status_code == 200
            assert a.json() == b.json()
            conflict = await http.post("/chat", json={**payload, "message":"different"})
            assert conflict.status_code == 409
        assert create.await_count == 1
        assert await redis.llen(mem._wm_key("u", "c")) == 2
        pending = await evo.pending("u")
        assert len(pending) == 1 and pending[0].content == "hello"
        rows = [json.loads(raw) for raw in await redis.lrange(mem._wm_key("u", "c"), 0, -1)]
        assert rows[1]["message_id"] == pending[0].message_id
        assert rows[0]["sequence"] == rows[1]["sequence"] == pending[0].sequence
        assert await redis.get(MemoryEvolution.key("u", "sequence")) == "1"
        evo.schedule.assert_called_once_with("u")
        pipeline.capture.assert_awaited_once()
    finally:
        await redis.aclose()


@pytest.mark.asyncio
async def test_failed_turn_does_not_queue_memory_evidence():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    evo = MemoryEvolution(redis, MemoryStore(FakeCollection()), FakeModel(), "test")
    evo.schedule = Mock()
    mem = MemoryManager.__new__(MemoryManager)
    mem._redis, mem._evolution = redis, evo
    try:
        await mem.commit_turn("u", "c", "one", "hello", {"request_id":"r", "response":"error", "status":"failed"})
        assert await redis.llen(mem._wm_key("u", "c")) == 0
        assert await evo.pending("u") == []
        assert await mem.get_turn_receipt("u", "c", "one")
        evo.schedule.assert_not_called()
    finally:
        await redis.aclose()


def test_diagnostic_trace_removes_ordinary_tool_arguments_and_error_prose():
    original = {"request_id":"r", "tool_calls":[{"input":{"query":"private normal text", "order":"123"},
        "error":"private exception", "result":{"content":"private document"}, "latency_ms":12, "success":False}]}
    clean = diagnostic_trace(original)
    assert "private" not in json.dumps(clean) and "123" not in json.dumps(clean)
    assert clean["tool_calls"][0]["latency_ms"] == 12
    assert original["tool_calls"][0]["input"]["query"] == "private normal text"


def test_api_users_can_only_read_and_update_their_own_memory_and_experience(monkeypatch):
    user, admin = "user-token-" + "x" * 24, "admin-token-" + "x" * 24
    monkeypatch.setenv("ECHOMIND_API_TOKENS", json.dumps({user:{"user_id":"u", "role":"user"}, admin:{"user_id":"admin", "role":"admin"}}))
    monkeypatch.setattr(main, "_memory", SimpleNamespace(inspect_memory=AsyncMock(return_value={"ok":True})))
    experience = SimpleNamespace(enabled=True, io_timeout=1, list_cases=AsyncMock(return_value=[]))
    monkeypatch.setattr(main, "_context_pipeline", SimpleNamespace(experience=experience))
    client = TestClient(main.app)
    for path in ["/memory/u", "/experience/u/cases"]:
        assert client.get(path, headers={"Authorization":"Bearer " + user}).status_code == 200
    for path in ["/memory/other", "/experience/other/cases"]:
        assert client.get(path, headers={"Authorization":"Bearer " + user}).status_code == 403
    assert client.get("/memory/other", headers={"Authorization":"Bearer " + admin}).status_code == 200


def test_history_accepts_api_admin_or_dedicated_trace_token(monkeypatch):
    from observability import api as history_api
    admin, user, trace = (prefix + "x" * 24 for prefix in ["admin-", "user-", "trace-"])
    monkeypatch.setenv("ECHOMIND_API_TOKENS", json.dumps({admin:{"user_id":"a", "role":"admin"}, user:{"user_id":"u", "role":"user"}}))
    monkeypatch.setenv("TRACE_ADMIN_TOKEN", trace)
    monkeypatch.setattr(history_api, "get_history", lambda: SimpleNamespace(search=lambda **kwargs:{"total":0, "items":[]}))
    client = TestClient(main.app)
    for token in [admin, trace]:
        assert client.get("/trace/history", headers={"Authorization":"Bearer " + token}).status_code == 200
    assert client.get("/trace/history", headers={"Authorization":"Bearer " + user}).status_code == 403
    assert client.get("/memory/u", headers={"Authorization":"Bearer " + trace}).status_code == 401

