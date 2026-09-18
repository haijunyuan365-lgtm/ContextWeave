import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from agents.agent_orchestrator import AgentOrchestrator
from api import main
from core.context_planner import ContextPlanner, LightweightContext
from core.context_runtime import ContextPipeline
from core.intent_recognizer import IntentCategory, IntentResult, UrgencyLevel
from evaluation.evaluator import QualityScores
from experience.runtime import ExperienceIndex, ExperienceManager, ExperienceStore
from mcp.knowledge_base import KnowledgeBase
from mcp.rag_config import RAGConfig
from mcp.tool_manager import MCPToolManager, Tool, ToolResult
from memory.conversation_memory import MemoryManager, MsgRole
from memory_fakes import FakeCollection as MemoryCollection, FakeRedis
from projects.models import Project, ProjectMember
from projects.repository import ProjectRepository
from projects.service import ProjectService


def project_service():
    return ProjectService(ProjectRepository(
        [Project("project-a", "Project A"), Project("project-b", "Project B")],
        [ProjectMember("project-a", "alice"), ProjectMember("project-b", "alice"),
         ProjectMember("project-b", "bob")],
    ))


class KnowledgeCollection:
    def __init__(self):
        self.ids, self.documents, self.metadatas = [], [], []

    def count(self):
        return len(self.ids)

    def add(self, ids, documents, metadatas):
        self.ids.extend(ids)
        self.documents.extend(documents)
        self.metadatas.extend(metadatas)

    def get(self, ids=None, where=None, include=None):
        indexes = range(len(self.ids)) if ids is None else [self.ids.index(item) for item in ids]
        indexes = [i for i in indexes if not where or all(self.metadatas[i].get(k) == v for k, v in where.items())]
        result = {"ids": [self.ids[i] for i in indexes]}
        if include and "documents" in include:
            result["documents"] = [self.documents[i] for i in indexes]
        if include and "metadatas" in include:
            result["metadatas"] = [self.metadatas[i] for i in indexes]
        if include and "embeddings" in include:
            result["embeddings"] = [[float(i), 1.0] for i in indexes]
        return result

    def query(self, query_texts, n_results, where, include):
        indexes = [i for i, meta in enumerate(self.metadatas)
                   if all(meta.get(k) == v for k, v in where.items())][:n_results]
        return {
            "ids": [[self.ids[i] for i in indexes]],
            "documents": [[self.documents[i] for i in indexes]],
            "metadatas": [[self.metadatas[i] for i in indexes]],
            "distances": [[0.1 for _ in indexes]],
        }


def knowledge_base():
    kb = object.__new__(KnowledgeBase)
    kb.rag_config = RAGConfig()
    kb._bm25_lock = threading.RLock()
    kb._bm25 = None
    kb._bm25_items = ()
    kb._collection = KnowledgeCollection()
    return kb


class ExperienceRedis(FakeRedis):
    async def hget(self, key, field):
        return self.hashes[key].get(field)

    async def hscan_iter(self, key):
        for item in list(self.hashes[key].items()):
            yield item


class ExperienceCollection:
    def __init__(self):
        self.docs = {}

    def upsert(self, ids, documents, metadatas):
        for case_id, text, metadata in zip(ids, documents, metadatas):
            self.docs[case_id] = (text, metadata)

    def query(self, query_texts, n_results, where, include):
        filters = {key: value for item in where["$and"] for key, value in item.items()}
        ids = [case_id for case_id, (_, metadata) in self.docs.items()
               if all(metadata.get(key) == value for key, value in filters.items())][:n_results]
        return {"ids": [ids], "distances": [[0.1 for _ in ids]]}


def test_memory_working_state_is_partitioned_by_project():
    async def scenario():
        redis = FakeRedis()
        mem = object.__new__(MemoryManager)
        mem._redis = redis
        mem._conversation_locks = {}
        mem._evolution = SimpleNamespace(enabled=False, enqueue_commands=lambda pipe, source: None,
                                         schedule=lambda *args: None)
        mem.COMPRESS_AT = 20
        await mem.add_message("project-a", "alice", "same-conversation", MsgRole.USER, "A-only")
        await mem.add_message("project-b", "alice", "same-conversation", MsgRole.USER, "B-only")
        a = await mem.get_lightweight_context("project-a", "alice", "same-conversation")
        b = await mem.get_lightweight_context("project-b", "alice", "same-conversation")
        assert [item["content"] for item in a.recent_history] == ["A-only"]
        assert [item["content"] for item in b.recent_history] == ["B-only"]
        assert set(redis.lists) == {
            "wm:project-a:alice:same-conversation", "wm:project-b:alice:same-conversation"}
    asyncio.run(scenario())


def test_knowledge_vector_and_bm25_never_cross_projects():
    kb = knowledge_base()
    kb.add_documents("project-a", [{"title": "A secret", "content": "ZX-A-ONLY deployment guide"}])
    kb.add_documents("project-b", [{"title": "B guide", "content": "ZX-B-ONLY billing guide"}])
    assert {item["project_id"] for item in kb._vector_search("project-a", "guide", 10)} == {"project-a"}
    assert {item["project_id"] for item in kb._bm25_search("project-a", "ZX-A-ONLY", 10)} == {"project-a"}
    assert {item["project_id"] for item in kb._bm25_search("project-b", "ZX-A-ONLY", 10)} <= {"project-b"}
    assert kb.project_doc_count("project-a") == 1
    assert kb.project_doc_count("project-b") == 1


def test_experience_keeps_phase1_project_and_user_scope():
    async def scenario():
        manager = ExperienceManager(
            ExperienceStore(ExperienceRedis()), ExperienceIndex(ExperienceCollection()),
            SimpleNamespace(judge=AsyncMock(return_value=QualityScores(.9, .9, .9, .9))))
        req = SimpleNamespace(
            project_id="project-a", user_id="alice", conv_id="c", request_id="r",
            message="OAuth 401", intent=IntentCategory.TECHNICAL_LOGIN, entities={},
            execution_records=[{"agent_type": "technical", "success": True, "fallback": False}],
            experience_tools=[], retrieval_session=None,
        )
        result = SimpleNamespace(response="refresh token", escalated=False)
        case_id = await manager.submit(req, result)
        await manager.process("project-a", "alice", case_id)
        assert await manager.search("project-a", "alice", "OAuth 401")
        assert await manager.search("project-b", "alice", "OAuth 401") == []
        assert await manager.search("project-a", "bob", "OAuth 401") == []
    asyncio.run(scenario())


def test_project_chat_authorizes_before_context_planning(monkeypatch):
    memory = SimpleNamespace(begin_turn=AsyncMock())
    monkeypatch.setattr(main, "_project_service", project_service())
    monkeypatch.setattr(main, "_memory", memory)
    monkeypatch.setattr(main, "_orchestrator", SimpleNamespace())
    monkeypatch.setattr(main, "_context_pipeline", SimpleNamespace())
    with pytest.raises(HTTPException) as exc:
        asyncio.run(main.project_chat("project-a", main.ChatRequest(message="secret", user_id="bob")))
    assert exc.value.status_code == 403
    memory.begin_turn.assert_not_awaited()

    with pytest.raises(HTTPException) as mismatch:
        asyncio.run(main.project_chat("project-a", main.ChatRequest(
            message="x", user_id="alice", project_id="project-b")))
    assert mismatch.value.status_code == 409


def test_tool_cache_is_partitioned_by_project_id():
    async def scenario():
        calls = []

        async def handler(params, context):
            calls.append((params["project_id"], context["project_id"]))
            return [{"project_id": params["project_id"]}]

        manager = object.__new__(MCPToolManager)
        manager._tools = {"knowledge_search": Tool(
            "knowledge_search", "", handler,
            {"type": "object", "properties": {"project_id": {"type": "string"}},
             "required": ["project_id"]}, cache_ttl=60)}
        manager._cache = {}
        a1 = await manager.call("knowledge_search", {"project_id": "project-a"}, {"project_id": "project-a"})
        b = await manager.call("knowledge_search", {"project_id": "project-b"}, {"project_id": "project-b"})
        a2 = await manager.call("knowledge_search", {"project_id": "project-a"}, {"project_id": "project-a"})
        assert a1.data == [{"project_id": "project-a"}]
        assert b.data == [{"project_id": "project-b"}]
        assert a2.cached and calls == [("project-a", "project-a"), ("project-b", "project-b")]
    asyncio.run(scenario())


def test_authorized_project_flows_through_chat_context_and_retrieval(monkeypatch):
    intent = IntentResult(IntentCategory.REFUND, .95, UrgencyLevel.LOW, "billing", {}, "", 0,
                          {"pattern": 1.0})
    orchestrator = AgentOrchestrator("test", model="test")
    orchestrator.recognize_intent = AsyncMock(return_value=intent)
    response = SimpleNamespace(content=[{"type": "text", "text": "answer"}])
    for agents in orchestrator._pool.values():
        agents[0]._client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(return_value=response)))
    memory = SimpleNamespace(
        get_lightweight_context=AsyncMock(return_value=LightweightContext()),
        begin_turn=AsyncMock(return_value=1), add_message=AsyncMock())
    knowledge = SimpleNamespace(search_with_rewrite=AsyncMock(return_value=ToolResult(
        True, [{"title": "A policy", "content": "A-only"}], "knowledge_search")))
    pipeline = ContextPipeline(orchestrator, ContextPlanner(None, "test"), memory, knowledge)
    seen = {}
    original_run = orchestrator.run

    async def capture(req):
        seen["request"] = req
        return await original_run(req)

    orchestrator.run = capture
    monkeypatch.setattr(main, "_project_service", project_service())
    monkeypatch.setattr(main, "_orchestrator", orchestrator)
    monkeypatch.setattr(main, "_memory", memory)
    monkeypatch.setattr(main, "_context_pipeline", pipeline)
    body = asyncio.run(main.project_chat("project-a", main.ChatRequest(
        message="退款多久到账", user_id="alice", debug_context=True))).model_dump()
    assert body["project_id"] == "project-a"
    assert body["context_debug"]["project_id"] == "project-a"
    assert seen["request"].project_id == "project-a"
    assert seen["request"].project_config == project_service().repository.get("project-a").config
    assert seen["request"].context_bundle.project_id == "project-a"
    assert seen["request"].retrieval_session.project_id == "project-a"
    memory.begin_turn.assert_awaited_once()
    assert memory.begin_turn.await_args.args[0] == "project-a"
    assert all(call.args[0] == "project-a" for call in memory.add_message.await_args_list)
    assert knowledge.search_with_rewrite.await_args.args[:3] == (
        "knowledge_search", "退款多久到账", "project-a")
