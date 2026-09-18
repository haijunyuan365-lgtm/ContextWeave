import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from api import main
from mcp.knowledge_base import KnowledgeBase
from mcp.rag_config import RAGConfig
from mcp.tool_manager import MCPToolManager, Tool, ToolResult


class FakeCollection:
    def __init__(self):
        self.ids = []
        self.documents = []
        self.metadatas = []
        self.embeddings = {}

    def count(self):
        return len(self.ids)

    def add(self, ids, documents, metadatas):
        self.ids.extend(ids)
        self.documents.extend(documents)
        self.metadatas.extend(metadatas)

    def get(self, ids=None, where=None, include=None):
        selected = range(len(self.ids)) if ids is None else [self.ids.index(item) for item in ids]
        indexes = [index for index in selected if not where or all(
            self.metadatas[index].get(key) == value for key, value in where.items())]
        result = {"ids": [self.ids[index] for index in indexes]}
        if include and "documents" in include:
            result["documents"] = [self.documents[index] for index in indexes]
        if include and "metadatas" in include:
            result["metadatas"] = [self.metadatas[index] for index in indexes]
        if include and "embeddings" in include:
            result["embeddings"] = [self.embeddings.get(self.ids[index]) for index in indexes]
        return result


def bare_knowledge_base() -> KnowledgeBase:
    kb = object.__new__(KnowledgeBase)
    kb.rag_config = RAGConfig()
    kb._bm25_lock = threading.RLock()
    kb._bm25 = None
    kb._bm25_items = ()
    kb._collection = FakeCollection()
    return kb


def item(chunk_id, title, score=0.1, sources=None):
    return {
        "chunk_id": chunk_id,
        "title": title,
        "content": title + " content",
        "chunk": 0,
        "score": score,
        "retrieval_sources": sources or ["vector"],
    }


def test_bm25_index_refreshes_after_document_add_and_preserves_error_code():
    kb = bare_knowledge_base()
    assert "zx-900" in kb._tokenize("错误码 ZX-900")
    assert kb._bm25_search("project-a", "ZX-900", 3) == []
    kb.add_documents("project-a", [{"title": "认证故障", "content": "出现 ZX-900 时需要重新登录"}])
    results = kb._bm25_search("project-a", "ZX-900", 3)
    assert results and results[0]["title"] == "认证故障"
    assert results[0]["retrieval_sources"] == ["bm25"]


def test_per_query_rrf_merges_vector_and_bm25_by_chunk_id():
    kb = bare_knowledge_base()
    vector = [item("same", "shared", sources=["vector"]), item("vector", "semantic")]
    lexical = [item("same", "shared", sources=["bm25"]), item("bm25", "exact", sources=["bm25"])]
    fused = kb._rrf_fuse([("vector", vector), ("bm25", lexical)], 10)
    assert [entry["chunk_id"] for entry in fused].count("same") == 1
    assert fused[0]["chunk_id"] == "same"
    assert fused[0]["retrieval_sources"] == ["bm25", "vector"]


def test_hybrid_search_degrades_to_healthy_branch():
    kb = bare_knowledge_base()
    kb._vector_search = Mock(side_effect=RuntimeError("vector down"))
    kb._bm25_search = Mock(return_value=[item("lexical", "exact", sources=["bm25"])])
    result = asyncio.run(kb.search_async("project-a", "exact", 3))
    assert [entry["chunk_id"] for entry in result] == ["lexical"]


def test_cross_query_rrf_prefers_original_query_and_deduplicates():
    manager = object.__new__(MCPToolManager)
    manager.rag_config = RAGConfig(rerank_candidates=12)
    manager._tools = {}
    manager.rewrite_query = AsyncMock(return_value=["original", "rewrite"])
    manager.call = AsyncMock(side_effect=[
        ToolResult(True, [item("a", "A"), item("b", "B")], "knowledge_search"),
        ToolResult(True, [item("b", "B"), item("a", "A")], "knowledge_search"),
    ])
    manager._rerank = AsyncMock(side_effect=lambda query, items, top_k: items[:top_k])
    result = asyncio.run(manager.search_with_rewrite("knowledge_search", "original", "project-a", top_k=3))
    assert [entry["chunk_id"] for entry in result.data] == ["a", "b"]
    assert manager.call.call_args_list[0].args[1]["top_k"] == 10


def test_explicit_top_k_can_exceed_default_rerank_pool():
    manager = object.__new__(MCPToolManager)
    manager.rag_config = RAGConfig(rerank_candidates=12)
    manager._tools = {}
    candidates = [item(str(index), str(index), 1.0 - index / 100) for index in range(15)]
    manager.rewrite_query = AsyncMock(return_value=["original"])
    manager.call = AsyncMock(return_value=ToolResult(True, candidates, "knowledge_search"))
    manager._rerank = AsyncMock(side_effect=lambda query, items, top_k: items[:top_k])
    result = asyncio.run(manager.search_with_rewrite("knowledge_search", "original", "project-a", top_k=15))
    assert len(result.data) == 15


def test_mmr_uses_embeddings_to_avoid_near_duplicate_candidates():
    manager = object.__new__(MCPToolManager)
    candidates = [item("a", "best", 1.0), item("b", "duplicate", 0.99),
                  item("c", "complement", 0.95), item("d", "irrelevant", 0.0)]
    vectors = {"a": [1.0, 0.0], "b": [0.999, 0.01], "c": [0.0, 1.0], "d": [-1.0, 0.0]}

    async def embeddings(project_id, chunk_ids):
        assert project_id == "project-a"
        return {chunk_id: vectors[chunk_id] for chunk_id in chunk_ids}

    tool = Tool("knowledge_search", "", Mock(), {}, embedding_provider=embeddings)
    selected = asyncio.run(manager._mmr_select(tool, candidates, 2, 0.8, "project-a"))
    assert [entry["chunk_id"] for entry in selected] == ["a", "c"]


def test_rerank_validates_duplicates_and_fills_missing_indexes():
    manager = object.__new__(MCPToolManager)
    manager._model = "test"
    manager._client = SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(
        return_value=SimpleNamespace(content=[SimpleNamespace(type="text", text="[2, 2, 99]")]))))
    candidates = [item("a", "A"), item("b", "B"), item("c", "C"), item("d", "D")]
    result = asyncio.run(manager._rerank("query", candidates, 3))
    assert [entry["chunk_id"] for entry in result] == ["c", "a", "b"]


def test_embedding_provider_is_internal_only():
    kb = bare_knowledge_base()
    kb._collection.ids = ["a"]
    kb._collection.documents = ["secret"]
    kb._collection.metadatas = [{"project_id": "project-a", "title": "A", "chunk_index": 0}]
    kb._collection.embeddings = {"a": [0.1, 0.2]}
    assert kb.get_embeddings("project-a", ["a"]) == {"a": [0.1, 0.2]}
    public_item = kb._rrf_fuse([("bm25", [item("a", "A", sources=["bm25"])])], 1)[0]
    assert "embedding" not in public_item and "embeddings" not in public_item


def test_search_endpoint_uses_configured_top_three_by_default(monkeypatch):
    manager = SimpleNamespace(
        rag_config=RAGConfig(final_top_k=3),
        search_with_rewrite=AsyncMock(return_value=ToolResult(True, [], "knowledge_search")),
    )
    monkeypatch.setattr(main, "_tool_manager", manager)
    result = asyncio.run(main.search("refund", None))
    assert result["results"] == []
    manager.search_with_rewrite.assert_awaited_once_with(
        "knowledge_search", "refund", "default", top_k=3, context={"project_id": "default"})
