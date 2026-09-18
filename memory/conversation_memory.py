"""
亮点：多轮对话记忆管理

三级记忆架构，模拟人类记忆机制：
  1. 工作记忆（Redis）—— 当前会话的最近 N 条消息，毫秒级读写
  2. 情景记忆（ChromaDB）—— 跨会话的历史对话，按语义相似度检索
  3. 用户画像（ChromaDB）—— 从对话中提炼的长期偏好和实体

关键设计：
  - 上下文构建时三级记忆融合，按重要性 + 时效性排序
  - 工作记忆超过阈值时自动压缩（LLM 摘要），防止 context 爆炸
  - 情景检索由 Chroma 内置 embedding 支持；结构化画像按用户 ID 查询
  - 用户消息独立排队，LLM 提取候选，确定性 Resolver 维护版本和来源
"""
import hashlib
import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import redis.asyncio as redis
from anthropic import AsyncAnthropic

from core.llm_utils import extract_text_content
from core.context_planner import LightweightContext
from memory.evolution import SourceMessage, now
from memory.evolution_runtime import MemoryEvolution, MemoryStore

logger = logging.getLogger(__name__)


class MsgRole(Enum):
    USER      = "user"
    ASSISTANT = "assistant"
    SYSTEM    = "system"


@dataclass
class Message:
    role:       MsgRole
    content:    str
    timestamp:  datetime = field(default_factory=datetime.now)
    metadata:   Dict[str, Any] = field(default_factory=dict)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sequence: int = 0


@dataclass
class MemoryContext:
    """传给 Agent 的完整上下文。"""
    recent_messages:  List[Message]   # 工作记忆：最近对话
    relevant_history: List[str]       # 情景记忆：语义相关的历史片段
    user_profile:     Dict[str, Any]  # 用户画像：偏好、常用实体
    summary:          str             # 当前会话摘要（压缩后）

    @staticmethod
    def _clean(text: str) -> str:
        """移除 Unicode 代理字符，防止编码错误。"""
        return text.encode("utf-8", errors="ignore").decode("utf-8")

    def to_prompt_text(self) -> str:
        """将记忆上下文格式化为 LLM 可用的文本。"""
        parts = []
        if self.summary:
            parts.append(f"[会话摘要]\n{self._clean(self.summary)}")
        if self.relevant_history:
            parts.append("[相关历史]\n" + "\n".join(f"- {self._clean(h)}" for h in self.relevant_history[:3]))
        if self.user_profile:
            parts.append(f"[用户画像]\n{json.dumps(self.user_profile, ensure_ascii=True)}")
        if self.recent_messages:
            parts.append("[最近对话]")
            for m in self.recent_messages[-8:]:
                parts.append(f"{m.role.value}: {self._clean(m.content)}")
        return "\n\n".join(parts)


class MemoryManager:
    """
    三级记忆管理器。

    工作记忆存 Redis（TTL 24h），情景记忆和用户画像存 ChromaDB（持久化）。
    """

    WORKING_MAX   = 20    # 工作记忆最大条数，超过则触发压缩
    COMPRESS_AT   = 15    # 达到此条数时压缩，保留摘要 + 最近 5 条
    HISTORY_TOP_K = 5     # 情景记忆检索返回条数
    SUMMARY_MAX_CHARS = 800
    PROFILE_DOC_PREFIX = "user_profile:"

    def __init__(
        self,
        redis_url:    str = "redis://localhost:6379/0",
        chroma_host:  str = "localhost",
        chroma_port:  int = 8000,
        chroma_path:  str = "./data/chroma",
        api_key:      str = "",
        base_url:     Optional[str] = None,
        model:        str = "claude-3-5-sonnet-20241022",
    ):
        # Native embedding libraries are only needed when constructing a real backend.
        import chromadb

        kwargs: Dict[str, Any] = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
        self._client = AsyncAnthropic(**kwargs)
        self._model  = model

        self._redis = redis.from_url(redis_url, decode_responses=True)

        # ChromaDB：优先连接独立服务（docker compose 模式），连不上则降级为本地嵌入式
        try:
            # HttpClient 默认也会初始化 ChromaDB telemetry；显式关闭避免 posthog 兼容性错误日志。
            chroma = chromadb.HttpClient(
                host=chroma_host,
                port=chroma_port,
                settings=chromadb.Settings(anonymized_telemetry=False),
            )
            chroma.heartbeat()  # 测试连接
            logger.info(f"ChromaDB 已连接: {chroma_host}:{chroma_port}")
        except Exception:
            logger.info(f"ChromaDB 服务不可用，使用本地嵌入式模式: {chroma_path}")
            chroma = chromadb.PersistentClient(
                path=chroma_path,
                settings=chromadb.Settings(anonymized_telemetry=False),
            )

        # 情景记忆：存储历史对话片段
        self._chroma = chroma
        self._episodic = chroma.get_or_create_collection("episodic")
        # 用户画像：存储提炼出的偏好和实体
        self._profile  = chroma.get_or_create_collection("user_profile")
        self._memory_store = MemoryStore(chroma.get_or_create_collection("user_memory", embedding_function=None))
        self._evolution = MemoryEvolution(self._redis, self._memory_store, self._client, self._model)
        self._conversation_locks: Dict[tuple, asyncio.Lock] = {}

    async def create_experience(self):
        """Optional experience backend sharing existing Redis/Chroma connections."""
        from experience.runtime import ExperienceManager, ExperienceStore, ExperienceIndex, enabled
        from evaluation.evaluator import LLMJudge
        if not enabled():
            return None
        try:
            collection = await asyncio.wait_for(asyncio.to_thread(
                self._chroma.get_or_create_collection, "experience_cases_v2", metadata={"hnsw:space": "cosine"}), 5)
            manager = ExperienceManager(ExperienceStore(self._redis), ExperienceIndex(collection),
                                        LLMJudge(self._client.with_options(max_retries=0), self._model))
            await manager.start()
            return manager
        except Exception:
            logger.warning("Experience backend unavailable; continuing without experience")
            return None

    # ── 写入 ──────────────────────────────────────────────────────────────────

    async def begin_turn(self, project_id: str, user_id: str, conv_id: str, content: str) -> SourceMessage:
        """Reserve source order at receipt, before model calls can finish out of order."""
        user_id, conv_id = self._safe_text(user_id), self._safe_text(conv_id)
        project_id = self._safe_text(project_id)
        sequence = await self._redis.incr(MemoryEvolution.key(project_id, user_id, "sequence"))
        return SourceMessage(str(uuid.uuid4()), user_id, conv_id, sequence, now(),
                             self._safe_text(content), project_id=project_id)

    def _conversation_lock(self, project_id: str, user_id: str, conv_id: str) -> asyncio.Lock:
        return self._conversation_locks.setdefault((project_id, user_id, conv_id), asyncio.Lock())

    async def add_message(
        self, project_id: str, user_id: str, conv_id: str, role: MsgRole, content: str,
        metadata: Optional[Dict[str, Any]] = None, *, source: Optional[SourceMessage] = None,
    ) -> Message:
        """Atomically retain user evidence separately from compressible working memory."""
        project_id, user_id, conv_id = (self._safe_text(project_id), self._safe_text(user_id),
                                        self._safe_text(conv_id))
        content = self._safe_text(content)
        if source is None and role == MsgRole.USER:
            source = await self.begin_turn(project_id, user_id, conv_id, content)
        if source is not None:
            if (source.project_id != project_id or source.user_id != user_id
                    or source.conv_id != conv_id or source.role != "user"):
                raise ValueError("source scope mismatch")
            if role == MsgRole.USER and source.content != content:
                raise ValueError("source content mismatch")
        msg = Message(role, content, metadata={
            self._safe_text(k): self._safe_metadata_value(v) for k, v in (metadata or {}).items()
        })
        if source is not None:
            msg.sequence = source.sequence
            msg.message_id = source.message_id if role == MsgRole.USER else source.message_id + ":" + role.value
            if role == MsgRole.USER:
                msg.timestamp = datetime.fromisoformat(source.timestamp)
        key = self._wm_key(project_id, user_id, conv_id)
        async with self._conversation_lock(project_id, user_id, conv_id):
            pipe = self._redis.pipeline(transaction=True)
            pipe.lpush(key, json.dumps({
                "role": role.value, "content": content, "ts": msg.timestamp.isoformat(),
                "metadata": msg.metadata, "message_id": msg.message_id, "sequence": msg.sequence,
            }, ensure_ascii=False))
            pipe.expire(key, 86400)
            if role == MsgRole.USER:
                self._evolution.enqueue_commands(pipe, source)
            await pipe.execute()
            if role == MsgRole.USER:
                self._evolution.schedule(project_id, user_id)
            if await self._redis.llen(key) >= self.COMPRESS_AT:
                await self._compress_locked(project_id, user_id, conv_id)
        return msg

    async def update_profile(self, project_id: str, user_id: str, conv_id: str) -> None:
        """Compatibility entrypoint: schedule durable pending user messages, never rewrite legacy data."""
        self._evolution.schedule(self._safe_text(project_id), self._safe_text(user_id))

    async def start(self) -> None:
        await self._evolution.start()

    async def inspect_memory(self, project_id: str, user_id: str) -> Dict[str, Any]:
        project_id, user_id = self._safe_text(project_id), self._safe_text(user_id)
        legacy = await self._get_legacy_profile(project_id, user_id, strict=True)
        return await self._evolution.inspect(project_id, user_id, legacy)

    async def memory_events(self, project_id: str, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        return await self._evolution.events(self._safe_text(project_id), self._safe_text(user_id), limit)

    async def memory_status(self, project_id: str, user_id: str) -> Dict[str, Any]:
        return await self._evolution.status(self._safe_text(project_id), self._safe_text(user_id))

    # ── 读取 ──────────────────────────────────────────────────────────────────

    async def get_context(self, project_id: str, user_id: str, conv_id: str, query: str = "") -> MemoryContext:
        """
        构建完整的记忆上下文。

        query 用于从情景记忆中检索语义相关的历史片段。
        """
        # Legacy full-context wrapper. New requests use the selective public APIs.
        project_id = self._safe_text(project_id)
        user_id = self._safe_text(user_id)
        conv_id = self._safe_text(conv_id)
        query = self._safe_text(query)

        recent = await self._get_working_memory(project_id, user_id, conv_id)

        # 2. 情景记忆（跨会话语义检索）
        history = await self._search_episodic(
            project_id, user_id,
            conv_id,
            query or (recent[-1].content if recent else ""),
        )

        # 3. 用户画像
        profile = await self._get_profile(project_id, user_id)

        # 4. 会话摘要（如果已压缩过）
        summary = await self._redis.get(self._summary_key(project_id, user_id, conv_id)) or ""

        return MemoryContext(
            recent_messages=recent,
            relevant_history=history,
            user_profile=profile,
            summary=summary,
        )

    async def get_lightweight_context(self, project_id: str, user_id: str, conv_id: str) -> LightweightContext:
        project_id, user_id, conv_id = (self._safe_text(project_id), self._safe_text(user_id), self._safe_text(conv_id))
        recent, summary = await asyncio.gather(
            self._get_working_memory(project_id, user_id, conv_id, limit=5),
            self._redis.get(self._summary_key(project_id, user_id, conv_id)),
        )
        return LightweightContext(
            recent_history=[{"role": m.role.value, "content": m.content} for m in recent],
            summary=self._safe_text(summary or "")[:800],
        )

    async def search_episodic(self, project_id: str, user_id: str, conv_id: str,
                              query: str, top_k: int = 5) -> List[str]:
        if type(top_k) is not int or not 1 <= top_k <= self.HISTORY_TOP_K:
            raise ValueError("top_k must be between 1 and 5")
        return await self._search_episodic(project_id, user_id, conv_id, query, top_k=top_k, strict=True)

    async def get_profile(self, project_id: str, user_id: str) -> Dict[str, Any]:
        return await self._get_profile(self._safe_text(project_id), self._safe_text(user_id), strict=True)

    # ── 压缩（防止 context 爆炸）─────────────────────────────────────────────

    async def _compress(self, project_id: str, user_id: str, conv_id: str) -> None:
        async with self._conversation_lock(project_id, user_id, conv_id):
            await self._compress_locked(project_id, user_id, conv_id)

    async def _compress_locked(self, project_id: str, user_id: str, conv_id: str) -> None:
        """
        工作记忆压缩：
          1. 用 LLM 对旧消息生成摘要
          2. 摘要存 Redis（覆盖旧摘要）
          3. 旧消息存入情景记忆（ChromaDB）供跨会话检索
          4. 工作记忆只保留最近 5 条
        """
        messages = await self._get_working_memory(project_id, user_id, conv_id, limit=0)
        if len(messages) < self.COMPRESS_AT:
            return

        to_compress = messages[:-5]   # 保留最近 5 条

        # LLM 摘要
        text = self._safe_text("\n".join(f"{m.role.value}: {m.content}" for m in to_compress))
        prompt = self._safe_text(f"用 2-3 句话总结以下对话的关键信息：\n{text}")
        try:
            resp = await self._client.messages.create(
                model=self._model, max_tokens=256, temperature=0.0,
                messages=[{"role": "user", "content": prompt}],
            )
            summary = self._safe_text(extract_text_content(resp.content)).strip()
        except Exception as exc:
            logger.warning("工作记忆摘要失败: %s", type(exc).__name__)
            return
        if not summary:
            return

        # 存摘要到 Redis
        skey = self._summary_key(project_id, user_id, conv_id)
        old_summary = await self._redis.get(skey) or ""
        new_summary = await self._merge_summary(old_summary, summary)
        stored = await self._store_episodic(
            project_id, user_id, conv_id, text, summary, source_ids=[m.message_id for m in to_compress])
        if not stored:
            return  # Do not discard history when archival storage failed.
        key = self._wm_key(project_id, user_id, conv_id)
        pipe = self._redis.pipeline(transaction=True)
        pipe.setex(skey, 86400, new_summary)
        pipe.ltrim(key, 0, 4)  # Keep existing serialized messages and IDs in their original order.
        pipe.expire(key, 86400)
        await pipe.execute()
        logger.info(f"工作记忆压缩完成: {user_id}/{conv_id}，摘要 {len(summary)} 字")

    # ── 内部辅助 ──────────────────────────────────────────────────────────────

    async def _get_working_memory(self, project_id: str, user_id: str, conv_id: str,
                                  limit: Optional[int] = None) -> List[Message]:
        key  = self._wm_key(project_id, user_id, conv_id)
        end = -1 if limit == 0 else (limit or self.WORKING_MAX) - 1
        raws = await self._redis.lrange(key, 0, end)
        msgs = []
        for raw in reversed(raws):  # Redis lpush 最新在前，reversed 还原时序
            d = json.loads(raw)
            msgs.append(Message(
                role=MsgRole(d["role"]),
                content=d["content"],
                timestamp=datetime.fromisoformat(d["ts"]),
                metadata=d.get("metadata", {}),
                message_id=d.get("message_id") or "legacy:" + hashlib.sha256((key + raw).encode()).hexdigest(),
                sequence=d.get("sequence", 0),
            ))
        return msgs

    async def _search_episodic(self, project_id: str, user_id: str, conv_id: str, query: str,
                             top_k: Optional[int] = None, strict: bool = False) -> List[str]:
        """语义检索情景记忆。ChromaDB 内置 embedding，不依赖外部 API。"""
        query_text = self._safe_text(query).strip()
        if not query_text:
            return []
        count = top_k or self.HISTORY_TOP_K
        try:
            results = await self._query_episodic(
                query_text,
                n_results=count,
                where={"$and": [{"project_id": self._safe_text(project_id)},
                                {"user_id": self._safe_text(user_id)}, {"conv_id": self._safe_text(conv_id)}]},
            )
            docs = self._extract_docs(results)
            if len(docs) < count:
                fallback = await self._query_episodic(
                    query_text,
                    n_results=count,
                    where={"$and": [{"project_id": self._safe_text(project_id)},
                                    {"user_id": self._safe_text(user_id)}]},
                )
                docs.extend(self._extract_docs(fallback))
            return self._dedupe_texts(docs)[:count]
        except Exception as ex:
            if strict:
                raise
            logger.warning(f"情景记忆检索失败: {ex}")
            return []

    async def _store_episodic(self, project_id: str, user_id: str, conv_id: str, text: str, summary: str,
                              source_ids: Optional[List[str]] = None) -> bool:
        """将压缩后的对话片段存入情景记忆。ChromaDB 内置 embedding，不依赖外部 API。"""
        try:
            project_id = self._safe_text(project_id)
            user_id = self._safe_text(user_id)
            conv_id = self._safe_text(conv_id)
            text = self._safe_text(text)
            summary = self._safe_text(summary)
            identity = json.dumps([project_id, user_id, conv_id, source_ids or [text]], ensure_ascii=False)
            doc_id = hashlib.sha256(identity.encode()).hexdigest()
            # 直接传 documents，ChromaDB 内置模型自动生成 embedding
            await asyncio.to_thread(
                self._episodic.upsert,
                ids=[doc_id],
                documents=[summary],
                metadatas=[{"project_id": project_id, "user_id": user_id, "conv_id": conv_id,
                            "ts": datetime.now().isoformat(), "full_text": self._safe_text(text[:500]),
                            "source_message_ids": json.dumps(source_ids or [])}],
            )
            return True
        except Exception as ex:
            logger.warning("存储情景记忆失败: %s", type(ex).__name__)
            return False

    async def _get_profile(self, project_id: str, user_id: str, strict: bool = False) -> Dict[str, Any]:
        try:
            legacy = await self._get_legacy_profile(project_id, user_id, strict=True)
            evolution = getattr(self, "_evolution", None)
            return await evolution.profile(project_id, user_id, legacy) if evolution is not None else legacy
        except Exception:
            if strict:
                raise
            return {}

    async def _get_legacy_profile(self, project_id: str, user_id: str, strict: bool = False) -> Dict[str, Any]:
        """获取用户画像（取最新一条）。"""
        try:
            doc_id = self._profile_doc_id(project_id, user_id)
            direct = await asyncio.to_thread(self._profile.get, ids=[doc_id])
            if direct.get("documents"):
                return json.loads(direct["documents"][0])

            results = await asyncio.to_thread(self._profile.get, where={"$and": [
                {"project_id": project_id}, {"user_id": user_id}]})
            return self._latest_profile_from_results(results)
        except Exception:
            if strict:
                raise
        return {}

    async def close(self) -> None:
        """关闭异步 Redis 连接。"""
        await self._evolution.close()
        await self._client.close()
        await self._redis.aclose()

    @staticmethod
    def _wm_key(project_id: str, user_id: str, conv_id: str) -> str:
        return f"wm:{project_id}:{user_id}:{conv_id}"

    @staticmethod
    def _summary_key(project_id: str, user_id: str, conv_id: str) -> str:
        return f"summary:{project_id}:{user_id}:{conv_id}"

    @classmethod
    def _profile_doc_id(cls, project_id: str, user_id: str) -> str:
        return f"{cls.PROFILE_DOC_PREFIX}{project_id}:{user_id}"

    @staticmethod
    def _safe_text(value: Any) -> str:
        """转成 ChromaDB 可接受的普通 UTF-8 字符串。"""
        if value is None:
            return ""
        if not isinstance(value, str):
            value = str(value)
        return value.encode("utf-8", errors="ignore").decode("utf-8")

    @classmethod
    def _safe_metadata_value(cls, value: Any) -> Any:
        """递归清洗 metadata，避免 Redis/ChromaDB 后续读写遇到非法 UTF-8。"""
        if isinstance(value, str):
            return cls._safe_text(value)
        if isinstance(value, dict):
            return {cls._safe_text(k): cls._safe_metadata_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [cls._safe_metadata_value(v) for v in value]
        return value

    async def _query_episodic(
        self,
        query_text: str,
        n_results: int,
        where: Dict[str, Any],
    ) -> Dict[str, Any]:
        return await asyncio.to_thread(
            self._episodic.query,
            query_texts=[query_text],
            n_results=n_results,
            where=where,
        )

    @staticmethod
    def _extract_docs(results: Dict[str, Any]) -> List[str]:
        docs = results.get("documents") or []
        if not docs:
            return []
        first = docs[0] if isinstance(docs[0], list) else docs
        return [doc for doc in first if isinstance(doc, str) and doc.strip()]

    @staticmethod
    def _dedupe_texts(values: List[str]) -> List[str]:
        seen = set()
        deduped: List[str] = []
        for value in values:
            text = value.strip()
            if not text or text in seen:
                continue
            seen.add(text)
            deduped.append(text)
        return deduped

    async def _merge_summary(self, old_summary: str, new_summary: str) -> str:
        old_summary = self._safe_text(old_summary).strip()
        new_summary = self._safe_text(new_summary).strip()
        if not old_summary:
            return new_summary[: self.SUMMARY_MAX_CHARS]
        if not new_summary:
            return old_summary[: self.SUMMARY_MAX_CHARS]

        prompt = self._safe_text(
            f"""你是对话摘要器。请把下面两段摘要合并为一段不超过 {self.SUMMARY_MAX_CHARS} 个中文字符的摘要。
保留：用户偏好、关键实体、待办事项、约束条件、未解决问题。
只输出摘要正文，不要编号，不要解释。

旧摘要:
{old_summary}

新增摘要:
{new_summary}
"""
        )
        try:
            resp = await self._client.messages.create(
                model=self._model,
                max_tokens=256,
                temperature=0.0,
                messages=[{"role": "user", "content": prompt}],
            )
            merged = self._safe_text(extract_text_content(resp.content)).strip()
            if merged:
                return merged[: self.SUMMARY_MAX_CHARS]
        except Exception as ex:
            logger.warning(f"合并摘要失败，回退为截断拼接: {ex}")

        merged = self._safe_text(f"{old_summary}\n{new_summary}").strip()
        return merged[-self.SUMMARY_MAX_CHARS :]

    @staticmethod
    def _latest_profile_from_results(results: Dict[str, Any]) -> Dict[str, Any]:
        documents = results.get("documents") or []
        metadatas = results.get("metadatas") or []
        if not documents:
            return {}

        candidates: List[tuple[str, Dict[str, Any], str]] = []
        for idx, doc in enumerate(documents):
            if not doc:
                continue
            metadata = metadatas[idx] if idx < len(metadatas) and isinstance(metadatas[idx], dict) else {}
            ts = str(metadata.get("updated_at") or metadata.get("ts") or "")
            candidates.append((ts, metadata, doc))

        if not candidates:
            return {}

        candidates.sort(key=lambda item: item[0], reverse=True)
        latest_doc = candidates[0][2]
        try:
            return json.loads(latest_doc)
        except Exception:
            return {}
