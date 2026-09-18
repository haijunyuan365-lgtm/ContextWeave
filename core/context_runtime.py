"""Request-local retrieval, context assembly and shared HTTP/CLI preparation."""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from core.context_planner import ContextPlan, LightweightContext, SOURCES

logger = logging.getLogger(__name__)


@dataclass
class RetrievalResult:
    status: str
    data: Any = None
    reranked: bool = False
    cached: bool = False
    error: str = ""


@dataclass
class ContextBundle:
    project_id: str = "default"
    project_config: Any = None
    recent_history: list[dict[str, str]] = field(default_factory=list)
    summary: str = ""
    memories: list[str] = field(default_factory=list)
    profile: dict[str, Any] = field(default_factory=dict)
    knowledge: list[dict[str, Any]] = field(default_factory=list)
    skills: list[Any] = field(default_factory=list)
    experiences: list[Any] = field(default_factory=list)
    sources: dict[str, dict[str, Any]] = field(default_factory=lambda: {
        s: {"status": "unconfigured" if s == "experience" else "skipped", "count": 0,
            "latency_ms": 0.0, "calls": 0, "cache_hits": 0} for s in SOURCES
    })
    skill_prompts: dict[str, str] = field(default_factory=dict)

    def to_prompt_text(self) -> str:
        parts = []
        if self.summary:
            parts.append("[会话摘要]\n" + self.summary[:800])
        if self.memories:
            parts.append("[相关历史，仅供参考，不代表当前业务状态]\n" + "\n".join(self.memories[:3]))
        if self.profile:
            parts.append("[用户画像，仅供参考]\n" + json.dumps(self.profile, ensure_ascii=False))
        if self.recent_history:
            parts.append("[最近对话]\n" + "\n".join(f"{m['role']}: {m['content']}" for m in self.recent_history))
        if self.knowledge:
            parts.append("[知识库检索结果]\n" + "\n".join(
                f"{i}. {item.get('title', '未命名文档')}\n{str(item['content'])[:600]}"
                for i, item in enumerate(self.knowledge, 1)))
        if self.experiences:
            from experience.runtime import format_references
            parts.append(format_references(self.experiences))
        return "\n\n".join(parts)

    def skill_prompt_for(self, agent_type: str) -> str:
        return self.skill_prompts.get(agent_type, "")


class RetrievalSession:
    """One instance per request; completed results and in-flight work never cross requests."""
    def __init__(self, user_id: str, conv_id: str, bundle: ContextBundle,
                 memory: Any = None, knowledge: Any = None, experience: Any = None,
                 project_id: str = "default"):
        self.project_id, self.user_id, self.conv_id = project_id, user_id, conv_id
        self.bundle, self.memory, self.knowledge = bundle, memory, knowledge
        self.experience = experience
        if experience is not None:
            self.bundle.sources["experience"]["status"] = "skipped"
        self.knowledge_evidence: list = []
        self.evidence_failed = False
        self._cache: dict[tuple, RetrievalResult] = {}
        self._pending: dict[tuple, asyncio.Task] = {}
        self.knowledge_used = False

    async def retrieve(self, source: str, query: str = "", top_k: int | None = None) -> RetrievalResult:
        if source not in {"memory", "profile", "knowledge", "experience"}:
            raise ValueError("unsupported retrieval source")
        if top_k is None:
            config = getattr(self.knowledge, "rag_config", None) if source == "knowledge" else None
            top_k = getattr(config, "final_top_k", 3) if source == "knowledge" else 5
        maximum = 5 if source == "memory" else 20
        if source == "experience":
            top_k = min(top_k, 3) if type(top_k) is int else top_k
            maximum = 3
        if type(top_k) is not int or not 1 <= top_k <= maximum:
            raise ValueError(f"top_k must be between 1 and {maximum}")
        normalized = " ".join(query.split())
        if source == "profile":
            normalized, top_k = "", 5
        key = (source, self.project_id, self.user_id, self.conv_id, normalized, top_k)
        stat = self.bundle.sources[source]
        stat["calls"] += 1
        started = time.monotonic()
        hit = key in self._cache or key in self._pending
        try:
            if key in self._cache:
                result = self._cache[key]
            else:
                if key not in self._pending:
                    self._pending[key] = asyncio.create_task(self._fetch(source, normalized, top_k))
                task = self._pending[key]
                try:
                    result = await asyncio.shield(task)
                    if result.status in {"success", "empty"}:
                        self._cache[key] = result
                finally:
                    if task.done() and self._pending.get(key) is task:
                        del self._pending[key]
            result = copy.deepcopy(result)
            if source == "experience" and hit and result.data and self.experience is not None:
                try:
                    result.data = await self.experience.revalidate(self.project_id, self.user_id, result.data)
                    result.status = "success" if result.data else "empty"
                except Exception:
                    result = RetrievalResult("failed", error="经验状态核验失败")
            result.cached = hit
            stat.update(status=result.status, count=len(result.data) if isinstance(result.data, (list, dict)) else 0)
            if hit:
                stat["cache_hits"] += 1
            if source == "experience":
                stat["case_ids"] = [item["case_id"] for item in (result.data or [])]
            if source == "knowledge":
                if result.status in {"failed", "degraded"}:
                    self.evidence_failed = True
                for item in result.data or []:
                    if item not in self.knowledge_evidence:
                        self.knowledge_evidence.append(copy.deepcopy(item))
            return result
        finally:
            stat["latency_ms"] = round(stat["latency_ms"] + (time.monotonic() - started) * 1000, 2)

    async def _fetch(self, source: str, query: str, top_k: int) -> RetrievalResult:
        provider = self.experience if source == "experience" else self.knowledge if source == "knowledge" else self.memory
        if provider is None:
            return RetrievalResult("unconfigured", error="来源未配置")
        try:
            if source == "experience":
                data = await provider.search(self.project_id, self.user_id, query, top_k)
                return RetrievalResult("success" if data else "empty", data)
            if source == "knowledge":
                result = await provider.search_with_rewrite(
                    "knowledge_search", query, self.project_id, top_k=top_k,
                    context={"project_id": self.project_id})
                if not result.success or not isinstance(result.data, list):
                    return RetrievalResult("failed", error="知识检索失败")
                data = [d for d in result.data if isinstance(d, dict) and not d.get("fallback")
                        and isinstance(d.get("content"), str) and d["content"].strip()]
                if not data and any(isinstance(d, dict) and d.get("fallback") for d in result.data):
                    return RetrievalResult("failed", error="知识检索降级")
                # Mixed fallback and evidence is useful, but must be retriable.
                degraded = bool(getattr(result, "error", None)) or any(isinstance(d, dict) and d.get("fallback") for d in result.data)
                return RetrievalResult("degraded" if degraded else ("success" if data else "empty"),
                                       data[:top_k], bool(getattr(result, "reranked", False)))
            if source == "memory":
                data = await provider.search_episodic(
                    self.project_id, self.user_id, self.conv_id, query, top_k=top_k)
                if not isinstance(data, list) or any(not isinstance(item, str) for item in data):
                    return RetrievalResult("failed", error="历史记忆格式无效")
            else:
                data = await provider.get_profile(self.project_id, self.user_id)
                if not isinstance(data, dict):
                    return RetrievalResult("failed", error="用户画像格式无效")
            return RetrievalResult("success" if data else "empty", data)
        except Exception:
            # Provider errors may contain private query text; do not echo them into debug output.
            return RetrievalResult("failed", error="上下文来源读取失败")

    def mark_knowledge_delivered(self, data: list[Any]) -> None:
        if data:
            self.knowledge_used = True

    async def close(self) -> None:
        tasks = list(self._pending.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._pending.clear()
        self._cache.clear()


class ContextPipeline:
    def __init__(self, orchestrator: Any, planner: Any, memory: Any = None,
                 knowledge: Any = None, skills: Any = None, experience: Any = None):
        self.orchestrator, self.planner = orchestrator, planner
        self.memory, self.knowledge, self.skills = memory, knowledge, skills
        self.experience = experience

    async def prepare(self, req: Any) -> Any:
        light_started = time.monotonic()
        light_failed = False
        try:
            light = (await self.memory.get_lightweight_context(req.project_id, req.user_id, req.conv_id)
                     if self.memory is not None else LightweightContext((req.history or [])[-5:]))
        except Exception:
            light = LightweightContext((req.history or [])[-5:])
            light_failed = True
        light_ms = round((time.monotonic() - light_started) * 1000, 2)
        intent = await self.orchestrator.recognize_intent(req.message, history=light.recent_history or None,
                                                         summary=light.summary)
        req.history = light.recent_history or None
        req.intent, req.intent_group = intent.intent, intent.intent_group
        req.intent_confidence, req.urgency, req.entities = intent.confidence, intent.urgency, intent.entities
        req.intent_source_scores = dict(intent.source_scores)
        req.context_plan = await self.planner.plan(req.message, intent, light)
        bundle = ContextBundle(project_id=req.project_id, project_config=req.project_config,
                               recent_history=light.recent_history, summary=light.summary)
        for source, data in (("recent_history", light.recent_history), ("summary", light.summary)):
            bundle.sources[source] = {
                "status": "failed" if light_failed else ("success" if data else "empty"),
                "count": len(data) if source == "recent_history" else int(bool(data)),
                "latency_ms": light_ms, "calls": int(self.memory is not None), "cache_hits": 0,
            }
        session = RetrievalSession(req.user_id, req.conv_id, bundle, self.memory, self.knowledge, self.experience,
                                   project_id=req.project_id)
        req.context_bundle, req.retrieval_session = bundle, session
        plan = req.context_plan

        async def preload(source: str, query: str) -> None:
            result = await session.retrieve(source, query)
            if result.status in {"success", "empty", "degraded"}:
                attr = {"memory": "memories", "profile": "profile", "knowledge": "knowledge", "experience": "experiences"}[source]
                setattr(bundle, attr, result.data)

        try:
            await asyncio.gather(*(preload(s, getattr(plan, s + "_query", None) or req.message)
                                   for s in ("memory", "profile", "knowledge", "experience") if getattr(plan, "need_" + s)))
            if plan.need_skill:
                started = time.monotonic()
                stat = bundle.sources["skill"]
                stat["calls"] = 1
                if self.skills is None:
                    stat["status"] = "unconfigured"
                else:
                    try:
                        bundle.skills = self.skills.select(req.message)
                        bundle.skill_prompts = {role: self.skills.format_skills(
                            [s for s in bundle.skills if not s.agents or role in s.agents])
                            for role in ("general", "billing", "technical", "escalation")}
                        stat.update(status="success" if bundle.skills else "empty", count=len(bundle.skills))
                    except Exception:
                        stat["status"] = "failed"
                stat["latency_ms"] = round((time.monotonic() - started) * 1000, 2)
            req.context = bundle.to_prompt_text()
            return intent
        except BaseException:
            await session.close()
            raise

    async def close(self) -> None:
        if self.experience is not None:
            await self.experience.close()
        if self.planner.client is not None:
            await self.planner.client.close()

    async def capture(self, req: Any, result: Any) -> None:
        if self.experience is not None:
            try:
                await self.experience.submit(req, result)
            except Exception:
                logger.warning("experience_capture_failed request_id=%s", req.request_id)


def context_debug(req: Any, result: Any = None) -> dict[str, Any]:
    """Build a redacted Intent/ContextPlan/Routing decision snapshot."""
    plan: ContextPlan = req.context_plan
    intent = getattr(req, "intent", None)
    urgency = getattr(req, "urgency", None)
    entities = getattr(req, "entities", {}) or {}
    primary = getattr(result, "primary_agent", None) if result is not None else None
    agent_type = getattr(result, "agent_type", None) if result is not None else None
    supporting = getattr(result, "supporting_agents", []) if result is not None else []
    agent_types = getattr(result, "agent_types", []) if result is not None else []
    return {
        "request_id": req.request_id,
        "project_id": req.project_id,
        # Keep the original Context Planner fields for API compatibility.
        "planner_source": plan.planner_source,
        "plan": {"need_" + s: getattr(plan, "need_" + s) for s in SOURCES},
        "reasons": dict(plan.reasons),
        "sources": copy.deepcopy(req.context_bundle.sources),
        "intent": {
            "category": getattr(intent, "value", intent),
            "group": getattr(req, "intent_group", None),
            "urgency": getattr(urgency, "name", None),
            "confidence": round(float(getattr(req, "intent_confidence", 0.0)), 4),
            "source_scores": dict(getattr(req, "intent_source_scores", {}) or {}),
            # Entity types are useful for debugging; values may contain private data.
            "entity_counts": {
                name: len(values) if isinstance(values, list) else int(values is not None)
                for name, values in entities.items()
            },
        },
        "routing": {
            "primary_agent": getattr(primary or agent_type, "value", primary or agent_type),
            "supporting_agents": [getattr(item, "value", item) for item in supporting],
            "agent_types": [getattr(item, "value", item) for item in agent_types],
            "reason": getattr(result, "routing_reason", "") if result is not None else "",
            "confidence": round(float(getattr(result, "routing_confidence", 0.0)), 4)
            if result is not None else 0.0,
            "escalated": bool(getattr(result, "escalated", False)) if result is not None else False,
        },
        "execution": copy.deepcopy(getattr(req, "execution_records", [])),
    }


def log_context(req: Any, result: Any = None) -> None:
    logger.info("request_decision %s", json.dumps(context_debug(req, result), ensure_ascii=False))
