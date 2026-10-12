"""
亮点：MCP 工具调用框架

核心问题：工具调用出错（检索不全、召回不好）怎么优化？

本模块的答案：
  1. 查询改写（Query Rewriting）—— 用 LLM 把用户原始问题扩写成多个角度的子查询，
     再合并去重，解决"召回不全"问题。
  2. 结果重排（Reranking）—— 对召回结果用 LLM 打分，按相关性重新排序，
     解决"召回不好/排序差"问题。
  3. 熔断器（Circuit Breaker）—— 连续失败超阈值时自动断开，防止雪崩。
  4. 结果缓存（TTL Cache）—— 相同参数直接返回缓存，减少重复调用。
  5. 降级策略（Fallback）—— 工具不可用时返回有意义的降级结果。
"""
import asyncio
import hashlib
import inspect
import json
import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from anthropic import AsyncAnthropic

from core.llm_utils import extract_text_content
from mcp.rag_config import RAGConfig
from observability.llm import generate
from observability import observed, set_outcome
from observability.instrumentation import instrumented, annotate, outcome
from observability.rag_snapshot import candidates, ranked
from observability.sanitization import error_code

logger = logging.getLogger(__name__)


# ── 数据结构 ──────────────────────────────────────────────────────────────────

class CircuitState(Enum):
    CLOSED    = "closed"     # 正常
    OPEN      = "open"       # 熔断，拒绝请求
    HALF_OPEN = "half_open"  # 探测恢复


@dataclass
class ToolResult:
    success:        bool
    data:           Any
    tool_name:      str
    error:          Optional[str] = None
    cached:         bool = False
    latency_ms:     float = 0.0
    reranked:       bool = False   # 是否经过重排


@dataclass
class ToolStats:
    """工具运行时统计，供 Monitor 读取。"""
    total:              int = 0
    success:            int = 0
    failed:             int = 0
    total_latency_ms:   float = 0.0
    consecutive_fails:  int = 0

    @property
    def success_rate(self) -> float:
        return self.success / self.total if self.total else 1.0

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.total if self.total else 0.0


# ── 熔断器 ────────────────────────────────────────────────────────────────────

class CircuitBreaker:
    """
    三态熔断器：CLOSED → OPEN → HALF_OPEN → CLOSED

    连续失败 failure_threshold 次后打开；
    打开 recovery_s 秒后进入 HALF_OPEN 探测；
    探测成功则关闭，失败则重新打开。
    """

    def __init__(self, failure_threshold: int = 5, recovery_s: float = 60.0):
        self.threshold   = failure_threshold
        self.recovery_s  = recovery_s
        self.state       = CircuitState.CLOSED
        self.fail_count  = 0
        self.opened_at:  Optional[float] = None

    def allow(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.monotonic() - self.opened_at >= self.recovery_s:  # type: ignore
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        return True  # HALF_OPEN：放行一次探测

    def record_success(self) -> None:
        self.fail_count = 0
        self.state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self.fail_count += 1
        if self.fail_count >= self.threshold:
            self.state     = CircuitState.OPEN
            self.opened_at = time.monotonic()
            logger.warning(f"熔断器打开（连续失败 {self.fail_count} 次）")


# ── 工具定义 ──────────────────────────────────────────────────────────────────

@dataclass
class Tool:
    name:        str
    description: str
    handler:     Callable                    # async (params, context) -> Any
    schema:      Dict[str, Any]              # JSON Schema
    cache_ttl:   float = 0.0                 # 0 = 不缓存
    timeout_s:   float = 30.0
    supports_rerank: bool = False            # 是否支持结果重排
    fallback:    Optional[Callable] = None    # sync/async (params, context, error) -> Any
    embedding_provider: Optional[Callable] = None  # async (chunk_ids) -> {chunk_id: vector}

    # 运行时状态（不参与构造）
    stats:   ToolStats    = field(default_factory=ToolStats, init=False)
    breaker: CircuitBreaker = field(default_factory=CircuitBreaker, init=False)


# ── MCP 工具管理器 ────────────────────────────────────────────────────────────

class MCPToolManager:
    """
    MCP 工具调用框架。

    核心优化链路（针对检索类工具）：
      用户查询 → 查询改写 → 混合召回 → RRF → chunk ID 去重 → MMR → 重排 → Top-K
    """

    def __init__(self, api_key: str, base_url: Optional[str] = None,
                 model: str = "claude-3-5-sonnet-20241022", rag_config: Optional[RAGConfig] = None):
        kwargs: Dict[str, Any] = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
        self._client = AsyncAnthropic(**kwargs)
        self._model  = model
        self._metrics_owner = str(uuid.uuid4())
        self.rag_config = rag_config or RAGConfig.from_env()
        self._tools: Dict[str, Tool] = {}
        self._cache: Dict[str, tuple] = {}   # key → (result, expire_at, reranked)

    # ── 注册 / 注销 ───────────────────────────────────────────────────────────

    def register(self, tool: Tool) -> None:
        self._tools[tool.name] = tool
        logger.info(f"注册工具: {tool.name}")

    def unregister(self, name: str) -> None:
        self._tools.pop(name, None)

    def clear_cache(self, name: Optional[str] = None) -> None:
        """Invalidate cached tool output after a backing data source changes."""
        if name is None:
            self._cache.clear()
            return
        prefix = name + ":"
        for key in [key for key in self._cache if key.startswith(prefix)]:
            del self._cache[key]

    # ── 核心调用 ──────────────────────────────────────────────────────────────

    @instrumented("tool.execute", span_type="tool")
    async def call(
        self,
        name: str,
        params: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
        *,
        use_cache: bool = True,
        rerank_top_k: int = 0,          # >0 时对结果重排，取 Top-K
    ) -> ToolResult:
        """
        调用工具，完整执行链：
          缓存检查 → 熔断检查 → 参数校验 → 执行（含超时）→ 可选重排 → 缓存写入
        """
        annotate(**{"app.tool.name": name, "app.tool.layer": "mcp", "app.cache_hit": False})
        tool = self._tools.get(name)
        annotate(**{"app.tool.monitor_owner": getattr(self, "_metrics_owner", None), "app.tool.stats_registered": tool is not None,
                    "app.tool.stats_total": 0, "app.tool.stats_success": 0, "app.tool.stats_failed": 0, "app.tool.stats_latency_ms": 0.0})
        if not tool:
            annotate(**{"app.tool.success": False, "app.tool.result_success": False})
            outcome("error", code="TOOL_NOT_FOUND")
            return ToolResult(success=False, data=None, tool_name=name, error=f"工具不存在: {name}")

        cache_rerank_top_k = rerank_top_k if rerank_top_k > 0 and tool.supports_rerank else 0

        # 缓存命中
        if use_cache and tool.cache_ttl > 0:
            cached = self._get_cache(name, params, cache_rerank_top_k)
            if cached is not None:
                cached_data, cached_reranked = cached
                annotate(**{"app.cache_hit": True, "app.tool.success": True, "app.tool.result_success": not isinstance(cached_data, dict) or cached_data.get("success") is not False})
                if isinstance(cached_data, dict) and cached_data.get("success") is False:
                    outcome("error", code="TOOL_BUSINESS_FAILED")
                tool.stats.total += 1
                tool.stats.success += 1
                annotate(**{"app.tool.stats_total": 1, "app.tool.stats_success": 1})
                self._observe_tool_state(tool)
                return ToolResult(
                    success=True,
                    data=cached_data,
                    tool_name=name,
                    cached=True,
                    reranked=cached_reranked,
                )

        annotate(**{"app.tool.circuit_state": tool.breaker.state.value})
        # 熔断检查
        if not tool.breaker.allow():
            self._observe_tool_state(tool)
            annotate(**{"app.tool.circuit_open": True, "app.error_code": "CIRCUIT_OPEN"})
            error = f"工具熔断中: {name}，请稍后重试"
            return await self._fallback_result(tool, params, context, error)

        t0 = time.monotonic()
        tool.stats.total += 1
        annotate(**{"app.tool.stats_total": 1})
        try:
            # 参数校验（根据 JSON Schema 的 required 和 properties.type）
            self._validate_params(tool, params)

            data = await asyncio.wait_for(self._run_handler(tool, params, context), timeout=tool.timeout_s)
            latency = (time.monotonic() - t0) * 1000
            business_success = not isinstance(data, dict) or data.get("success") is not False
            annotate(**{"app.tool.success": True, "app.tool.result_success": business_success})
            if not business_success:
                outcome("error", code="TOOL_BUSINESS_FAILED")

            tool.stats.success += 1
            tool.stats.consecutive_fails = 0
            tool.stats.total_latency_ms += latency
            tool.breaker.record_success()
            annotate(**{"app.tool.stats_success": 1, "app.tool.stats_latency_ms": latency})
            self._observe_tool_state(tool)

            # 重排（针对返回列表的检索工具）
            reranked = False
            if rerank_top_k > 0 and tool.supports_rerank and isinstance(data, list):
                query = params.get("query", "")
                data, reranked = await self._rerank(query, data, rerank_top_k), True

            # 写缓存：缓存最终返回结果，避免下次命中未重排的原始结果。
            if tool.cache_ttl > 0:
                self._set_cache(name, params, data, tool.cache_ttl, cache_rerank_top_k, reranked)

            return ToolResult(success=True, data=data, tool_name=name,
                              latency_ms=latency, reranked=reranked)

        except asyncio.TimeoutError:
            annotate(**{"app.error_code": "TIMEOUT"})
            tool.stats.failed += 1
            tool.stats.consecutive_fails += 1
            tool.breaker.record_failure()
            annotate(**{"app.tool.stats_failed": 1})
            self._observe_tool_state(tool)
            logger.error(f"工具超时: {name} ({tool.timeout_s}s)")
            return await self._fallback_result(tool, params, context, "执行超时")

        except Exception as ex:
            annotate(**{"app.error_code": error_code(ex)})
            tool.stats.failed += 1
            tool.stats.consecutive_fails += 1
            tool.breaker.record_failure()
            annotate(**{"app.tool.stats_failed": 1})
            self._observe_tool_state(tool)
            logger.error(f"工具异常: {name} — {ex}")
            return await self._fallback_result(tool, params, context, str(ex))

    @staticmethod
    def _observe_tool_state(tool):
        # State changes precede potentially slow fallbacks; order by mutation, not span end.
        annotate(**{"app.tool.stats_version": time.monotonic_ns(),
                    "app.tool.stats_consecutive_fails": tool.stats.consecutive_fails,
                    "app.tool.stats_circuit_state": tool.breaker.state.value})

    async def _fallback_result(
        self,
        tool: Tool,
        params: Dict[str, Any],
        context: Optional[Dict[str, Any]],
        error: str,
    ) -> ToolResult:
        """工具不可用时返回降级结果，而不是把空错误直接暴露给调用方。"""
        annotate(**{"app.tool.success": False})
        if tool.fallback is None:
            annotate(**{"app.tool.result_success": False})
            outcome("error")
            return ToolResult(success=False, data=None, tool_name=tool.name, error=error)
        try:
            data = tool.fallback(params, context, error)
            if asyncio.iscoroutine(data):
                data = await data
            business_success = not isinstance(data, dict) or data.get("success") is not False
            annotate(**{"app.tool.result_success": business_success})
            outcome("degraded" if business_success else "error", fallback=True)
            return ToolResult(
                success=True,
                data=data,
                tool_name=tool.name,
                error=error,
            )
        except Exception as ex:
            annotate(**{"app.tool.result_success": False})
            outcome("error", fallback=True, code="TOOL_FALLBACK_FAILED")
            logger.error(f"工具降级失败: {tool.name} — {ex}")
            return ToolResult(success=False, data=None, tool_name=tool.name, error=f"{error}; fallback失败: {ex}")

    async def _run_handler(
        self,
        tool: Tool,
        params: Dict[str, Any],
        context: Optional[Dict[str, Any]],
    ) -> Any:
        """
        执行工具 handler。

        优先支持 async handler；如果历史工具仍是同步函数，则放入线程池执行，
        避免阻塞事件循环。
        """
        if inspect.iscoroutinefunction(tool.handler):
            return await tool.handler(params, context)
        result = await asyncio.to_thread(tool.handler, params, context)
        if inspect.isawaitable(result):
            return await result
        return result

    # ── 查询改写（解决召回不全）────────────────────────────────────────────────

    @instrumented("rag.rewrite", span_type="retrieval", result_count="app.rag.output_count", propagate_degradation=True)
    async def rewrite_query(self, query: str, n: int = 3) -> List[str]:
        """
        用 LLM 将原始查询改写为 n 个不同角度的子查询。

        目的：单一查询往往只能召回某一角度的文档，
        多角度子查询并行检索后合并，显著提升召回率。

        示例：
          原始: "退款流程"
          改写: ["如何申请退款", "退款需要多少天", "退款政策是什么"]
        """
        prompt = f"""将以下用户查询改写为 {n} 个不同角度的搜索子查询，用于检索知识库。
要求：每个子查询角度不同，覆盖原始问题的不同方面。
原始查询: "{query}"
返回 JSON 数组，例如: ["子查询1", "子查询2", "子查询3"]"""
        prompt = self._clean_text(prompt)
        try:
            resp = await generate(self._client, stage="rag.rewrite",
                model=self._model, max_tokens=256, temperature=0.3,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = extract_text_content(resp.content)
            s, e = raw.find("["), raw.rfind("]") + 1
            queries = json.loads(raw[s:e])
            if not isinstance(queries, list):
                raise ValueError("查询改写结果不是数组")
            cleaned = [str(item).strip() for item in queries if isinstance(item, str) and item.strip()]
            # 原始查询始终排在第一位，改写数量受 n 限制。
            return list(dict.fromkeys([query.strip()] + cleaned))[:n + 1]
        except Exception as ex:
            outcome("degraded", fallback=True, code="TIMEOUT" if error_code(ex) == "TIMEOUT" else "REWRITE_FAILED")
            logger.warning(f"查询改写失败，使用原始查询: {ex}")
            return [query]

    @instrumented("rag.pipeline", span_type="retrieval")
    async def search_with_rewrite(
        self,
        tool_name: str,
        query: str,
        top_k: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> ToolResult:
        """
        完整的检索优化链路：查询改写 → 混合召回 → RRF → 去重 → MMR → 重排 → Top-K

        这是解决"检索不全、召回不好"的完整方案。
        """
        config = getattr(self, "rag_config", RAGConfig())
        top_k = config.final_top_k if top_k is None else top_k
        if type(top_k) is not int or not 1 <= top_k <= 20:
            raise ValueError("top_k must be between 1 and 20")
        timings: Dict[str, float] = {}

        # 1. 查询改写：生成多角度子查询
        started = time.monotonic()
        sub_queries = await self.rewrite_query(query, n=config.rewrite_count)
        timings["rewrite_ms"] = (time.monotonic() - started) * 1000
        annotate(**{"app.rag.subquery_count": len(sub_queries)})
        logger.info("rag_rewrite subquery_count=%d", len(sub_queries))

        # 2. 并行召回：所有子查询同时检索
        started = time.monotonic()
        recall_k = max(top_k, config.recall_k)
        async def retrieve_query(index, value):
            with observed("rag.query", span_type="retrieval", **{"app.rag.query_index": index}) as query_span:
                result = await self.call(tool_name, {"query": value, "top_k": recall_k}, context, use_cache=True)
                query_span.set_attribute("app.rag.output_count", len(result.data) if isinstance(result.data, list) else 0)
                query_span.set_attribute("app.cache_hit", result.cached)
                if not result.success:
                    set_outcome(query_span, "error", code="RETRIEVAL_FAILED")
                elif result.error:
                    set_outcome(query_span, "degraded", fallback=True, code="RETRIEVAL_DEGRADED")
                return result
        tasks = [retrieve_query(index, value) for index, value in enumerate(sub_queries)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        timings["retrieval_ms"] = (time.monotonic() - started) * 1000

        # 3. 按子查询排名做第二级加权 RRF，并按稳定 chunk_id 精确去重。
        started = time.monotonic()
        with observed("rag.rrf.stage2", span_type="retrieval", **{"app.rag.stage": "rrf_stage2"}) as fusion_span:
            fused: Dict[str, Dict[str, Any]] = {}
            fused_scores: Dict[str, float] = {}
            fused_sources: Dict[str, set[str]] = {}
            degraded = False
            successful_lists = 0
            for query_index, r in enumerate(results):
                if not isinstance(r, ToolResult) or not r.success or r.error:
                    degraded = True
                if isinstance(r, ToolResult) and r.success and isinstance(r.data, list):
                    usable = [item for item in r.data if isinstance(item, dict) and not item.get("fallback")]
                    candidates(usable, "input", source=f"subquery_{query_index}")
                    if usable:
                        successful_lists += 1
                    weight = 1.0 if query_index == 0 else config.rewritten_query_weight
                    for rank, item in enumerate(usable, 1):
                        if isinstance(item, dict) and item.get("fallback"):
                            degraded = True
                            continue
                        chunk_id = self._chunk_identity(item)
                        if chunk_id not in fused:
                            fused[chunk_id] = dict(item, chunk_id=chunk_id)
                            fused_scores[chunk_id] = 0.0
                            fused_sources[chunk_id] = set()
                        fused_scores[chunk_id] += weight / (config.rrf_k + rank)
                        fused_sources[chunk_id].update(str(s) for s in item.get("retrieval_sources", []))

            for chunk_id, item in fused.items():
                item["score"] = round(fused_scores[chunk_id], 8)
                item["retrieval_sources"] = sorted(fused_sources[chunk_id])
            merged = sorted(fused.values(), key=lambda item: (-float(item["score"]), str(item["chunk_id"])))
            candidates(merged)
            timings["rrf_ms"] = (time.monotonic() - started) * 1000

            fusion_span.set_attribute("app.rag.input_list_count", len(results))
            fusion_span.set_attribute("app.rag.input_count", sum(len(r.data) for r in results if isinstance(r, ToolResult) and isinstance(r.data, list)))
            fusion_span.set_attribute("app.rag.output_count", len(merged))
        if degraded:
            outcome("degraded", fallback=True, code="RETRIEVAL_FAILED")
        annotate(**{"app.rag.output_count": len(merged)})
        if not merged:
            if degraded:
                outcome("error", code="RETRIEVAL_FAILED")
            return ToolResult(success=not degraded, data=[], tool_name=tool_name,
                              error="检索来源失败或降级" if degraded else None)

        # 4. 对有限候选做 MMR，再用 LLM 重排到最终 Top-K。
        tool = getattr(self, "_tools", {}).get(tool_name)
        rerank_limit = max(top_k, config.rerank_candidates)
        mmr_pool = merged[:max(rerank_limit, config.mmr_candidates)]
        started = time.monotonic()
        diverse = await self._mmr_select(tool, mmr_pool, rerank_limit, config.mmr_lambda)
        timings["mmr_ms"] = (time.monotonic() - started) * 1000
        started = time.monotonic()
        reranked = await self._rerank(query, diverse, top_k)
        timings["rerank_ms"] = (time.monotonic() - started) * 1000
        logger.info(
            "rag_pipeline sub_queries=%d successful_lists=%d fused_count=%d mmr_count=%d final_count=%d "
            "rewrite_ms=%.2f retrieval_ms=%.2f rrf_ms=%.2f mmr_ms=%.2f rerank_ms=%.2f degraded=%s",
            len(sub_queries), successful_lists, len(merged), len(diverse), len(reranked),
            timings["rewrite_ms"], timings["retrieval_ms"], timings["rrf_ms"],
            timings["mmr_ms"], timings["rerank_ms"], degraded,
        )
        annotate(**{"app.rag.output_count": len(reranked)})
        return ToolResult(success=True, data=reranked, tool_name=tool_name,
                          reranked=len(diverse) > top_k,
                          error="部分检索来源失败或降级" if degraded else None)

    @staticmethod
    def _chunk_identity(item: Dict[str, Any]) -> str:
        """Use the persisted ID; synthesize only for legacy/custom handlers."""
        chunk_id = str(item.get("chunk_id") or "").strip()
        if chunk_id:
            return chunk_id
        stable = json.dumps({
            "title": item.get("title", ""), "content": item.get("content", ""),
            "chunk": item.get("chunk", 0),
        }, ensure_ascii=False, sort_keys=True)
        return "legacy:" + hashlib.sha256(stable.encode()).hexdigest()

    @instrumented("rag.mmr", span_type="retrieval", result_count="app.rag.output_count", propagate_degradation=True)
    @ranked
    async def _mmr_select(self, tool: Optional[Tool], items: List[Dict[str, Any]],
                          limit: int, relevance_weight: float) -> List[Dict[str, Any]]:
        """Select a relevant but non-redundant pool using stored Chroma embeddings."""
        annotate(**{"app.rag.stage": "mmr", "app.rag.input_count": len(items)})
        limit = min(limit, len(items))
        if limit <= 0:
            return []
        provider = tool.embedding_provider if tool is not None else None
        if provider is None or len(items) <= 1:
            outcome("skipped")
            return items[:limit]
        try:
            vectors = provider([str(item["chunk_id"]) for item in items])
            if inspect.isawaitable(vectors):
                vectors = await vectors
            if not isinstance(vectors, dict) or any(str(item["chunk_id"]) not in vectors for item in items):
                raise ValueError("候选 embedding 不完整")
        except Exception as ex:
            outcome("degraded", fallback=True, code="TIMEOUT" if error_code(ex) == "TIMEOUT" else "MMR_EMBEDDING_FAILED")
            logger.warning("MMR embedding 获取失败，保留 RRF 排序: %s", ex)
            return items[:limit]

        scores = [float(item.get("score", 0.0)) for item in items]
        low, high = min(scores), max(scores)
        relevance = [1.0 if high == low else (score - low) / (high - low) for score in scores]
        selected: List[int] = []
        remaining = list(range(len(items)))
        while remaining and len(selected) < limit:
            best_index, best_score = remaining[0], -math.inf
            for index in remaining:
                vector = vectors[str(items[index]["chunk_id"])]
                redundancy = max((max(0.0, self._cosine(vector, vectors[str(items[i]["chunk_id"])]))
                                  for i in selected), default=0.0)
                score = relevance_weight * relevance[index] - (1.0 - relevance_weight) * redundancy
                if score > best_score:
                    best_index, best_score = index, score
            selected.append(best_index)
            remaining.remove(best_index)
        return [items[index] for index in selected]

    @staticmethod
    def _cosine(left: List[float], right: List[float]) -> float:
        if not left or len(left) != len(right):
            return 0.0
        dot = sum(a * b for a, b in zip(left, right))
        left_norm = math.sqrt(sum(value * value for value in left))
        right_norm = math.sqrt(sum(value * value for value in right))
        return dot / (left_norm * right_norm) if left_norm and right_norm else 0.0

    # ── 结果重排（解决召回不好）──────────────────────────────────────────────

    @instrumented("rag.rerank", span_type="retrieval", result_count="app.rag.output_count", propagate_degradation=True)
    @ranked
    async def _rerank(self, query: str, items: List[Any], top_k: int) -> List[Any]:
        """
        用 LLM 对召回结果重新打分排序。

        解决问题：向量检索的相似度分数不等于"对用户有用"，
        LLM 能理解语义相关性，重排后 Top-K 质量显著提升。
        """
        annotate(**{"app.rag.stage": "rerank", "app.rag.input_count": len(items), "app.rag.llm_used": len(items) > top_k})
        if len(items) <= top_k:
            outcome("skipped")
            return items

        # 将结果序列化为文本供 LLM 评分
        items_text = "\n".join(f"{i}. {json.dumps(item, ensure_ascii=False)[:200]}"
                               for i, item in enumerate(items))
        prompt = f"""根据用户查询，对以下检索结果按相关性打分（0-10），返回 JSON 数组。
用户查询: "{query}"
检索结果:
{items_text}

返回格式（按相关性降序排列的索引列表）: [最相关的索引, ..., 最不相关的索引]
只返回 JSON 数组，不要其他文字。"""
        prompt = self._clean_text(prompt)

        try:
            resp = await generate(self._client, stage="rag.rerank",
                model=self._model, max_tokens=256, temperature=0.0,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = extract_text_content(resp.content)
            s, e = raw.find("["), raw.rfind("]") + 1
            order = json.loads(raw[s:e])
            if not isinstance(order, list):
                raise ValueError("重排结果不是数组")
            valid: List[int] = []
            seen = set()
            for index in order:
                if type(index) is int and 0 <= index < len(items) and index not in seen:
                    valid.append(index)
                    seen.add(index)
            valid.extend(index for index in range(len(items)) if index not in seen)
            return [items[index] for index in valid[:top_k]]
        except Exception as ex:
            outcome("degraded", fallback=True, code="TIMEOUT" if error_code(ex) == "TIMEOUT" else "RERANK_FAILED")
            logger.warning(f"重排失败，返回原始顺序: {ex}")
            return items[:top_k]

    # ── 缓存 ──────────────────────────────────────────────────────────────────

    def _cache_key(self, name: str, params: Dict, rerank_top_k: int = 0) -> str:
        payload = {"params": params, "rerank_top_k": rerank_top_k}
        return f"{name}:{hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()}"

    def _get_cache(self, name: str, params: Dict, rerank_top_k: int = 0) -> Optional[Tuple[Any, bool]]:
        key = self._cache_key(name, params, rerank_top_k)
        if key in self._cache:
            data, expire_at, reranked = self._cache[key]
            if time.monotonic() < expire_at:
                return data, reranked
            del self._cache[key]
        return None

    def _set_cache(
        self,
        name: str,
        params: Dict,
        data: Any,
        ttl: float,
        rerank_top_k: int = 0,
        reranked: bool = False,
    ) -> None:
        if len(self._cache) >= 5000:
            # 清掉最旧的 1/4
            for k in list(self._cache)[:1250]:
                del self._cache[k]
        self._cache[self._cache_key(name, params, rerank_top_k)] = (data, time.monotonic() + ttl, reranked)

    # ── 参数校验 ──────────────────────────────────────────────────────────────

    _TYPE_MAP = {"string": str, "number": (int, float), "integer": int, "boolean": bool, "array": list, "object": dict}

    def _validate_params(self, tool: Tool, params: Dict[str, Any]) -> None:
        """根据工具的 JSON Schema 校验参数，不合法时抛出 ValueError。"""
        schema = tool.schema
        required = schema.get("required", [])
        properties = schema.get("properties", {})

        for field in required:
            if field not in params:
                raise ValueError(f"工具 {tool.name} 缺少必需参数: {field}")

        for key, value in params.items():
            if key in properties:
                expected_type = properties[key].get("type")
                if expected_type and expected_type in self._TYPE_MAP:
                    if not isinstance(value, self._TYPE_MAP[expected_type]):
                        raise ValueError(
                            f"工具 {tool.name} 参数 {key} 类型错误: 期望 {expected_type}，实际 {type(value).__name__}"
                        )

    @staticmethod
    def _clean_text(value: Any) -> str:
        """移除 Unicode 代理字符，避免 LLM 请求编码失败。"""
        if value is None:
            return ""
        if not isinstance(value, str):
            value = str(value)
        return value.encode("utf-8", errors="ignore").decode("utf-8")

    # ── 统计 ──────────────────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        return {
            name: {
                "total": t.stats.total,
                "success_rate": round(t.stats.success_rate, 3),
                "avg_latency_ms": round(t.stats.avg_latency_ms, 1),
                "consecutive_fails": t.stats.consecutive_fails,
                "circuit_state": t.breaker.state.value,
            }
            for name, t in self._tools.items()
        }
