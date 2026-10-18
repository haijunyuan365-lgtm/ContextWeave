"""
亮点：利用 Monitor 监控 Agent 在线表现

核心问题：如何利用 Monitor 监控 Agent 的在线表现？

本模块的答案：
  1. 实时采集 —— 每隔 N 秒从 Orchestrator 和 ToolManager 拉取最新统计
  2. 异常检测 —— Z-score 统计方法，自动发现指标突变
  3. 路由反馈 —— 将 Agent 成功率/延迟写回 Orchestrator，
     Orchestrator 的 _best_agent() 会据此动态调整路由权重
  4. 优化建议 —— 基于规则生成可操作的优化建议（不是空话）
  5. 告警 —— 超阈值时打日志 + 可选 Webhook
"""
import asyncio
import logging
import os
import statistics
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Deque, Dict, List, Optional

import httpx
from prometheus_client import start_http_server, REGISTRY, Histogram
from observability.bootstrap import metrics_available
from observability.metrics_projection import get_metrics_projection, AgentMetricsSnapshot, ToolMetricsSnapshot

logger = logging.getLogger(__name__)


# ── 数据结构 ──────────────────────────────────────────────────────────────────

class Severity(Enum):
    INFO     = "info"
    WARNING  = "warning"
    ERROR    = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    severity:    Severity
    metric:      str
    message:     str
    value:       float
    threshold:   float
    ts:          str = field(default_factory=lambda: datetime.now().isoformat())
    resolved:    bool = False


@dataclass
class Suggestion:
    """可操作的优化建议。"""
    title:       str
    detail:      str
    action:      str    # 具体操作步骤
    priority:    int    # 1-10


# ── 异常检测 ──────────────────────────────────────────────────────────────────

class AnomalyDetector:
    """
    基于滑动窗口 Z-score 的异常检测。

    Z-score = |当前值 - 均值| / 标准差
    超过 sensitivity 倍标准差则判定为异常。
    """

    def __init__(self, window: int = 60, sensitivity: float = 2.5):
        self._window      = window
        self._sensitivity = sensitivity
        self._history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=window))

    def record(self, metric: str, value: float) -> Optional[Dict[str, Any]]:
        """记录一个数据点，如果异常则返回异常信息，否则返回 None。"""
        buf = self._history[metric]
        buf.append(value)

        if len(buf) < self._window // 2:
            return None  # 数据不足，不检测

        mean  = statistics.mean(buf)
        stdev = statistics.stdev(buf) if len(buf) > 1 else 0.0
        if stdev == 0:
            return None

        z = abs(value - mean) / stdev
        if z > self._sensitivity:
            return {
                "metric":   metric,
                "value":    value,
                "mean":     mean,
                "z_score":  round(z, 2),
                "severity": "high" if z > self._sensitivity * 1.5 else "medium",
            }
        return None


# ── 性能监控器 ────────────────────────────────────────────────────────────────

class PerformanceMonitor:
    """
    Agent 在线表现监控。

    与 Orchestrator 的联动：
      Monitor 采集 → 发现某 Agent 成功率下降 →
      Orchestrator.get_stats() 中该 Agent 的 routing_score 自动降低 →
      _best_agent() 路由时自动绕开该 Agent

    这就是"利用 Monitor 监控在线表现"的闭环。
    """

    # 告警阈值
    THRESHOLDS = {
        "agent_success_rate":  (0.90, Severity.ERROR,   "less_than"),
        "tool_success_rate":   (0.95, Severity.WARNING,  "less_than"),
        "agent_avg_ms":        (3000, Severity.WARNING,  "greater_than"),
        "tool_avg_ms":         (5000, Severity.ERROR,    "greater_than"),
    }

    def __init__(
        self,
        orchestrator,
        tool_manager,
        interval_s:       float = 10.0,
        webhook_url:      Optional[str] = None,
        prometheus_port:  Optional[int] = None,   # None = 不启动
        registry=None,
        metrics_projection=None,
        stats_source: Optional[str] = None,
    ):
        self._orchestrator = orchestrator
        self._tool_manager = tool_manager
        self._interval     = interval_s
        self._webhook      = webhook_url
        self._detector     = AnomalyDetector()

        self._alerts:      List[Alert]      = []
        self._suggestions: List[Suggestion] = []
        self._active       = False
        self._task:        Optional[asyncio.Task] = None

        # Prometheus 指标（可选）
        from observability.metrics_projection import MetricsProjection
        self._registry = registry if registry is not None else getattr(metrics_projection, "registry", REGISTRY)
        self._owned_metrics = registry is not None and metrics_projection is None
        before = set(self._registry._collector_to_names)
        self._projection = metrics_projection or (MetricsProjection(registry) if self._owned_metrics else get_metrics_projection())
        self._http_server = None
        self._prometheus_port = prometheus_port
        self._registered = True
        self._capture_available = metrics_available
        self._stats_source = stats_source or os.getenv("ECHOMIND_MONITOR_STATS_SOURCE", "span").lower()
        if self._stats_source not in {"legacy", "shadow", "span"}:
            self._stats_source = "shadow"
        # Registered independently of the optional HTTP listener; /metrics always works.
        self._prom = self._projection.compatibility
        self._prom = dict(self._prom)
        self._owned_collectors = set(self._registry._collector_to_names) - before if self._owned_metrics else set()
        if self._owned_metrics:
            for kind, agents in getattr(self._orchestrator, "_pool", {}).items():
                for i, agent in enumerate(agents):
                    agent.stats.observer = self._prom["agent_latency_ms"].labels(agent=f"{kind.value}_{i}").observe
        self._comparison = {}


    def _setup_prometheus(self, port: int) -> None:
        self._http_server, _ = start_http_server(port, registry=self._registry)
        logger.info(f"Prometheus 已启动: :{port}")

    # ── 生命周期 ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._active:
            return
        if not self._registered:
            for metric in self._owned_collectors:
                self._registry.register(metric)
            self._registered = True
        if self._prometheus_port:
            self._setup_prometheus(self._prometheus_port)
        self._active = True
        self._task   = asyncio.create_task(self._loop())
        logger.info(f"Monitor 已启动，采集间隔 {self._interval}s")

    async def stop(self) -> None:
        self._active = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._http_server is not None:
            await asyncio.to_thread(self._http_server.shutdown)
            self._http_server.server_close()
            self._http_server = None
        if self._owned_metrics and self._registered:
            for metric in self._owned_collectors:
                self._registry.unregister(metric)
            self._registered = False

    # ── 采集循环 ──────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        while self._active:
            try:
                await self._collect()
            except Exception as ex:
                logger.error(f"Monitor 采集异常: {ex}")
            await asyncio.sleep(self._interval)

    async def _collect(self) -> None:
        """
        采集 Agent 和工具的实时统计，检测异常，生成建议。

        关键：这里读取的 stats 就是 Orchestrator/ToolManager 在处理请求时
        实时更新的数据，Monitor 不需要额外埋点。
        """
        agent_stats, tool_stats = self._select_stats()
        routing_penalties: Dict[str, float] = {}

        # ── Agent 指标 ────────────────────────────────────────────────────────
        for agent_key, s in agent_stats.items():
            sr  = s["success_rate"]
            ms  = s["avg_ms"]

            # 异常检测
            for metric, value in [("agent_success_rate", sr), ("agent_avg_ms", ms)]:
                anomaly = self._detector.record(f"{metric}:{agent_key}", value)
                if anomaly:
                    logger.warning(f"异常检测 [{agent_key}] {metric}={value:.3f} z={anomaly['z_score']}")

            # 阈值告警
            self._check_threshold("agent_success_rate", sr, agent_key)
            self._check_threshold("agent_avg_ms", ms, agent_key)

            # Prometheus
            if "agent_success_rate" in self._prom:
                self._prom["agent_success_rate"].labels(agent=self._projection.label("agent", agent_key)).set(sr)
                self._prom["agent_avg_ms"].labels(agent=self._projection.label("agent", agent_key)).set(ms)

            routing_penalties[agent_key] = (self._routing_penalty(s["recent_success_rate"], s["recent_avg_ms"])
                                          if s.get("recent_total", 0) >= 5 else 0.0)

        # ── 工具指标 ──────────────────────────────────────────────────────────
        for tool_name, s in tool_stats.items():
            sr = s["success_rate"]
            ms = s["avg_latency_ms"]
            cf = s["consecutive_fails"]

            self._check_threshold("tool_success_rate", sr, tool_name)
            self._check_threshold("tool_avg_ms", ms, tool_name)

            if "tool_success_rate" in self._prom:
                self._prom["tool_success_rate"].labels(tool=self._projection.label("tool", tool_name)).set(sr)

            # 连续失败 → 生成具体建议
            if cf >= 3:
                self._add_suggestion(Suggestion(
                    title=f"工具 {tool_name} 连续失败 {cf} 次",
                    detail=f"成功率 {sr:.1%}，平均延迟 {ms:.0f}ms，熔断状态: {s['circuit_state']}",
                    action="1. 检查工具依赖服务是否正常\n2. 查看错误日志\n3. 考虑增加超时时间或降级策略",
                    priority=9,
                ))

        # ── 路由优化建议 ──────────────────────────────────────────────────────
        updater = getattr(self._orchestrator, "update_routing_penalties", None)
        if updater:
            updater(routing_penalties)
        self._generate_routing_suggestions(agent_stats)

    @staticmethod
    def _routing_penalty(success_rate: float, avg_ms: float) -> float:
        """把在线表现转成 0-0.9 的路由降权系数。"""
        penalty = 0.0
        if success_rate < 0.90:
            penalty += min(0.5, (0.90 - success_rate) * 2)
        if avg_ms > 3000:
            penalty += min(0.4, (avg_ms - 3000) / 10000)
        return min(penalty, 0.9)

    def _check_threshold(self, metric: str, value: float, label: str) -> None:
        if metric not in self.THRESHOLDS:
            return
        threshold, severity, operator = self.THRESHOLDS[metric]
        triggered = (operator == "less_than" and value < threshold) or \
                    (operator == "greater_than" and value > threshold)
        if triggered:
            alert = Alert(
                severity=severity,
                metric=f"{metric}:{label}",
                message=f"{label} 的 {metric} = {value:.3f}，阈值 {threshold}",
                value=value,
                threshold=threshold,
            )
            self._alerts.append(alert)
            logger.warning(f"[{severity.value.upper()}] {alert.message}")
            # 异步发送 Webhook（不阻塞采集循环）
            if self._webhook:
                asyncio.create_task(self._send_webhook(alert))

    def _generate_routing_suggestions(self, agent_stats: Dict[str, Any]) -> None:
        """
        基于 Agent 在线表现生成路由优化建议。
        这是 Monitor → Orchestrator 反馈闭环的体现。
        """
        for agent_key, s in agent_stats.items():
            if s["success_rate"] < 0.85 and s["total"] > 10:
                self._add_suggestion(Suggestion(
                    title=f"Agent {agent_key} 成功率偏低",
                    detail=f"成功率 {s['success_rate']:.1%}，路由评分 {s['routing_score']:.3f}",
                    action=(
                        "Orchestrator 的 _best_agent() 已自动降低该 Agent 的路由权重。\n"
                        "建议：1. 检查 system_prompt 是否需要优化\n"
                        "      2. 检查该类型问题的复杂度是否超出 Agent 能力\n"
                        "      3. 考虑增加同类型 Agent 实例"
                    ),
                    priority=8,
                ))

    def _add_suggestion(self, s: Suggestion) -> None:
        # 去重：相同 title 不重复添加
        if not any(x.title == s.title for x in self._suggestions):
            self._suggestions.append(s)
            logger.info(f"优化建议 [P{s.priority}]: {s.title}")

    async def _send_webhook(self, alert: Alert) -> None:
        try:
            async with httpx.AsyncClient(timeout=5.0) as c:
                await c.post(self._webhook, json=asdict(alert))  # type: ignore
        except Exception as ex:
            logger.error(f"Webhook 发送失败: {ex}")

    # ── 查询接口 ──────────────────────────────────────────────────────────────

    def _select_stats(self):
        """Dual-read with identical legacy success/timing definitions; fall back per row."""
        legacy_agents = self._orchestrator.get_stats()
        legacy_tools = self._tool_manager.get_stats()
        projected_agents, projected_tools = self._projection.snapshots(
            getattr(self._orchestrator, "_metrics_owner", None), getattr(self._tool_manager, "_metrics_owner", None))
        available = self._capture_available()
        result, comparison = [], {}
        for component, legacy, projected, empty in (
            ("agent", legacy_agents, projected_agents, AgentMetricsSnapshot().legacy_view()),
            ("tool", legacy_tools, projected_tools, ToolMetricsSnapshot().legacy_view()),
        ):
            selected, report = {}, {}
            for key, row in legacy.items():
                sample = projected.get(key)
                if sample is None and row.get("total") == 0:
                    sample = empty
                matches = sample is not None and all(row.get(field) == sample.get(field) for field in empty)
                use_span = available and matches and self._stats_source == "span"
                selected[key] = {**row, **sample} if use_span else row
                report[key] = {"match": matches, "source": "span" if use_span else "legacy", "span": sample}
            comparison[component] = report
            self._projection.reconciliation.labels(component).set(sum(not r["match"] for r in report.values()))
            result.append(selected)
        self._comparison = {"mode": self._stats_source, "capture_available": available, **comparison}
        return result[0], result[1]

    def summary(self) -> Dict[str, Any]:
        """返回当前监控摘要，供 API 层暴露。"""
        agents, tools = self._select_stats()
        return {
            "agent_stats": agents,
            "tool_stats": tools,
            "stats_reconciliation": self._comparison,
            "active_alerts": [asdict(a) for a in self._alerts if not a.resolved][-10:],
            "suggestions":   [
                {"title": s.title, "action": s.action, "priority": s.priority}
                for s in sorted(self._suggestions, key=lambda x: -x.priority)[:5]
            ],
        }
