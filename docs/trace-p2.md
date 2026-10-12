# Trace 第三阶段（P2）

P2 将请求执行的 Span 作为统一事实源，在 Span 完成时生成 Prometheus 指标，并接入 Monitor 的统计输入。保留原接口、旧统计和路由反馈算法，采用双读对账逐步迁移。

## 完成范围

- observability/metrics_projection.py：线程安全、无网络 IO 的 SpanProcessor，投影请求、Agent、Tool、LLM、RAG、Context 指标。
- 默认注册指标，不依赖 PROMETHEUS_PORT；现有 GET /metrics 可直接抓取。额外端口仍可按原配置开启，复用同一套指标，不重复注册。
- 每个 chat.pipeline 完成时请求计数一次；每个 agent.execute 完成时向 agent_latency_ms 观察一次真实 Span 耗时。Monitor 的周期轮询只更新 Gauge，不再将累计平均值写入 Histogram。
- 旧 agent_success_rate / tool_success_rate / requests_total 名称保留，新增 agent_avg_ms Gauge 和 contextweave_* 指标。直方图使用毫秒桶，覆盖 1ms 到 60s。
- Monitor 默认尝试采用 Span 汇总视图；每个实例先对账，不一致、请求尚未完成或采集关闭时，使用旧统计。异常检测、告警、建议、路由降权和旧 /monitor 字段保留。
- Agent 以实际实例索引区分，MCP 以管理器区分；内部 owner ID 不进入 Prometheus 标签。缓存、失败、熔断和异步降级保留旧统计定义。
- OTLP 完成真实本机 HTTP/Protobuf 发送验证，包含成功接收和 503 故障。提供可重复运行的 scripts/otlp-smoke.py。
- 提供可选 Collector Compose 覆盖文件和 Langfuse 转发模板；没有改变默认部署或发送到真实 Langfuse 账户。

## 指标来源

| 来源 Span | 主要指标 | 标签 |
|---|---|---|
| chat.pipeline | requests_total、contextweave_requests_total、contextweave_request_duration_ms | outcome（旧 requests_total 无标签） |
| agent.execute | contextweave_agent_calls_total、agent_latency_ms | agent、outcome |
| tool.execute | contextweave_tool_calls_total、tool_duration_ms、tool_results_total、tool_cache_hits_total、tool_fallbacks_total | tool、layer、outcome、结果维度 |
| llm.generate | contextweave_llm_calls_total、llm_duration_ms、llm_tokens_total、llm_usage_known_total | provider、model、stage、type |
| rag.* | contextweave_rag_calls_total、rag_duration_ms、rag_candidates、rag_empty_results_total | stage、direction、outcome |
| context.source.retrieve | contextweave_context_calls_total、context_duration_ms | source、outcome |

表中的缩写指标均带 contextweave_ 前缀。Counter 抓取名以 _total 结尾，Histogram 暴露 _bucket / _count / _sum。

requests_total 统计已创建 chat.pipeline 的业务请求完成量，包括成功和业务/执行失败；入口校验、服务未就绪等在 pipeline 创建前结束的 HTTP 请求不计入这个兼容计数。一次正常 HTTP 请求不会因为同时存在 HTTP Server Span 而重复计数。请求直方图覆盖 pipeline 内的记忆、规划、检索、Agent、清理和回写；HTTP Server Span 仍可用于 Trace 中查看响应发送时间。

LLM Token 只汇总返回 Usage 中的已知值；已知 0 会产生 usage_known 样本，缺失值没有样本。input / output / cache_read / cache_write 分开存储，没有估算账单费用。Tool 的 agent / mcp 两层分别计数，分析时应选择层级，避免把同一业务的嵌套层级简单相加。

Prometheus 标签不使用 request_id、trace_id、user_id、conv_id 或 Query。动态标签长度最多 96，单维度最多 64 个值，超限归入 other；无效格式归入 unknown。兼容快照最多保留 64 个组件 owner，每个 owner 最多 64 条实例/工具记录；超限或淘汰造成对账不一致时保留旧 Monitor 输入。

## 新旧对账与迁移

```env
ECHOMIND_MONITOR_STATS_SOURCE=span
```

- span：默认值。逐行比较完成量、旧定义的成功率、平均耗时，以及工具的连续失败/熔断状态。一致时使用 Span 汇总，保留路由分数、角色等原元数据；不一致时回退到旧行。
- shadow：继续用旧输入，同时对账，适合观察迁移。
- legacy：继续用旧输入，同时保留指标与对账信息。

GET /monitor 新增 stats_reconciliation，包含模式、采集是否可用、每行 match、实际使用的 source 和 Span 汇总。contextweave_stats_reconciliation_mismatches 按 agent / tool 显示最新不一致行数。

对账采用原有成功和计时定义：AgentStats 成功由 Agent.handle 的返回行为决定；MCP 的缓存计入旧调用量，熔断拒绝不增加旧 total，失败耗时仍按原逻辑处理。兼容快照携带这些固定数字事件，Histogram 则始终使用完整单次 Span 的真实时间，两者不混用。请求进行中，旧 total 已增加而完成事件尚未到达，短暂不一致属于正常情况；请求结束后重新对账。

异步 fallback 的状态可能在另一个成功调用之后才返回。快照按统计状态变更的顺序选择连续失败数和熔断状态，避免慢 fallback 覆盖较新的恢复状态。内部 owner ID 用于隔离组件重建和多个 Agent 实例，既不作为指标标签，也不替代 request_id。

旧 AgentStats / ToolStats 暂时保留用于对账和回退；没有同时把新旧统计各计为一份 Prometheus 事件。

## 导出、采样与故障

业务 Span -> 本地 MetricsSpanProcessor -> Prometheus Registry
          -> BatchSpanProcessor -> OTLP/Console -> Collector/其他 Trace 后端

本阶段强制使用 ALWAYS_ON，保证指标基于完整 Span；不读取 OTEL_TRACES_SAMPLER 来采样请求。后续详细 Trace 采样应放在本地投影之后或 Collector，不应把 SDK 的采样后数据当全量路由统计。

OTEL_TRACES_EXPORTER=none 关闭 Trace 出口，保留完整指标。Exporter 初始化失败同样只关闭出口；队列/出口故障不影响本地投影。OTEL_SDK_DISABLED=true 会同时关闭 Span 和指标采集，Monitor 使用旧输入，contextweave_metrics_capture_enabled=0；此时不能把没有新增样本解释为没有业务请求。

- contextweave_metrics_projection_errors_total：本地投影处理错误。处理器隔离异常，业务继续；检查 metrics_projection_failed 日志和对账结果。
- contextweave_trace_export_failures_total：导出失败批数。检查 trace_export_failed、地址、认证、接收端和超时。
- trace_setup_failed：出口或 SDK 配置初始化失败；出口失败仍保留本地指标。
- trace_flush_incomplete：退出时限时 flush 未完成，详细 Trace 可能丢失。SDK 有界导出队列不参与本地计数，队列中 Trace 丢弃不会减少已经投影的完成事件；当前未提供逐条队列丢弃量指标。

按 /chat 响应的 request_id / trace_id 在日志和 Trace 中定位具体请求，不要为它们创建 Prometheus 标签。

## Collector 本地配置

默认部署仍可使用 Console。需要 Collector 时显式选择覆盖文件：

```powershell
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.observability.yml logs -f otel-collector
```

覆盖文件启用固定版本的 Collector，应用以 HTTP/protobuf 发送到 http://otel-collector:4318；主机端口只绑定 127.0.0.1。config/otel-collector.yml 使用 memory_limiter -> batch -> debug，适合开发查看，不提供历史查询存储。

无需模型 Key 的接收端验证：

```powershell
.\.venv-win\Scripts\python.exe scripts/otlp-smoke.py --endpoint http://127.0.0.1:4318/v1/traces
```

脚本明确标记 synthetic=true，发送两个合成 Span，不调用 /chat 或真实模型，不伪造 LLM Usage。ok 同时检查队列 flush 和 Exporter 的失败批数，不把 flush 成功当成接收成功。

## Langfuse 与其他后端

业务代码继续只依赖 OTel。使用 config/otel-collector-langfuse.example.yml 作为 Collector 转发示例，在 Collector 环境中配置 LANGFUSE_OTLP_ENDPOINT 和完整的 LANGFUSE_OTLP_AUTH_HEADER；密钥不写入源码。端点应为所选区域或自建服务的 /api/public/otel 基础地址，由 exporter 添加 /v1/traces。示例包含当前原生 OTLP 接入的 x-langfuse-ingestion-version: 4 请求头。

这是对接模板，没有验证真实 Langfuse 账户、权限或看板字段展示。部署前按所使用的服务版本和官方文档确认端点、认证和字段映射。本地 Prometheus 投影不依赖 Langfuse 是否可用。

## PromQL 示例

请求 P95（毫秒）：

```promql
histogram_quantile(0.95, sum by (le) (rate(contextweave_request_duration_ms_bucket[5m])))
```

每实例 Agent P95：

```promql
histogram_quantile(0.95, sum by (agent, le) (rate(agent_latency_ms_bucket[5m])))
```

MCP 业务结果失败率（选择业务维度）：

```promql
sum by (tool) (rate(contextweave_tool_results_total{layer="mcp",dimension="business",success="false"}[5m]))
/
sum by (tool) (rate(contextweave_tool_results_total{layer="mcp",dimension="business"}[5m]))
```

已知输出 Token 速率：

```promql
sum by (model) (rate(contextweave_llm_tokens_total{type="output"}[5m]))
```

分位数来自桶估计，需要有足够请求样本；没有 Usage 的模型调用仍计入 llm_calls，但没有相应 Token 样本。

## 本次验证

- 完整回归 229 项通过，包括原接口、Tool Trace、路由、记忆、RAG、经验和新增 27 项 P2 测试。
- 实际 loopback OTLP HTTP 发送和 Protobuf 解码，确认 Trace ID、父子关系及成功/503 故障。可重复 smoke 脚本也实际发送验证通过。
- 固定 N 次完成得到 N 个请求/Agent 样本；反复 Monitor 采集不会新增 Histogram 样本；缓存、熔断、并发、慢 fallback、取消和组件隔离的新旧对账通过。
- pip check 和 git diff --check 通过。Collector Compose 合并配置校验通过；当前机器 Docker 引擎未启动，因此没有运行真实 Collector 容器或验证镜像拉取。
- 本机合成基线：每种模式 5000 个 SDK Span，SDK 约 24.00 微秒/Span，附加本地投影后约 35.01 微秒/Span，额外约 11.01 微秒。没有网络出口，只用于开发基线，不替代真实负载压测。

```powershell
$env:OTEL_TRACES_EXPORTER='none'
.\.venv-win\Scripts\python.exe -m pytest tests -q
```

后续 P3 才增加历史查询、受控候选快照、评测关联、告警/成本看板、详细 Trace 采样和保留策略。

参考：[OTel Collector 配置](https://opentelemetry.io/docs/collector/configuration/)、[Prometheus Python Histogram](https://prometheus.github.io/client_python/instrumenting/histogram/)、[Langfuse 原生 OTel 接入](https://langfuse.com/integrations/native/opentelemetry)。
