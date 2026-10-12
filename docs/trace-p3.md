# Trace 第四阶段（P3）

本阶段提供可离线验证的本地历史后端和质量回归入口；原有 OTLP 出口继续可用。业务代码仍只调用 observability，不依赖 SQLite、Langfuse 或其他平台 API。

## 历史与访问控制

TraceHistory 用 SQLite WAL 存储清洗后的 Span 元数据、父子关系、Link、质量分数与数值事件。独立 BatchSpanProcessor 在后台写入；业务完成线程只运行原有全量 Metrics 投影，不访问磁盘。写入故障计入 contextweave_trace_export_failures_total，不改变业务返回值。API 查询在线程池执行，存储错误返回固定的 503。

默认关闭本地历史。以下配置在进程启动前设置，修改后重启：

```dotenv
TRACE_HISTORY_ENABLED=true
TRACE_ADMIN_TOKEN=<独立管理令牌，不要使用模型密钥>
OTEL_TRACES_EXPORTER=none
```

none 关闭 Console/OTLP 出口，仍允许本地历史；otlp 可以同时发送到外部后端。所有新 /trace/history 与 /trace/regressions 接口要求 Authorization: Bearer <TRACE_ADMIN_TOKEN>。未配置令牌返回 503，缺失或错误令牌返回 401。旧工具 Trace 接口保持原有兼容行为，新鉴权不覆盖旧接口。

| 接口 | 行为 |
| --- | --- |
| GET /trace/history | request_id、outcome、since_ns 筛选；limit 1–100，offset 分页 |
| GET /trace/history/{trace_id} | Span 明细、父子/Link、候选快照、关联评测；无数据返回 404 |
| DELETE /trace/history/{trace_id} | 删除 Span、评测、回归文本；近期删除 ID 阻止已排队 Span 再写入 |
| GET /trace/regressions | Judge 有效且未通过质量门槛的候选，包含 replay_ready |
| PUT /trace/regressions/{trace_id} | 明确提交审核后的 {"turns": ["测试问题"]}；每轮最多 2000 字，最多 10 轮 |
| POST /trace/regressions/{trace_id}/run | 调用现有 Evaluator 重跑；未提交文本返回 409；新 Trace 链接源 Trace |

历史最终一致，正常写入延迟由 OTEL_BSP_SCHEDULE_DELAY 决定（SDK 默认 5 秒）。404 也可能表示采样未保存、尚未导出或已经过期。关闭时执行 force_flush。队列溢出、进程崩溃、Span 数上限可能造成不完整 Trace；详情返回上限及 possibly_truncated，这不能证明所有 Span 已收齐。

## 候选证据与评测关联

Dense/BM25、两级 RRF、MMR、Rerank 记录 rag.candidates 事件。快照包含 Chunk ID 的 SHA256 前 32 位、从 1 开始的排名及有限数值 score，最多 16 项；附完整候选数量和截断标记。MMR/Rerank 的输入、输出及降级返回也保存快照。各级分数定义不同，只在对应阶段解读，跨阶段用 ID 哈希对照候选是否丢失。已知原 Chunk ID 可用 hash_identifier 计算对应引用。

不记录查询、标题、文档正文、向量或工具参数。历史明细去掉内部 Monitor owner 与兼容统计属性。普通对话和低分回答不会自动复制到回归库；管理员需要明确提交审核后的测试文本。提交后的回归文本属于受控数据，存入本地 SQLite，随源 Trace 过期或删除一起清理。

每轮端到端评测建立独立 evaluation.dialog Trace，将相关性、准确性、完整性、有用性、overall、passed、judge_failed 写入 Span，报告 metadata 返回 trace_id/request_id。多轮输入仍复用原有历史上下文。Judge 故障返回的占位分数不会变成低分候选，也不会通过评测。

Experience 后台评审将四维分数写入独立工作 Trace，并关联原始对话 Trace。历史后端把评审附加到源 Trace 的 evaluations；源与评审批次乱序也能关联。候选采用对应评测器实际的 passed 判定：端到端门槛 0.75，Experience 使用已有综合分和准确性门槛。未保存的源 Trace 不会凭空补出对话明细。回归重跑通过真实来源 Span 的 Link 关联，报告返回新旧 Trace ID。

## 采样、保留与成本

统一参数位于 [config/observability.yml](../config/observability.yml)。TRACE_POLICY_PATH 可指定其他文件；TRACE_HISTORY_ENABLED、TRACE_HISTORY_PATH、TRACE_EXPORT_RATIO 覆盖对应配置。

- 默认保留 7 天、最多 2000 个 Trace，每个最多 500 个 Span。批次写入和查询时清理，Span/评测/回归文本联动删除。删除标记也按期限及数量清理，无限久以后重复导入不在保护范围内。
- 明细采样默认 1.0，按 Trace ID 低 64 位确定，历史与外部出口决策一致；0 不保存明细。采样发生在完整 Metrics 投影之后，不改变调用量、成功率、延迟、Token 和可估算费用统计。队列仍有界，采样不能代替容量规划。
- 本项目不使用 SDK head sampler 做本地采样；OTEL_TRACES_SAMPLER 不改变全量投影。OTEL_SDK_DISABLED=true 会关闭全量采集，采集状态 Gauge 为 0。
- 非法策略配置记录固定警告、关闭明细出口并保留指标，避免无效采样配置扩大明细保存。
- prices 默认为空。模型名匹配实际 response.model（没有时用 request.model），提供币种、版本及 input/output/cache_read/cache_write 四类每百万 Token 单价。四项 usage 全部已知才生成估算；缺价格或任一 usage 均保持未知，已知零值可以产生零费用样本。

费用指标为 contextweave_llm_estimated_cost_total{provider,model,stage,currency,price_version}，contextweave_llm_cost_known_total 表示可估算调用覆盖率。历史 estimated_cost 附币种及价格版本，看板按币种/版本分组。估算不能当作账单；兼容服务的 Token/缓存口径须先核对，当前按 Anthropic 协议四项独立用量计算。修改价格后重启，历史估算保留当时版本。

本地数据库不加密，部署时限制数据卷及备份的访问权限。逻辑保留上限不保证 SQLite 文件立刻缩小；关闭时 WAL checkpoint，DELETE 使用 secure_delete。备份及外部 OTLP 后端须分别配置保留规则。本实现适用于单机 SQLite 部署，尚未做生产负载压测。

## 告警与看板

Prometheus 已挂载 config/alerts，包含 P95 超过 10 秒、错误/降级率超过 10%、追踪导出失败、全量采集关闭、应用不可用共 5 条告警。前两条使用 5 分钟窗口、至少 20 次请求，要求持续 5 分钟；低流量不触发。告警可在 Prometheus /alerts 查看，未配置 Alertmanager 或外部通知渠道。

Grafana 的独立可选 Compose 文件自动配置 Prometheus 数据源和 12 个面板：吞吐、P95、降级率、Token、费用、费用未知比例、RAG、工具和出口状态。依据 [Grafana 文件 provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) 和 [Prometheus 告警规则](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/) 配置。

```powershell
$env:GRAFANA_ADMIN_PASSWORD='<独立管理密码>'
docker compose -f docker-compose.yml -f docker-compose.dashboard.yml up -d
# 同时启动 Collector 时，再加 -f docker-compose.observability.yml
```

Grafana 只绑定本机 127.0.0.1:3000，禁用匿名访问和注册。应用历史使用独立 observability-data 命名数据卷，策略文件只读挂载，镜像预建目录并设置应用用户权限。本地 Python 运行使用 data/observability。服务启动不会自动运行计费模型回归；手动回放使用现有模型配置。

## 验证

- 全量 Python 回归 258 项通过，新增 29 项 P3 用例：真实 SQLite 后台持久化/重开、鉴权和边界、保留及数量上限、删除后迟到写入、分数关联及批次乱序、回放 Link、采样 0/0.5/1 时全量指标、成本未知/零值和写入故障隔离。
- Prometheus 官方 promtool 3.5.0：5 条规则语法通过，3 组时间序列测试验证持续触发、低流量抑制及应用不可用；12 个看板 PromQL 表达式语法通过。fixture 位于 tests/fixtures/trace_alerts.yml，没有放入生产 rules 通配目录。
- Compose 基础 + Collector + Grafana 合并配置及 pip check 通过。Docker 引擎未启动，尚未验证容器实际启动、Grafana 页面或外部通知；未调用真实计费模型、Redis、Chroma 或 Langfuse 账户。

```powershell
$env:OTEL_TRACES_EXPORTER='none'
.\.venv-win\Scripts\python.exe -m pytest tests -q
promtool check rules config/alerts/trace.yml
promtool test rules tests/fixtures/trace_alerts.yml
```
