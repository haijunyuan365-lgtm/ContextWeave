# Trace 第一阶段（P0）

对应改进计划的 P0，建立完整 /chat 主链路。

## 完成范围

- 新增 observability 包，统一初始化、Span 包装、业务结果、上下文传播、日志关联和出口脱敏。
- OTel API / SDK / OTLP Exporter 固定 1.44.0，FastAPI Instrumentor 固定配套的 0.65b0；要求 Python 3.10+，现有 Docker Python 3.12 符合要求。
- FastAPI 和业务显式共享一个 Provider，避免覆盖 Chroma 等依赖的全局 Provider；默认 service.name=contextweave，MVP 全量采集。
- Console / OTLP HTTP-protobuf 使用 BatchSpanProcessor 和有界队列；导出失败不会中断业务。服务关闭 force_flush / shutdown，业务组件启动或关闭失败时也执行追踪收尾。
- request_id 保留业务 UUID，trace_id 使用标准 OTel ID。日志附带 request_id / trace_id / span_id，通过 ContextVar 和 OTel Context 隔离请求。
- /chat 新增 trace_id 和 request_duration_ms，后者覆盖记忆、Context、Agent、提交、清理、写回。原 latency_ms 继续表示 Agent 编排耗时，HTTP Server Span 进一步覆盖响应发送。
- prepare 纳入异常清理范围，关闭已经创建的检索会话，普通清理异常不会覆盖原异常。
- 保留原 Tool Trace、AgentStats、路由、Monitor 和 Prometheus 行为。

## 主链路

```text
POST /chat [HTTP server]
└── chat.pipeline
    ├── memory.begin_turn
    ├── context.prepare
    │   ├── memory.load_lightweight
    │   ├── intent.recognize
    │   ├── context.plan
    │   ├── context.retrieve
    │   └── context.skills [按需]
    ├── agent.orchestrate
    │   ├── agent.route [无需澄清时]
    │   ├── agent.execute [并行时为兄弟节点]
    │   └── agent.compose [多 Agent 时]
    ├── experience.submit [未配置时 skipped]
    ├── context.cleanup
    └── memory.persist
```

异常时只记录实际执行阶段。Span 时间可重叠，不能累加全部子 Span 计算总耗时。成功使用 OTel UNSET 和 app.outcome=success；异常使用 ERROR 和固定错误码，已处理降级在对应阶段记录 degraded。

## 配置与本地查看

沿用项目现有 .env / 环境变量，不修改密钥。安装更新后的 requirements.txt；Docker 需要重新构建。默认无需额外配置，Console 输出 JSON：

```env
OTEL_SERVICE_NAME=contextweave
OTEL_TRACES_EXPORTER=console
OTEL_SDK_DISABLED=false
OTEL_BSP_MAX_QUEUE_SIZE=2048
OTEL_BSP_MAX_EXPORT_BATCH_SIZE=512
OTEL_BSP_SCHEDULE_DELAY=5000
```

已有 OTLP 接收端时设置：

```env
OTEL_TRACES_EXPORTER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318
OTEL_EXPORTER_OTLP_TIMEOUT=3
```

采用 HTTP/protobuf，基础 endpoint 会添加 /v1/traces；也支持完整 OTEL_EXPORTER_OTLP_TRACES_ENDPOINT。认证使用 OTEL_EXPORTER_OTLP_HEADERS，不要写入源码。

调用 /chat 后，用响应的 request_id / trace_id 在 docker compose logs -f echomind 中定位日志和 Span JSON。parent_id / context.span_id 可还原树，start_time / end_time 定位耗时。默认每 5 秒批量输出，正常退出尝试 flush。

OTEL_TRACES_EXPORTER=none 保留上下文和 ID，但没有出口。OTEL_SDK_DISABLED=true 停止采集，trace_id 可能为空或只来自上游上下文。初始化失败降为 NoOp，日志记录 trace_setup_failed，业务可继续。配置在进程初始化时读取，修改后重启；Provider 生命周期与服务进程相同。

本阶段没有新增历史 Trace API，也没有部署 Collector / Langfuse；旧 /trace/tool/{request_id} 仍返回旧工具视图。

## 脱敏与故障

仅采集结构化元数据，会话使用哈希；默认不采集消息、Prompt、模型正文、工具参数、异常原文或堆栈。自动 HTTP Span 的异常也经过出口过滤，URL 移除查询参数并使用路由模板，HTTP 请求/响应头默认不采集。

trace_export_failed batches=N 表示当前出口失败批数，业务保持原行为。检查接收端地址、4318 端口、认证和网络。有界队列可能丢弃 Trace，P0 尚未提供队列/丢弃量的 Prometheus 指标。trace_flush_incomplete 表示限时 flush 未完成，可能丢失待导出数据。

## 验证与后续

离线验证不调用真实模型，不要求模型 Key、Redis 或 Chroma 服务：

```powershell
$env:OTEL_TRACES_EXPORTER='none'
.\.venv-win\Scripts\python.exe -m pytest tests -q
```

新增 tests/test_observability.py，使用 InMemorySpanExporter 验证 HTTP 树、耗时边界、旧工具接口、并发隔离、Agent 兄弟关系、gather/create_task/to_thread、W3C 传播、取消、各阶段异常、清理、脱敏、Console、初始化、关闭和模拟 Exporter 故障。

P1 已完成，详见 [第二阶段文档](trace-p1.md)。P2 实现 Span→Metrics 并修正 Histogram。本阶段未修改原周期性监控统计。

参考：[OTel Python 导出](https://opentelemetry.io/docs/languages/python/exporters/)、[FastAPI Instrumentor](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html)。
