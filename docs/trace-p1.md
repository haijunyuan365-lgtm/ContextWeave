# Trace 第二阶段（P1）

本阶段落实改进计划中的并发、RAG、Tool、LLM 和 Experience 后台关联，沿用 P0 的导出配置。原有接口、路由、缓存、熔断和监控统计继续可用。

## 已实现

- 意图识别：intent.llm / intent.embedding / intent.pattern 是 intent.recognize 的兄弟子节点，记录分支结果和回退。复用入口已有根 Span，缓存命中只记录本次调用，不补造内部执行。
- Context：每次 RetrievalSession.retrieve 产生 context.source.retrieve，记录来源、数量、缓存、失败/降级状态。原有 single-flight 保留，同一请求的等待者各有调用 Span，实际后端只执行一次。
- 混合召回：rag.dense / rag.bm25 在线程中并行，正确继承 rag.hybrid 和请求上下文。失败来源和健康分支降级可定位。
- RAG：rag.rewrite、带数字 query_index 的 rag.query、两级 RRF、MMR 和 Rerank 记录实际执行、候选数量及回退。空召回不补造未执行阶段；MMR/Rerank 回退同步标记上层 RAG 降级。
- Agent：每个实际执行的 Agent 产生 agent.execute；专属 Agent 失败后，General 的 Span 和模型调用使用实际 Agent 名称。并行 Agent 是兄弟节点，合成在它们结束之后执行。直接调用 handle、run_parallel、compose 也建立正确的请求上下文，正常 API 路径不会产生重复的同名层级。
- 工具：每次 Agent 工具派发和 MCP 调用产生 tool.execute，区分技术成功和业务结果成功，并记录缓存、超时、熔断与降级。原 tool_traces 的字段和行为保留。
- LLM：现有 10 个 messages.create 调用点全部使用 observability.llm.generate，覆盖 Agent、Composer、Intent、Planner、Rewrite、Rerank、记忆压缩、摘要合并、记忆提取和 Judge。
- Experience：案例持久保存来源 Trace / Span ID。Worker 启动时不继承请求上下文，每次处理建立独立 Trace，通过 Span Link 关联原提交。重试使用新的 Trace；旧案例没有来源字段仍可处理；追踪不额外读取存储。

## 主要结构

```text
intent.recognize
├── intent.llm
│   └── llm.generate [intent]
├── intent.embedding
└── intent.pattern

context.source.retrieve [knowledge]
└── rag.pipeline
    ├── rag.rewrite
    │   └── llm.generate [rag.rewrite]
    ├── rag.query [query_index=0]
    │   └── tool.execute [layer=mcp]
    │       └── rag.hybrid
    │           ├── rag.dense
    │           ├── rag.bm25
    │           └── rag.rrf.stage1
    ├── rag.query [query_index=1，可能并行或缓存命中]
    ├── rag.rrf.stage2
    ├── rag.mmr
    └── rag.rerank
        └── llm.generate [需要模型排序时]

agent.orchestrate
├── agent.execute [technical]
│   ├── llm.generate [round=1]
│   ├── tool.execute [layer=agent]
│   └── llm.generate [round=2]
├── agent.execute [billing，可能并行]
└── agent.compose
    └── llm.generate [需要合成时]

experience.submit -- Span Link --> experience.process [独立 Trace]
                                      └── llm.generate [evaluation.judge]
```

这是结构示意，只记录实际执行的阶段。缓存命中的 MCP 工具仍有调用 Span，内部 Dense / BM25 不会重放。子 Span 可重叠，总耗时应读取父 Span 的时间范围。

## 字段约定

- 所有业务 Span 使用 app.request_id / app.operation / app.span_type / app.outcome。
- RAG 使用 app.rag.input_count / output_count / input_list_count、app.rag.query_index、app.rag.subquery_count；只记录数字，不导出查询文本或 Chunk 正文。
- Tool 使用 app.tool.name / app.tool.layer / app.tool.success / app.tool.result_success、app.cache_hit、app.fallback_used 和固定 app.error_code。app.tool.success 表示原始调用是否成功，result_success 表示返回数据的业务 success；未明确声明 success 的普通返回沿用成功返回约定。旧 ToolResult.success 和旧统计仍保持原语义，包括降级可能返回 success=true。
- layer=agent 表示模型触发的工具派发，layer=mcp 表示内部 MCP 调用。两者是不同层级；后续指标聚合需明确选择层级，避免把嵌套调用重复计为同一类调用。
- LLM 使用 gen_ai.provider.name / request.model / response.model / response.finish_reasons、gen_ai.usage.input_tokens / output_tokens、app.llm.stage / round。
- provider.name=anthropic 表示当前 Anthropic 协议的遥测格式，可兼容第三方服务；request.model 和 response.model 分别记录请求模型与实际返回模型。
- Token 直接使用返回的 Usage 字段；未知、缺失或非法值不记录，已知的 0 会记录。缓存读取/写入 Token 单独记录 app.llm.cache_read_tokens / cache_write_tokens，不估算未知用量或成本，也不把 SDK 内部重试次数冒充已知次数。
- GenAI 字段固定参考语义约定 1.37.0，通过 app.gen_ai.semconv_version 标注；与 Python SDK 的 1.44.0 版本分别维护。
- 超时、业务失败、LLM/Embedding/RAG 回退、Agent 升级、Composer 确定性合成均可在相应 Span 定位。异常内容仍经过 P0 出口脱敏。

## 验证

```powershell
$env:OTEL_TRACES_EXPORTER='none'
.\.venv-win\Scripts\python.exe -m pytest tests -q
```

新增 tests/test_trace_propagation.py，使用真实业务方法和离线替代服务，覆盖意图/Context/检索/Agent 并发关系、single-flight、缓存、两级 RRF 和候选数量、MMR/Rerank 回退、工具双维成功/超时/熔断、LLM Usage、所有模型调用点覆盖、直接方法调用和 Experience Link/重试/旧案例兼容。

所有验证均不调用真实计费模型，也不要求 Redis、Chroma 或外部 OTLP 服务。P2 的指标投影、Histogram 修正和真实 Collector 联调尚未实施。

参考：[GenAI 语义约定 1.37.0](https://github.com/open-telemetry/semantic-conventions/blob/v1.37.0/docs/gen-ai/gen-ai-spans.md)、[OTel Python Trace API](https://opentelemetry-python.readthedocs.io/en/latest/api/trace.html)。
