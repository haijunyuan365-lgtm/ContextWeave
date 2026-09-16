# Context Planner 使用与设计

## 主链路

`/chat` 与 CLI 共用 `ContextPipeline.prepare()`：先从 Redis 读取最近 5 条消息和最多 800 字既有摘要，再进行三路意图融合、上下文规划和按需检索。准备流程不会调用旧的全量 `get_context()`，也不会为了规划生成新的摘要。

```text
Query + LightweightContext
  -> IntentResult（含摘要输入，缓存键也包含摘要）
  -> ContextPlan（Rule / LLM / Fallback）
  -> Memory / Profile / Knowledge 并发按需读取 + Skill 候选
  -> ContextBundle
  -> 原有路由与多 Agent 执行
  -> 必要时使用同一 RetrievalSession 补查
```

返回回复后，仍写入工作记忆并异步更新画像。这里的“跳过画像读取”指回复前的上下文预加载；后台画像更新仍可能读取旧画像。Memory Evolution 不在本次改造范围内。

## 规则和 LLM 兜底

规则依据意图、意图组、实体、置信度、历史依赖、偏好信号和业务关键词规划。轻量历史用于判断指代是否有近期业务线索。LLM 规划接收相同的轻量输入。

| 输入场景 | 预加载行为 |
|---|---|
| 退款多久到账？ | 知识、匹配的 Skill |
| 还是按照我上次那个退款方式 | 情景记忆、画像、知识、Skill |
| 你好 / 谢谢 | 跳过长期记忆、画像和知识；允许匹配通用 Skill |
| 转人工 | 沿用升级流程，不额外查询知识 |
| 近期已讨论退款，再问“这个需要多久” | 使用近期线索，不仅因指代就读长期记忆 |
| 登录失败同时重复扣款 | 触发 LLM 规划，路由仍由原编排器决定 |

置信度低于阈值、指代未解决、涉及多个业务领域或规则无法判定时，最多调用一次 LLM。SDK 自动重试关闭，默认 5 秒超时。JSON 字段、布尔值和理由格式严格校验；失败回退到已检测信号对应的规则计划，不执行全量检索。

`need_experience` 始终为 false。query 默认使用用户原话；有指代时可追加近期消息或摘要中的实际业务片段，不接受模型生成的订单号等新事实。调试理由使用固定描述，避免模型将个人信息写进理由。

| 环境变量 | 默认值 | 含义 |
|---|---|---|
| `ECHOMIND_PLANNER_LLM_ENABLED` | `true` | 关闭后仍运行规则，疑难场景标记为 fallback |
| `ECHOMIND_PLANNER_MODEL` | 当前 `ANTHROPIC_MODEL` | 单独覆盖 Planner 模型 |
| `ECHOMIND_PLANNER_CONFIDENCE_THRESHOLD` | `0.7` | LLM 兜底阈值 |
| `ECHOMIND_PLANNER_TIMEOUT_SECONDS` | `5` | 单次 Planner 调用超时 |

这些配置只控制 Planner。意图识别、原有 RAG 改写/重排和 Agent 本身的模型调用保持原有流程。

## 检索、缓存与补查

`ContextBundle` 保存近期消息、摘要、记忆、画像、知识、Skill 候选及 Experience 空列表。知识默认取 5 条，背景文本保留现有局部长度限制；Skill 根据实际 Agent 类型过滤，Composer 使用 general Skill。这里没有实现统一 Token Budget。

`RetrievalSession` 每个请求独立创建。缓存键包含来源、用户、会话、规范化 query 和 top_k。同参数进行中的任务也可共享；不同 query 或 top_k 不做近似复用。成功结果和正常空结果可以缓存，失败或部分降级结果不进入本轮缓存，允许后续重试。请求结束清理缓存及未完成任务。底层公共知识库的原有 TTL 缓存继续保留，个人记忆和画像不会放入该全局缓存。

Agent 工具：

- `search_knowledge_base(query, top_k=5)`：知识检索。
- `search_conversation_memory(query, top_k=5)`：当前用户的历史语义检索。
- `get_user_profile()`：当前用户画像。

知识 `top_k` 范围为 1–20，情景记忆沿用最多 5 条的限制。用户与会话由服务端 Request 绑定，工具不接受 `user_id` 或 `conv_id` 参数。`need_* = false` 不会禁用工具。原有角色工具和升级节点行为保留；升级节点仍是确定性的交接节点，不额外调用 LLM。

记忆和画像只提供历史与偏好，不能当成订单状态、退款结果或余额等实时业务事实。

## 调试与接口兼容

```bash
curl -X POST http://localhost:8000/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"退款多久到账？","user_id":"demo","conv_id":"demo-1","debug_context":true}'
```

`context_debug` 包含：

- `planner_source`：`rule`、`llm`、`fallback`。
- `plan`：五个 `need_*` 标志。
- `reasons`：各来源的固定理由摘要。
- `sources`：最近一次结果状态与数量、累计等待毫秒数、访问次数、缓存命中次数。

状态包括 `skipped`、`success`、`empty`、`failed`、`unconfigured`；知识部分成功时使用 `degraded`，只提供真实证据，不将降级提示注入为知识。多个并发等待会分别计入累计等待时间，该值不是整轮延迟。

默认响应不含 `context_debug`。`knowledge_used` 表示有效知识已提供给 Agent，不表示模型答案必然引用了该知识；仅调用失败或得到空结果时为 false。`tools_used` 只记录实际 Agent 工具调用。日志中的 `context_summary` 使用同一摘要，不包含原始 query、历史、画像和知识正文；已有其他模块的日志策略不由该功能改变。

旧 `MemoryManager.get_context()` 保留完整上下文行为。直接调用 `AgentOrchestrator.run(Request(...))` 和既有评测仍可使用 `context/history`；需要完整 Planner 流程时先调用 `ContextPipeline.prepare()`。CLI 共用该流程，但沿用原来未配置知识库的设置，其知识来源显示 `unconfigured`。

## 测试与本地环境

测试使用模拟 LLM、Redis、Chroma 和工具提供方，验证真实准备流程、HTTP 序列化、工具执行及缓存行为，不依赖在线模型或真实数据库。

Windows 示例：

```powershell
python -m venv .venv-win
$env:PYTHONUTF8 = '1'
.\.venv-win\Scripts\python.exe -m pip install -r requirements.txt pytest
.\.venv-win\Scripts\python.exe -m pytest tests -q
```

仓库原 `.venv` 带有 macOS 路径，不应直接用于 Windows。当前 Windows 本地环境曾出现 ONNX DLL 初始化失败；离线测试不构造真实 Chroma 后端。真实部署仍需具备可用的 ONNX/Chroma、Redis 和模型连接，不能用模拟测试代替实际服务联调。

首版不新增数据库表或迁移，不包含长期记忆演化、RAG 文档分层、Experience 检索、统一 Token 预算或持久化 Context Trace。
