# Phase 0：建立改造基线

> 本文严格对应当前代码。Phase 0 只建立回归与调试基线，没有实现 `project_id`、多租户隔离、真实身份认证或外部渠道。

## 1. 流程定位

当前 Web 请求的端到端主流程是：

```text
POST /chat
  ↓
ChatRequest（message / user_id / conv_id / debug_context）
  ↓
MemoryManager.begin_turn（预留本轮消息顺序）
  ↓
ContextPipeline.prepare
  ├─ 读取轻量上下文：最近 5 条消息 + 摘要
  ├─ IntentRecognizer：Rule / Embedding / LLM 融合
  ├─ ContextPlanner：规则优先，不确定时 LLM 兜底
  └─ 按需预加载 Memory / Profile / Knowledge / Skill / Experience
  ↓
AgentOrchestrator.run
  ├─ 低置信度澄清
  ├─ 领域打分与主/辅 Agent 路由
  ├─ Agent 执行、Tool 调用或降级
  └─ 必要时合并多 Agent 回复
  ↓
Experience Capture（如果已配置）
  ↓
写入用户/助手消息
  ↓
ChatResponse
```

本次功能不是该业务链上的新节点，而是覆盖整条链路的“回归安全网”：

```text
固定 Demo Cases ──输入契约──▶ /chat 主链路 ──输出契约──▶ 回归断言
                                      │
                                      └──▶ Intent + ContextPlan + Routing 决策快照
```

它的上游是请求输入和意图结果，下游是 Planner、Router、Agent 与最终 API 契约。后续 Phase 1 开始加入 `project_id` 时，这些基线用例会立即暴露无意的行为变化。

## 2. 问题与作用

以“登录时报 401，同时订单被重复扣款”为例，当前系统应识别为技术问题，由 `TechnicalAgent` 主处理，并让 `BillingAgent` 辅助。如果后续加入项目隔离时误改了 Request 或路由逻辑，表面上 API 仍然返回 200，实际上却可能只调用 BillingAgent，甚至跳过知识检索。

Phase 0 前有三个具体问题：

1. 用例分散在各测试文件中，没有一组能表达核心业务行为的固定 Demo Cases。
2. `/chat` 虽然返回部分意图和路由字段，`debug_context` 却只覆盖 Context Planner，日志无法串起三段决策。
3. 当前 Windows 环境中，TestClient 工作线程首次导入 AnyIO 后写入 `.pyc` 会卡住，使“测试是否通过”本身都不稳定。

本次改动后：

- 六个业务案例固定了输入、规划和路由预期。
- 一个 `request_id` 下可同时看到 Intent、ContextPlan 和 Routing 的决策快照。
- 默认 `python -m pytest -q` 在当前环境可稳定完成。

## 3. 设计思想

### 用契约保护行为，不锁死实现

基线断言关注用户可感知的结果：API 字段、是否需要知识、主 Agent、辅助 Agent、是否升级。它不断言内部每一次函数调用，因此后续可以重构内部类，只要行为契约不变即可。

### 离线、确定性的编排回归

Demo Cases 将意图识别结果作为固定输入，Agent/Composer 使用离线假 LLM，Memory 和 Knowledge 使用内存假实现。这样测试的失败能指向编排契约的变化，而不是网络、API Key 或模型随机性。

替代方案是每次直接调用真实 LLM、Redis 和 ChromaDB。这适合发布前的端到端验收，但不适合当作每次代码改动都必跑的回归门禁。当前取舍是“编排契约稳定”优先，真实模型效果仍需用 Evaluation 和集成环境补充。

### 结构化、脱敏的决策快照

调试快照保留意图类别、置信度、来源分数、Planner 开关、上下文来源状态、主/辅 Agent 和路由理由。实体只记“类型和数量”，不记订单号、金额等值；同时不记录 query、知识正文和最终回复。

替代方案是直接记录完整 Request/Response，排查会更直观，但客服语料极易包含账号、订单和个人信息。当前选择缩小日志面，用 `request_id` 进行链路关联。更完整的 Trace/OpenTelemetry 是后续可选方案，不在 Phase 0 范围内。

### 向后兼容

`debug_context=false` 时仍不返回调试信息；开启时，原有 `planner_source`、`plan`、`reasons`、`sources` 继续保留，只新增 `intent`、`routing`、`execution` 和 `request_id`。因此原有调用方无需修改。

## 4. 实现过程

下面沿“登录时报 401，同时订单被重复扣款”的一次请求说明。

### 4.1 输入和意图结果

`api/main.py::chat` 将 `ChatRequest` 转换为 `agents/agent_orchestrator.py::Request`。`ContextPipeline.prepare` 先读取轻量上下文，再调用 `recognize_intent`。

除了原有 `intent`、`intent_group`、`confidence`、`urgency` 和 `entities`，现在还会将多路识别的 `source_scores` 写入 `Request.intent_source_scores`。这一字段是当次请求内存状态，没有新增数据库表。

### 4.2 Context Plan 和数据流转

`ContextPlanner` 看到 401 和扣款信号后，将该问题判定为业务知识需求，设置 `need_knowledge=true`。`ContextPipeline` 将检索结果写入 `ContextBundle.knowledge`，并在 `ContextBundle.sources` 中累计状态、数量、耗时、调用次数和缓存命中次数。

### 4.3 路由和执行

`AgentOrchestrator._route_decision` 使用意图、关键词、实体和 Agent 可用性打分。该案例中：

- `technical_login` 和 `error_code=401` 使 Technical 得分最高。
- “扣款”是明确的 Billing 信号，因此 Billing 成为 supporting agent。
- 两个 Agent 并行执行，`ResponseComposer` 负责合并成一个回复。

### 4.4 决策快照和输出

`core/context_runtime.py::context_debug` 现在接收 Request 和可选的 OrchestratorResult，生成统一快照：

```json
{
  "intent": {
    "category": "technical_login",
    "group": "technical",
    "urgency": "HIGH",
    "confidence": 0.92,
    "entity_counts": {"error_code": 1}
  },
  "plan": {"need_knowledge": true},
  "routing": {
    "primary_agent": "technical",
    "supporting_agents": ["billing"],
    "escalated": false
  }
}
```

上面为结构示意，真实快照还包含其他 Planner 开关、原因、来源状态和执行记录。`api/main.py::chat` 在 Agent 执行后将该快照写为 `request_decision` 日志；只有请求显式传入 `debug_context=true` 时，才同时把它放入 API 响应。

### 4.5 固定 Demo Cases

`tests/fixtures/phase0_demo_cases.json` 是可读的业务案例清单，`tests/test_phase0_baseline.py` 将每一条送入真实 `chat()`、`ContextPipeline`、`ContextPlanner` 和 `AgentOrchestrator`。它只替换了外部不稳定边界：

- 意图识别结果由 fixture 提供。
- LLM 返回固定文本。
- Memory 和 Knowledge 使用内存假实现。

主链路、Planner 规则、Router 打分、并行 Agent 调度和 ChatResponse 构造仍是真实生产代码。

### 4.6 测试运行基线

`tests/conftest.py` 在 pytest 启动时禁止后续导入写入 `.pyc`，解决当前 Windows 环境的 TestClient 卡住问题。`requirements-dev.txt` 则固定本次验证使用的 pytest 版本。

## 5. 边界与验证

### 权限与隔离

- **已完成**：决策快照不写入 `user_id`、`conv_id`、消息原文、实体值、知识正文和 Agent 回复。
- **已完成**：`debug_context` 仍然是显式开关，默认 API 响应不包含调试快照。
- **待开发**：当前 `/chat` 仍信任请求传入的 `user_id`，没有 JWT、RBAC 和 Project 隔离；这些属于后续 Phase 1/12。

### 异常处理与适用限制

- Agent 执行失败时，`finally` 仍会记录已经产生的 Intent/ContextPlan 快照，此时 Routing 部分使用空值。
- 调试日志中的 `sources` 只记状态和统计，不包含检索数据。
- Demo Cases 是离线编排回归，不验证真实 Anthropic 模型质量、Redis 持久化、Chroma 检索效果、Docker 网络或生产性能。
- 基线不保证用户输入的意图一定被真实 LLM 识别为 fixture 中的结果；意图模型质量应由现有 Evaluator 和独立数据集评估。

### 实际执行的验证

在 2026-09-18 的当前工作区执行：

```text
python -m pytest tests/test_phase0_baseline.py -q
结果：6 passed

python -m pytest -q
结果：147 passed

python -m compileall -q agents api core tests
结果：通过，无输出

git diff --check
结果：通过；仅出现 Git 的 LF/CRLF 转换提示，无空白错误
```

未执行的内容：真实 LLM/API Key 调用、Docker Compose 全栈启动、Redis/Chroma 真实数据写入和压力测试。

## 6. 面试表达

### 可自然复述的讲解

> 在做多项目改造前，我先给 EchoMind 建立了一层行为基线。原因是 Agent 系统的 API 返回 200 并不代表编排正确，意图、上下文选择或路由任意一环变了，都可能造成业务退化。我用六个固定案例保护 `/chat` 契约，其中外部 LLM、Memory 和 Knowledge 用离线假实现，但 Planner、Router、多 Agent 调度和 API 构造都走真实代码，这样测试稳定且能精准定位编排回归。同时我把 Intent、ContextPlan 和 Routing 合并为一个结构化决策快照，用 request_id 串联，只记决策和数量，不记用户原文和实体值。最终全量 147 个测试通过。这一阶段不是新增多租户能力，而是为后续的 `project_id` 全链路改造建立可信的安全网。

### 可能的追问与回答要点

1. **为什么不直接拿真实 LLM 做基线测试？**
   - 真实 LLM 有随机性、网络与成本问题，会让回归失败原因不清晰。
   - 离线测试保护编排契约；真实模型质量用 Evaluation 和发布前集成测试补充。

2. **为什么固定的是主/辅 Agent 和 Plan，不是完整回复文本？**
   - 完整文本容易因 Prompt 和模型变化产生无业务影响的差异。
   - Plan 和 Route 是更稳定、更关键的业务契约。

3. **调试快照如何避免泄露客户数据？**
   - 实体只保留类型与数量，不保留值。
   - 不记录用户原文、用户 ID、会话 ID、检索正文和回复正文。
   - 通过 `request_id` 关联决策链路。

4. **这组测试能证明生产系统可用吗？**
   - 不能。它能证明当前代码的离线编排契约和已有单元/集成行为没有回归。
   - 生产可用性还需真实存储、模型、网络、安全和负载验证。

5. **Phase 1 加入 `project_id` 后，这个基线怎么扩展？**
   - 先保持现有六个案例向后兼容。
   - 再为 Project A/B 增加成对的隔离案例，断言 Request、ContextBundle、RetrievalSession 和数据访问都携带正确 project scope。
   - 特别增加“A 项目无法读到 B 项目数据”的反向测试。

