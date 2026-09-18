# Phase 1：Project 全链路改造学习文档

> 本文只描述当前仓库中已经实现并经过测试的 Phase 1 能力。文中的“隔离”指应用层的项目作用域隔离，不等同于已经具备完整的生产级身份认证、RBAC 或数据库行级安全。

## 1. 流程定位

EchoMind 的端到端主流程是：外部入口接收请求，先确定项目与用户，再组装上下文，随后识别意图、选择 Agent、调用知识与记忆工具，最后生成回复并回写本轮数据。

```text
HTTP / CLI 入口
    │  project_id + user_id + conv_id + content
    ▼
【本次 Phase 1】ProjectService.authorize
    │  校验项目存在、启用状态、成员关系
    ▼
Request（携带 project_id 与 ProjectConfig）
    ▼
ContextPipeline.prepare
    ├─ Memory：工作记忆 / 情景记忆 / 用户画像
    ├─ Experience：历史成功案例
    └─ Knowledge：项目知识库
            │
            ▼
ContextBundle → RetrievalSession（继续携带 project_id）
    ▼
IntentRecognizer → AgentOrchestrator → Agent / Tool
    ▼
ResponseComposer
    ▼
ChatResponse（回显 project_id）
    ▼
Memory / Experience 回写（仍按 project_id 隔离）
```

Phase 1 位于所有业务编排的入口边界，也贯穿后续的数据访问链。它的上游是 HTTP 路由或 CLI 调用方，下游是 `Request`、上下文流水线、Agent 工具、知识库、记忆和经验系统。关键点不是“多传了一个字段”，而是从授权到读写都不能丢失这个字段。

推荐的新入口是 `POST /api/projects/{project_id}/chat`。原有 `POST /chat` 继续存在：请求体可携带 `project_id`，省略时进入 `default` 项目。知识库、记忆与经验查询也提供了 `/api/projects/{project_id}/...` 形式的项目路由。

## 2. 问题与作用

假设同一个 EchoMind 实例同时服务“仓储售后项目”和“校园缴费项目”，两个项目里都可能出现用户 `alice`，甚至会复用会话号 `web-001`。

改造前，系统主要依靠 `user_id` 和 `conv_id` 查数据。这样会产生三类风险：

1. `alice + web-001` 的 Redis 工作记忆可能发生键冲突，仓储对话被校园项目读到。
2. 知识检索只按查询文本匹配，“出库失败”的请求可能召回另一个项目的文档。
3. 经验案例和长期画像只按用户定位，同名用户会共享不属于自己的历史经验。

Phase 1 把 `project_id` 提升为强制的数据作用域：

- 请求进入后先执行项目授权，未通过时不会启动记忆写入和 Agent 编排。
- `Request → ContextBundle → RetrievalSession → Tool` 全链路携带项目上下文。
- Redis 键、Chroma 元数据与过滤条件、内存锁、后台任务注册表、工具缓存键都包含项目维度。
- 即使 `user_id`、`conv_id`、查询语句完全相同，两个项目仍会访问不同的数据集合。

因此，本次改动解决的是“项目边界无法表达”的基础问题，为后续项目级配置、Agent 策略和更细粒度权限打底。

## 3. 设计思想

### 3.1 URL 中的 project_id 是新接口的权威来源

新接口使用 `/api/projects/{project_id}/chat`，因为项目属于资源定位的一部分，放在 URL 中比藏在 Prompt、Header 或任意请求字段里更直观，也方便日志、网关和权限策略识别。请求体仍保留可选 `project_id` 以兼容旧客户端，但如果它与 URL 冲突，接口返回 `409`，避免系统在两个值之间静默猜测。

替代方案是只使用请求体或只使用自定义 Header。请求体适合旧接口，但资源语义不够明显；Header 更适合租户上下文，却更容易在内部转发时漏传。当前采用“URL 权威、请求体兼容”的折中。

### 3.2 先授权，再创建任何业务副作用

`api/main.py` 的 `_chat()` 首先调用 `_authorized_project()`，成功后才执行 `MemoryManager.begin_turn()` 和 `ContextPipeline.prepare()`。这是“入口粗粒度授权 + 存储细粒度过滤”的组合：入口尽早拒绝非法请求，每个数据组件仍显式接收 `project_id`，避免内部调用绕过隔离。

只在入口校验而不改数据接口，会让后续代码很容易忘记过滤；只在存储层过滤，又会让非法请求继续消耗编排和模型资源。两层共同存在更适合当前模块化结构。

### 3.3 Project 是领域对象，不是一段散落配置

`projects/models.py` 定义了不可变的 `Project`、`ProjectConfig` 和 `ProjectMember`：

- `Project` 描述项目 ID、名称、启用状态和开放/封闭模式。
- `ProjectMember` 描述用户在项目中的角色和成员状态。
- `ProjectConfig` 预留系统提示词、允许的 Agent/Skill/Tool、模型配置和转人工策略。

`ProjectService.authorize()` 统一处理项目不存在、项目停用、用户为空、封闭项目无成员关系或成员停用等情况，API 再分别映射为 `404` 和 `403`。

当前 Repository 从 `config/projects.json` 启动时加载。这避免 Phase 1 同时引入数据库表、迁移工具和管理后台，便于先把领域契约和调用边界做正确。代价是配置修改后需要重启，且不适合大量项目或高频动态管理；数据库化属于后续演进方向。

### 3.4 数据隔离必须覆盖键、过滤器、缓存和后台任务

只给 Chroma 文档加一个 `project_id` 字段还不够。一次请求可能经过 Redis、向量检索、BM25、MMR、TTL 缓存、异步记忆演化和经验后台任务。因此实现采用统一原则：所有能够“定位一份数据或一个任务”的地方，都必须包含项目维度。

例如：

- 工作记忆键是 `wm:{project_id}:{user_id}:{conv_id}`。
- 知识向量检索使用 `where={"project_id": project_id}`，BM25 分支也再次过滤项目。
- Tool Manager 的调用参数包含 `project_id`，所以由参数生成的缓存键自然分区。
- 记忆演化的锁、任务、待处理队列和 Chroma 文档 ID 都由 `project_id + user_id` 共同确定。
- 经验案例当前按 `project_id + user_id` 隔离；“项目内共享经验”是计划中的 Phase 9，不在本次伪装实现。

另一种方案是每个项目创建独立 Redis DB 或独立 Chroma Collection，物理隔离更强，但项目数量增加时连接、Collection 和运维成本会快速上升。Phase 1 选择共享基础设施中的逻辑隔离，适合先完成应用架构改造；更高安全级别可在后续替换存储策略，而不改变上层接口。

### 3.5 兼容优先，但明确兼容边界

为了让现有调用继续工作，`default` 项目被配置为 `open`。旧 Chat 和 Knowledge 接口在没有显式项目参数时使用 `default`，旧 Memory 与 Experience 路由固定访问 `default`；新建项目在模型中默认是 `closed`，必须配置启用的成员关系。

知识库启动时会把缺少 `project_id` 的旧元数据归入 `default`。旧 Memory 和 Experience 的历史键没有自动搬迁到 v2 结构，因为错误合并长期记忆比暂时读不到旧数据风险更高；如需保留，应在真实数据盘点后单独编写可回滚迁移脚本。

## 4. 实现过程

下面沿一次真实形态的请求说明数据如何流动：

```http
POST /api/projects/warehouse/chat
Content-Type: application/json

{
  "user_id": "alice",
  "conv_id": "web-001",
  "content": "扫码出库失败，错误码 E102 怎么处理？"
}
```

### 4.1 入口识别项目并完成授权

`api/main.py` 的 `project_chat()` 以 URL 中的 `warehouse` 为权威项目 ID；若请求体显式填写另一个项目，立即返回 `409`。随后 `_chat()` 调用 `_authorized_project()`，最终进入 `projects/service.py` 的 `ProjectService.authorize()`：

1. 从 `ProjectRepository` 查找 `warehouse`。
2. 项目不存在或已停用时抛出 `ProjectNotFound`，API 返回 `404`。
3. 封闭项目查找 `(warehouse, alice)` 的启用成员关系；不存在或停用时返回 `403`。
4. 授权成功后返回经过验证的 `Project` 与 `ProjectMember`。

只有第 4 步成功，系统才会记录本轮用户消息。这一顺序由隔离测试验证：被拒绝的用户不会触发 Memory 和 Planner。

### 4.2 项目上下文贯穿编排链

`_chat()` 构造 `agents/agent_orchestrator.py` 中的 `Request`，写入 `project_id` 和 `project.config`。`core/context_runtime.py` 的 `ContextPipeline.prepare()` 再创建包含同一字段的 `ContextBundle` 与 `RetrievalSession`。

因此，Agent 延迟调用工具时不需要重新相信客户端参数，而是从已经授权的 `Request` 读取项目。`context_debug()` 与最终 `ChatResponse` 也会回显 `project_id`，方便确认本次响应属于哪个边界。

当前 `ProjectConfig` 已完成加载和传递，但 `allowed_agents`、`allowed_tools`、`system_prompt` 等策略尚未改变路由和 Prompt 行为；按改造计划，它们将在后续阶段接入。Phase 1 不宣称已经实现项目级 Agent 配置生效。

### 4.3 知识写入与检索

项目文档通过 `POST /api/projects/warehouse/knowledge/add` 或上传接口进入 `mcp/knowledge_base.py` 的 `KnowledgeBase.add_documents()`：

1. 文档切块。
2. Chunk ID 的生成输入包含 `project_id`，避免两个项目相同文本发生 ID 冲突。
3. Chroma 元数据写入 `project_id`。
4. BM25 内存索引保留同一项目字段。

当 Agent 的 `search_knowledge_base` 工具检索时，`agents/tools.py` 把 `req.project_id` 传给 `MCPToolManager.search_with_rewrite()`。向量分支用 Chroma `where` 过滤，BM25 分支在输出候选前过滤，RRF 融合和 MMR 去冗余也只处理当前项目候选。由于 `project_id` 同时在工具参数中，TTL 缓存不会把 A 项目的结果命中给 B 项目。

输出仍是当前项目的知识片段列表，交给 Context 或 Agent 生成答案。隔离测试使用同一查询词向两个项目写入不同文档，分别验证向量和 BM25 都只返回对应项目内容。

### 4.4 对话记忆和长期记忆

`memory/conversation_memory.py` 的 `MemoryManager.begin_turn()`、`add_message()`、`get_context()`、`search_episodic()`、`get_profile()` 等接口现在都显式要求 `project_id`。

以本次消息为例：

- Redis 工作记忆写到 `wm:warehouse:alice:web-001`。
- 摘要写到 `summary:warehouse:alice:web-001`。
- 情景记忆写入 Chroma 时带 `project_id=warehouse`，查询同时按项目、用户和会话过滤。
- Profile 文档 ID 由 `warehouse + alice` 生成，避免另一个项目的 `alice` 共用画像。

`memory/evolution_runtime.py` 的异步记忆演化同样按 `(project_id, user_id)` 建立待处理队列、锁、任务注册和快照文档。`memory/evolution.py` 的快照升级到 schema version 2，并在 `MemoryRecord` 和 `SourceMessage` 中记录项目。来源项目与目标快照不一致时不会合并。

### 4.5 经验案例

一次请求完成后，Context Pipeline 可能把成功结果交给 `experience/runtime.py` 的 `ExperienceManager.submit()`。案例快照包含 `project_id`，Redis case key 和 Chroma 元数据都以 `project_id + user_id` 为范围；`search()`、`list_cases()`、`feedback()` 和后台处理函数也必须传入两个值。

这样仓储项目的 `alice` 无法读到校园项目同名用户的案例。当前设计仍是“项目内按用户私有”，没有提前实现 Phase 9 的项目共享经验库。

### 4.6 返回与回写

Agent 编排完成后，API 返回的 `ChatResponse` 必定包含 `project_id=warehouse`。随后助手消息、记忆演化事件和经验候选继续使用同一个已验证项目作用域回写。一次请求从入口到输出的项目身份没有重新从用户输入推断，也没有退回只使用 `user_id` 的旧接口。

## 5. 边界与验证

### 5.1 已完成的边界

- Project ID 有格式校验：1–64 个字符，以字母或数字开头，仅允许字母、数字、下划线和连字符。
- Project、ProjectConfig、ProjectMember 在加载时校验状态、角色和 Agent 类型。
- 封闭项目必须存在启用成员；开放项目允许任意非空用户进入。
- 停用项目对外按不存在处理，返回 `404`；无成员权限返回 `403`。
- URL 与请求体项目冲突返回 `409`。
- Chat、Knowledge、Memory、Experience 的新项目路由都会先授权。
- Knowledge、Memory、Experience、工具缓存和异步任务均包含项目维度。

### 5.2 当前限制与待开发内容

- `user_id` 仍由调用方直接提交，尚未由 JWT 或可信身份系统解析；完整认证和 RBAC 属于 Phase 12。
- 当前知识库写接口只检查“是否是启用成员”，尚未限制为 `project_admin` 或 `support_staff`。因此它是基础成员隔离，不是最终管理权限模型。
- Project 配置存放在 JSON 中，运行期不可动态增删，修改后需重启。
- `ProjectConfig` 已进入请求上下文，但项目级 Prompt、模型、Agent、Skill、Tool 白名单尚未生效。
- 旧 Knowledge 元数据会在启动时归入 `default`；旧 Memory 与 Experience 数据没有自动迁移，需要另行设计迁移与回滚。
- 本次没有做跨进程锁、数据库行级安全、加密隔离或独立基础设施隔离。

### 5.3 实际执行的验证

本次在仓库中实际执行并通过：

- `python -m pytest tests/test_projects.py -q`：Project 模型、JSON 加载、开放/封闭项目、停用状态与成员权限用例通过。
- `python -m pytest tests/test_project_isolation.py -q`：覆盖工作记忆、Knowledge 向量/BM25、Experience、非法请求提前拒绝、URL 冲突、工具缓存和 Request/Context/Session 全链路传递。
- `python -m pytest -q`：全量回归通过；最终数量以本次提交前的命令输出为准。
- `python -m compileall -q agents api core experience mcp memory projects tests`：Python 编译检查通过。
- `git diff --check`：补丁空白字符检查通过。

尚未验证：连接真实 Redis/Chroma/Anthropic 的 Docker 端到端运行、已有生产数据迁移、并发压测、性能指标和真实业务收益。测试使用仓库中的 Fake/Mock 隔离外部依赖，因此不能把测试结果描述成生产验证。

## 6. 面试表达

### 6.1 可以自然复述的一段话

“这个系统原来主要按 user_id 和 conv_id 管理上下文，部署成多项目客服平台后，同名用户、相同会话号和相似知识都有串数据风险。我在 Phase 1 没有只给接口加一个 project_id，而是先建立 Project、ProjectMember、ProjectConfig 领域对象和统一授权服务，再让 project_id 从 API 入口贯穿 Request、ContextBundle、RetrievalSession 和 Tool。存储层也同步改造：Redis 键、Chroma 元数据与过滤条件、BM25 候选、工具缓存、异步任务和经验案例都带项目维度。新接口以 URL 中的项目为权威，冲突时返回 409；旧接口在未指定项目时兼容到 default。这样建立的是可验证的全链路作用域，而不是 Prompt 级的软约束。当前我也明确保留了边界：JWT、细粒度 RBAC、项目配置真正驱动 Agent，以及生产数据迁移仍是后续工作。”

### 6.2 可能的追问与回答要点

**追问 1：为什么入口授权后，存储层还要继续传 project_id？**

入口授权解决“能不能进入”，存储过滤解决“进去后能读写哪份数据”。内部代码可能被其他入口或后台任务复用，如果数据接口默认只按 user_id，就仍可能越界。两层约束属于纵深防御。

**追问 2：为什么不直接给每个项目建一套 Redis 和 Chroma？**

物理隔离安全边界更强，但连接、Collection、部署和监控成本也更高。Phase 1 先用显式作用域完成逻辑隔离，并保持接口可替换；如果项目安全等级或合规要求更高，可以在 Repository/Store 层演进到独立资源。

**追问 3：怎么证明不是只改了 API 字段？**

可以展示同一 `user_id + conv_id` 在两个项目生成不同 Redis 键，同一查询在 Knowledge 向量和 BM25 分支只返回本项目内容，以及同名用户的 Experience 无法跨项目读取。测试还检查 `Request → ContextBundle → RetrievalSession` 的项目字段和工具缓存分区。

**追问 4：为什么 URL 和 Body 冲突要返回 409，而不是选一个？**

URL 是新接口的资源边界。静默选择会掩盖客户端 Bug，也可能让审计记录与实际数据范围不一致。显式冲突错误能让调用方尽早修正，并保证服务端没有模糊解释。

**追问 5：这版离生产级多租户还差什么？**

主要还差可信身份来源、项目角色到操作权限的 RBAC、配置动态管理与审计、旧数据迁移与回滚、真实中间件端到端验证，以及视合规要求决定是否采用数据库行级安全或物理隔离。本次完成的是这些能力所依赖的 Project 全链路基础。
