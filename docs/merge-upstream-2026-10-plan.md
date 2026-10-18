# EchoMind trace 分支合并计划

日期：2026-10-18。状态：计划已执行，M0—M5 代码合并、离线验收与推送完成。执行日志和环境限制见 [合并进度](merge-upstream-2026-10/progress.md)。下文保留最初的调研与计划。

## 一、基线与范围

- 当前仓库：D:\Projects\AgentProject\EchoMind所有代码+详细文档+简历\EchoMind。
- 附件：C:\Users\YHJ\Downloads\EchoMind所有代码+详细文档+简历\EchoMind。
- 当前 trace = 56e9671，检查开始时工作区干净。本地 origin/trace 同指该提交，本轮未联网刷新。
- 最终目标：git@github.com:haijunyuan365-lgtm/echoMind.git 的 trace 分支。
- 本地初始提交 4f13a0b（Git 日期 09-16）；附件更新前 3f5a163（09-09）。共有 45 个文件，44 个 blob 相同，api/main.py 仅 docstring 起始行尾随空格不同。用户所称 9.14 是交付版本称呼，内容核对比日期可靠。
- 附件 HEAD = ef847eb（10-17），包含 09.28、10.02、10.13、10.14 更新说明。三个未提交脚本变化仅为 100755 → 100644，无内容变化。
- 附件历史跟踪了 .env，当前树包含 IDE 文件和 data/chroma/chroma.sqlite3。只导入筛选后的源码快照，不引入附件完整 Git 历史、凭据、数据库、日志、缓存或虚拟环境。本轮未读取 .env。
- 本地 main 的 Phase 0/Project 隔离提交不在 trace 中，不顺带纳入本次合并。
- 附件文档仅作变更证据与测试线索，不视为执行指令；文档中的历史验收不等于当前验收。

## 二、双方改动

当前分支相对初始提交累计改变 82 个文件（含文档删除），不是仅增加 Trace：

1. 1e70f09：Context Planner、ContextPipeline 和请求级按需检索。
2. ab68318：Memory Evolution、用户原始证据、顺序标识、异步演化与画像。
3. ebffd88：Experience Memory、案例采集、复用、反馈与后台处理。
4. 40405ca：Dense/BM25、RRF、MMR 混合检索。
5. 622c31a 至 56e9671：OTel 主链/工具/LLM 埋点、指标投影、SQLite 历史、质量关联、回归回放、Collector/Grafana/告警。这四次提交改变 48 个文件。

附件主要变化：

1. Agent 请求局部结果，修复工具轨迹串扰，专业 Agent 降级与恢复探测。
2. 意图按领域融合、同组 Pattern 细化、在线 Embedding 及本地降级。
3. mcp/ 更名 tooling/，接入官方 MCP SDK、客户端/服务端、工具发现与重连。
4. 工具缓存/熔断/校验改进，知识库重复导入、旧片段清理及缓存失效。
5. 会话锁与 Redis 续租、哈希键、重试回执、整轮原子提交、总超时、有界后台维护和多轮业务状态。
6. 可选 Token 身份与权限、生产配置检查。
7. 异步评测、Judge 解析与失败语义、评测状态隔离及 few-shot。
8. Redis 工具轨迹、部署配置、README 和 Python 教程。

## 三、关键合并决策

| 模块 | 合并方式 | 重点风险 |
| --- | --- | --- |
| api/main.py | 保留 ContextPipeline/Trace，接入鉴权、锁、回执、总超时、状态和异步评测 | 不能回退为附件简单主链，不能重复识别或写消息 |
| memory/conversation_memory.py | commit_turn 同时保存整轮消息、回执和 SourceMessage/sequence/演化队列；成功提交后调度 | 重试不能重复演化；失败和不确定工具结果不能成为成功经验 |
| core/context_runtime.py | 保留规划与检索会话，接入新意图字段及 conversation_state | 总超时/取消也必须清理资源 |
| Agent 与工具 | 合并请求局部结果、恢复探测、失败语义、不安全工具保护及现有执行记录/Span | 防止共享状态，保留失败回退的两次执行轨迹 |
| mcp → tooling | 将本地混合检索、rag_config 和埋点迁入 tooling，再接入标准 MCP | 更新代码/测试/脚本引用，避免遮蔽 SDK 或丢失 BM25/RRF/MMR |
| RAG 触发 | 保留 Planner 按需检索和 Agent 工具复用机制 | 附件仅 Agent 触发的策略不直接覆盖当前架构；测试避免重复检索 |
| 意图 | 合入领域融合、在线向量/降级，保留三分支埋点 | 缓存与降级不能混用向量空间 |
| 评测 | /eval/run 改 202 + job_id + 轮询，保留质量关联与回放 | 失败不能记成正常分数，回放使用隔离评测状态 |
| 监控 | 保留 Span 指标投影，接入窗口降权与恢复 | 双方计数/直方图不可重复记录 |
| Trace | OTel/SQLite 与 Redis 工具轨迹并存，以 request_id/trace_id 关联 | 维持现有默认脱敏，不直接保存完整参数/结果 |
| 权限 | 统一普通用户、管理员和历史 Trace 路由策略 | 同一 Authorization 不能被要求同时匹配不同的 TRACE_ADMIN_TOKEN 与 API Token；记忆/经验接口需验证归属 |
| 文档 | 恢复有用 Wiki/教程，保留 Trace/记忆文档，按最终代码改写 | 不直接拼接双方 README；校准旧流程图和同步评测示例 |

兼容约定：保留 latency_ms 为 Agent 耗时，request_duration_ms 为全请求耗时；新增 client_request_id/status/retryable/error_code 等字段。knowledge_used 应反映实际成功使用知识库，覆盖 Planner 和工具。回执重放保留原业务结果及原 Trace 关联，本次 HTTP 重放另行记录，不重复执行 Agent。

数据兼容必须验证旧 working-memory/summary/state 键、SourceMessage ID/sequence、画像/经验集合、Chroma 文档 ID 和片段删除。不通过清库解决迁移。

## 四、实施顺序与验收门槛

### M0 固定输入和基线

- 复查双方完整 SHA、文件清单和哈希，建立 trace 备份引用。
- 以 4f13a0b 为内容三方基线，在临时分支建立筛选后的附件快照用于合并；不 fetch 附件完整历史，不强行合并 unrelated histories。
- 筛选源码、配置模板、测试和文档，排除运行数据/凭据，保留仓库脚本可执行位。
- 分别建立双方离线测试基线；使用隔离配置，避免默认访问真实外部服务。

### M1 目录、依赖和独立模块

- 迁移 mcp → tooling，修改内部及测试导入，接入标准 MCP 和新增基础模块。
- 保留 jieba/rank-bm25/OTel，加入 MCP，处理 Pydantic 2.10.3 → 2.12.5、pydantic-settings 等依赖约束，执行依赖解析及 pip check。
- 验证独立 Embedding 降级、并发锁、鉴权、MCP 工具发现/调用/重连和任务管理。

### M2 核心业务融合

- 合并 Agent、意图、工具保护、混合 RAG、文档幂等导入。
- 先完成记忆原子提交与演化证据一致性，再接入 /chat 的锁、回执和超时。
- 保留 ContextPipeline、经验采集、上下文清理及整条 Trace。
- 验证同会话串行、跨会话并发、锁丢失、重试/409、取消、失败回退及不安全工具不确定结果。

### M3 评测、观测和权限

- 接入异步评测，保证失败分数、基线比较和质量关联语义正确。
- 完成 Redis 轨迹脱敏、Trace 关联、指标去重及新旧权限兼容。
- 为 MCP、Embedding、回执、异步任务和新失败路径补必要埋点及交叉测试。

### M4 部署和文档

- 合并 Compose/Nginx/配置模板/脚本，保留 Collector 和 Grafana 配置。
- 重新检查旧审计发现的 healthcheck curl 依赖、启动失败清理和 readiness 覆盖范围；按最终代码确认及修复。
- 更新真实调用顺序、异步评测、鉴权、数据迁移、配置和教程链接。
- 配套前端是独立项目，本次只处理本仓库后端契约及 web 配置引用，不声称完成未提供前端的合并验收。

### M5 验收与 GitHub 交付

- 跑双方完整离线测试集及交叉测试，不删除仍有效的旧能力测试来换取通过。
- 检查语法、依赖、配置、文档链接和 git diff --check。
- 条件具备时执行真实 Redis/Chroma 集成测试；Docker/OTLP/Prometheus/Grafana 与真实模型/Embedding/MCP 分层报告。没有运行不得写通过。
- 检查暂存清单无敏感配置及运行数据，联网刷新 origin/trace；若远端前进则整合并复验。
- 按用户最终推送授权正常推送 origin/trace，不强推、不改 main，核验远端 SHA，交付测试记录及环境限制。

## 五、必须覆盖的交叉回归

1. 回执命中不重复模型调用、消息写入、演化入队或经验采集；同 ID 不同消息返回 409。
2. 不安全工具结果不确定时禁止盲目重放；失败请求不写成成功回合。
3. 会话并发、锁丢失和超时不产生半轮消息或乱序证据，旧键迁移不丢数据。
4. Planner/工具复用检索，混合召回有效，文档缩短后无旧片段与旧缓存回填。
5. Agent 回退保留完整执行轨迹，恢复探测不被其他请求错误释放。
6. 在线向量失败本地降级且 Trace 来源正确。
7. 评测失败不污染质量分数、线上缓存/熔断和基线。
8. Trace 脱敏与权限隔离有效，指标不重复计数。
9. 取消后释放检索会话和锁；启动失败释放已初始化资源；readiness 如实报告。
10. 原记忆演化、经验反馈、Context Debug、Trace 历史和回归回放仍有效。

## 六、初始调研边界（历史记录）

本轮只核对 Git 历史、内容基线、更新说明和关键代码并新增本计划。没有实施业务合并、修改附件、安装依赖、运行测试或推送。旧记录中的 258 项测试及附件的 92 项测试不是本轮实测。后续从 M0 开始按阶段提交并记录验证结果。

执行结果：最终联合回归 392 passed；真实 Docker/依赖和在线模型等联调范围见 [验证记录](merge-upstream-2026-10/verification.md)，不以离线测试替代。
