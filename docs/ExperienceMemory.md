# Experience Memory 使用与设计

Experience Memory 复用同一项目、同一用户过去成功请求的处理过程，帮助 Agent 排障和选择检查步骤。它不会修改路由、执行历史工具操作，也不证明某个订单现在已退款或某个账户现在可登录。Phase 9 才会将它改为项目内跨用户共享。

## 执行链路

```text
HTTP / CLI → ContextPlanner → RetrievalSession → ContextBundle → Agent / 工具补查
                                                                   ↓
                  Redis 案例快照 + pending 任务 ← 执行状态 / 工具结果 / 知识 / 回答
                               ↓
                     后台 LLMJudge（工具和知识作为背景）
                               ↓
                   准入 → Chroma 向量写入 → Redis active
                               ↓
                 后续请求按用户检索 → Redis 状态复核 → 参考经验
```

关键实现入口是 `experience/runtime.py`、`core/context_runtime.py`、`agents/agent_orchestrator.py`。HTTP 与 CLI 都调用 `ContextPipeline.capture()`。上下文检索经 `RetrievalSession` 合并同轮相同请求，Agent 也可用 `search_experience` 补查。

### 案例与准入

`ExperienceCase` 包含完整 UUID 案例 ID、request/user/conversation ID、问题、意图、实体、实际参与 Agent、工具输入及结果摘要、知识证据、最终回答、执行状态、评分、反馈、尝试次数和 UTC 时间。案例 ID 根据用户和完整请求 UUID 确定性生成，同一请求重复提交不会覆盖已有案例或重复评分。

工具结果保存在独立的请求级采集字段中，不扩展公开工具 trace 的正文。每次 Agent 调用的工具按局部序号记录，保留 Agent 标识；并行 Agent 之间不声称存在全局执行顺序。知识证据包括预加载和运行时补查结果。

凭证字段如 password/token/secret/authorization/cookie/api_key 被过滤，常见内嵌凭证形式也被脱敏；字符串、列表、字典及嵌套深度有限制。这是凭证过滤，不是对任意自然语言个人信息的完整匿名化。案例不会跨用户共享。供 Agent 使用的投影只含问题、Agent、工具步骤和回答摘要，没有原始工具结果。

不准入：问候、正面反馈意图、澄清、转人工、空回答、执行失败、降级、并行部分失败、工具失败、知识检索失败或降级。编排器保留最初失败和后续降级记录，不能因最终有文字回答而当成成功案例。

候选案例由现有 `LLMJudge` 评分，必须完整返回 relevance/accuracy/completeness/helpfulness 四项有限数值，范围 `[0,1]`。平均值至少 0.8 且 accuracy 至少 0.8 才准入。格式错误、超时、服务错误和索引写入错误最多尝试 3 次；关闭 SDK 自动重试，避免层叠重试。

| 状态 | 含义 |
| --- | --- |
| pending | 待评分或等待重试，不能检索 |
| active | 评分达标且向量写入完成，可以复用 |
| rejected | 执行门槛或评分未达标，不复用 |
| failed | 已用尽处理尝试，不复用 |
| revoked | 收到负反馈，立即停止后续复用 |

正反馈只记录，不跳过评分，也不恢复已撤销案例。负反馈与后台激活通过同一进程内锁协调；后台处理结束时重新读取状态，避免覆盖撤销。

### 存储、恢复与故障边界

- Redis `experience:v2:cases:<project+user hash>` 保存权威案例；`experience:v2:pending` 保存待处理案例到项目/用户的映射。
- Chroma 独立 collection `experience_cases_v2` 使用 `project_id + user_id` 过滤和 cosine 距离。
- 默认一个后台消费者。启动和运行中扫描 pending 任务；终态成功持久化后才移除任务。进程在任意步骤停止都可在启动后恢复；重复索引写入使用 upsert。
- 关机取消消费者，未完成任务保留在 Redis。最后一次尝试被中断后，恢复时会保守标记 failed，不额外突破三次上限。
- 索引可能残留 revoked 或未激活条目，因此检索时按用户过滤、超额召回，再到 Redis 核验 active 状态。请求内缓存命中也重新核验；正在执行且已获得参考的模型请求不会被追溯取消。
- 前台只等待限时的 Redis 快照提交，不等待评分或 embedding。经验初始化、读取、写入失败不阻断聊天；初始化失败后本次进程关闭经验功能，恢复后重启应用。
- 第一版要求单应用进程（不要启用多个 Uvicorn workers，或让 HTTP 与 CLI 进程同时消费同一经验库）。现有 Docker 启动方式为单进程。多实例需要额外的分布式任务认领与原子状态转换。
- 案例随 Redis 持久化，Redis AOF/卷配置沿用现有 Compose。当前不自动清理历史案例，也不自动从已有对话回填经验。

## 规划与配置

技术故障、支付异常、明确参考上次处理方法时规则触发经验；普通状态查询、问候、正面反馈和人工转接不触发。复杂请求允许 LLM 判断经验需求；查询文本仍由确定性逻辑生成，不接受模型编造业务标识。关闭功能时不允许 LLM 打开经验来源。

默认 `user_id=anonymous`（忽略两端空白及大小写）不采集、不检索。用户 ID 由现有调用方提供，没有新增认证，因此这些调试接口应当与现有 Memory 接口一样在受控环境使用。

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| ECHOMIND_EXPERIENCE_ENABLED | true | false/0/no 关闭功能 |
| ECHOMIND_EXPERIENCE_MIN_SCORE | 0.8 | 四项平均分下限 |
| ECHOMIND_EXPERIENCE_MIN_ACCURACY | 0.8 | 准确性下限 |
| ECHOMIND_EXPERIENCE_MIN_SIMILARITY | 0.7 | cosine 相似度下限 |
| ECHOMIND_EXPERIENCE_TOP_K | 3 | 返回数量，1–3 |
| ECHOMIND_EXPERIENCE_TIMEOUT_SECONDS | 20 | 单次 Judge / 向量写入超时 |
| ECHOMIND_EXPERIENCE_IO_TIMEOUT_SECONDS | 3 | 前台提交、检索与接口存储超时 |
| ECHOMIND_EXPERIENCE_MAX_PROMPT_CHARS | 3000 | 经验提示词字符上限，含参考声明 |

Judge 复用当前模型配置；embedding 复用 Chroma 默认 embedding function，生产镜像已有模型缓存。本地运行可能需要首次下载模型。配置非法时采用默认值。关闭并重启不会删除原案例；再次开启后恢复未完成任务。

经验提示词按完整 JSON 条目装入预算，不截断 JSON。没有条目能放入时不注入。相似度不是业务成功概率；0.7 是初始阈值，需要用实际中文领域数据与 embedding 模型校准。

## 接口与演示

`GET /experience/{user_id}/cases?status=active&limit=20` 返回 `items`，每项包含 case_id、request_id、query 摘要、status、reason、judge_score、user_feedback、attempts、created_at、updated_at。status 可省略；limit 范围 1–100。

`POST /experience/{user_id}/cases/{case_id}/feedback` 接受 `{"feedback":"positive"}` 或 `{"feedback":"negative"}`。不存在或不属于该用户返回 404，非法参数 422，经验关闭或存储不可用返回 503；成功返回案例 ID、状态与反馈。重复相同反馈不更新时间。

下面在已启动且模型、Redis、Chroma 可用的服务上运行（PowerShell）：

```powershell
$api = 'http://localhost:8000'
$body = @{ user_id = 'experience-demo'; message = 'OAuth 登录报 401，应该怎么排查？'; debug_context = $true } | ConvertTo-Json
$first = Invoke-RestMethod "$api/chat" -Method Post -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($body))
Invoke-RestMethod "$api/experience/experience-demo/cases"
```

等待该 request_id 的案例从 pending 变为 active。若 rejected/failed，查看 reason、judge_score 和 attempts；不要把未达标案例手工强制激活。空库首次经验检索为 empty 属于正常冷启动。

```powershell
$body = @{ user_id = 'experience-demo'; message = 'OAuth 登录失败，提示 401，怎么处理？'; debug_context = $true } | ConvertTo-Json
$second = Invoke-RestMethod "$api/chat" -Method Post -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($body))
$second.context_debug.sources.experience
# 不提供 conv_id，以上请求自动使用新会话。查看 count、case_ids、cache_hits。
$rows = (Invoke-RestMethod "$api/experience/experience-demo/cases?status=active").items
$case = $rows | Where-Object { $_.request_id -eq $first.request_id } | Select-Object -First 1
Invoke-RestMethod "$api/experience/experience-demo/cases/$($case.case_id)/feedback" -Method Post -ContentType 'application/json' -Body '{"feedback":"negative"}'
$third = Invoke-RestMethod "$api/chat" -Method Post -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($body))
$third.context_debug.sources.experience
```

第三次请求的 case_ids 不应包含已撤销案例。第二次请求本身也可能生成新案例，因此第三次不一定完全没有结果。更换 user_id 不会召回原用户案例。实际模型可能不达评分门槛，语义召回也可能低于阈值，这不是程序可承诺的固定输出。

无需服务或模型下载的确定性闭环演示及回归测试：

```powershell
.\.venv-win\Scripts\python.exe -m pytest tests/test_experience.py::test_complete_lifecycle_and_cached_revocation -q -s
.\.venv-win\Scripts\python.exe -m pytest tests -q
```

演示打印首次 active、新会话命中数量、负反馈后命中数量。测试使用 fake Judge/Redis/Chroma，验证行为与隔离，不用它宣称真实模型准确率提升。

## 面试表达与局限

可以描述为“Retrieval-Augmented Experience”：先记录可追踪执行案例，以执行状态和 Judge 双重门槛筛选，再按需召回给 Agent。经验不参与路由，负反馈可撤销，权威状态与向量索引分离，避免失败经验直接控制策略。

Judge 高分不等于用户问题已实际解决；当前没有业务 Outcome 验证。系统没有训练模型、自动修改 Skill 或路由策略，也没有实现跨用户经验共享。后续提升应以人工校准、真实业务结果及授权体系为基础。
