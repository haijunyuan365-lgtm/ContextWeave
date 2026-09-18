# Memory Evolution 使用与设计

> Phase 1 后所有工作记忆、情景记忆、画像快照和演化 worker 均以 `project_id + user_id`为边界；工作记忆再加 `conversation_id`。推荐使用 `/api/projects/{project_id}/memory/{user_id}` 系列调试入口，原 `/memory/{user_id}` 仅映射到 `default` 迁移项目。

## 从覆盖画像到维护记忆

```text
接收用户输入，分配稳定消息 ID 与用户内顺序
  -> Context Planner 与原有 Agent 回复流程
  -> 保存消息，同时将用户证据放入独立 Redis 待处理缓冲
  -> 显式偏好立即调度 / 普通消息累计 5 轮调度
  -> LLM 提取 MemoryCandidate
  -> 校验字段、来源、证据、置信度
  -> MemoryResolver：CREATE / UPDATE / MERGE / SKIP
  -> 原子 upsert 用户快照，再确认待处理消息
  -> 后续 Context Planner 预加载或 Agent 补查读取有效画像
```

提炼在后台执行，不等待它完成才回复。明确偏好也采用最终一致性：后台任务未完成时，下一轮仍可能读到旧画像。可通过 status 接口查看进度。一次请求已经生成的 Bundle 和请求内缓存保持快照语义，不在执行中替换。

## 字段与可信来源

首版只允许注册表中的字段，增加字段时需要同时定义类型、取值校验和证据规则。

| key | 类型 | 值 |
|---|---|---|
| `refund_method` | 单值偏好 | `alipay`、`wechat`、`bank_card`、`original_route` |
| `payment_method` | 单值偏好 | `alipay`、`wechat`、`bank_card` |
| `response_language` | 单值偏好 | `zh`、`en`、`ja` |
| `response_style` | 单值偏好 | `concise`、`detailed`、`step_by_step` |
| `contact_channel` | 单值偏好 | `email`、`phone`、`sms`、`in_app` |
| `common_products` | 稳定背景集合 | 有“常用、长期”等证据的产品原文名称 |
| `technical_environment` | 稳定背景集合 | 有长期使用证据的环境原文名称 |

中文别名会转换为规范值。集合按去除首尾空格和忽略大小写去重，保留首次写入的显示形式及成员来源。

候选引用单条真实用户消息，证据摘录必须是消息中的原文，最多 200 字。默认置信度至少 0.8。助手回复、旧摘要和工具返回不能作为直接来源。字段和值必须有对应证据；未知字段、无效来源、假设、第三方转述、低置信度和不受支持的信息以 SKIP 处理。

订单状态、退款结果、余额、物流进度、密码及验证码不会成为结构化记忆。历史交易行为不会自动推断成未来偏好。“我的退款到账了”与“以后退款优先支付宝”不同。现有工作记忆和情景摘要仍可能记录用户说过的业务内容，它们只是历史信息，不是当前业务状态的权威来源。

首版的文本校验偏保守，不保证覆盖所有自然语言表达。无法判断归属或修改目标时跳过，不猜测用户偏好。

## Resolver 与审计

| 动作 | 行为 |
|---|---|
| CREATE | 为新的有效字段建立稳定 ID 和版本 1 |
| UPDATE | 较新的单值表达替换旧值；记录前后值、版本、前后来源 |
| MERGE | 集合新增不同成员，保留原有成员的证据来源 |
| SKIP | 不改变有效值；保留固定原因，重复投递的已处理消息不重复提炼和记事件 |

例如：

1. “以后退款优先支付宝”创建 `refund_method=alipay`。
2. “以后退款优先微信”更新为 `wechat`，旧值和对应证据仍在事件中。
3. “请忘记我的退款方式偏好”将该字段设为 inactive，兼容读取也不会把旧画像中的该偏好重新补回。
4. 后续重新表达明确偏好可激活该字段并增加版本。

“以后改为微信”只在现有记录或本批较早用户消息能唯一确定目标字段时接受；同时存在多个可能字段时跳过。指明撤回某个旧值时，必须与当前值相符。集合成员的单独撤回不在首版范围内，不从模糊表达推断删除操作。

冲突使用接收输入时分配的用户内顺序，不使用模型完成时间。即使新消息只是确认同一个值，也会推进字段顺序标记，阻止较早的冲突结果晚到后覆盖它。相同来源对同一字段提出互相矛盾的候选时跳过，不依赖模型输出顺序选一个。

## 存储、恢复与并发边界

- Redis 工作记忆仍保留原有 TTL；新消息附带稳定 ID 和顺序。旧消息仍可读，使用 `legacy:` 前缀的确定性标识，不声称恢复了原本不存在的来源 ID。
- 用户消息与独立待处理 Hash 在同一次 Redis 事务中写入。待处理消息没有工作记忆的 24 小时 TTL；已确认消息从缓冲移除。
- Chroma 新增 `user_memory` 集合，每个用户一个 JSON 快照，保存记录、演化事件、处理检查点和版本。使用按 ID 读取和单文档 upsert，不调用 embedding 模型提炼画像。
- 写入快照成功后才从 Redis 确认消息。确认失败或进程在确认前退出时，重启通过已处理来源 ID 清理重复投递，不再次演化。
- 同用户后台任务合并，用户级锁覆盖读取、提炼、合并和提交；不同用户可并行。
- 默认每批最多 20 条，普通消息不足 5 条继续等待。启动时扫描已登记用户并恢复任务；关闭时等待最多 10 秒，剩余任务取消且未确认内容保留。
- 提炼超时、模型异常或整体输出结构无效时，保留本批消息，等待新消息触发或服务重启重试；不进行无限自动重试。格式正确但被语义校验拒绝的候选属于 SKIP，该批仍可确认。

这些锁只保障同一个进程中的 MemoryManager。现有默认 Uvicorn 单进程适用；多 worker、多个服务实例或多个 CLI 同时写同一用户尚不受支持，需要后续引入跨进程协调与版本条件写入。Redis 数据本身需要保留，当前 Docker Compose 已配置 AOF；手动清空 Redis 会丢失待处理消息和顺序计数，不属于正常重启恢复。

快照的来源 ID 与事件当前不自动裁剪，也没有历史归档策略。它适合当前项目规模；长期大规模运行应另外设计审计保留与分页存储，不应将当前版本宣称为无限容量记忆库。

工作记忆压缩按会话加锁。先生成摘要并成功归档，再使用 Redis 事务写摘要和 LTRIM 保留最近 5 条，避免原实现重建列表时颠倒顺序或删除并发追加。消息 ID 原样保留；摘要或归档失败不裁剪。待处理用户证据独立存在，不受压缩影响。

## 旧画像兼容

旧 `user_profile` 集合不删除、不覆盖、不批量迁移。读取时只使用可确定映射的注册字段和明确偏好文本，标为 `legacy` 来源，原始消息 ID 为 null。

新结构化字段优先；inactive 字段阻止旧值复活。无法可靠映射的旧文本仅在检查接口的 `legacy_profile` 中保留，不混入 Agent 的有效画像。

`get_profile()` 仍返回字典，包含 `preferences`、`entities` 和 `sources`；没有有效信息时返回 `{}`。ContextBundle 和现有 `get_user_profile` 工具保持接入。`update_profile()` 兼容入口仅调度已有待处理证据，不再调用模型重写旧画像。

## 配置

| 环境变量 | 默认值 | 用途 |
|---|---|---|
| `ECHOMIND_MEMORY_EVOLUTION_ENABLED` | `true` | 启停后台提炼，关闭后仍保留既有记忆和待处理证据 |
| `ECHOMIND_MEMORY_MODEL` | `ANTHROPIC_MODEL` | 单独指定候选提取模型，连接配置沿用现有服务 |
| `ECHOMIND_MEMORY_BATCH_TURNS` | `5` | 普通新增用户消息的批量阈值，1–20 |
| `ECHOMIND_MEMORY_CONFIDENCE_THRESHOLD` | `0.8` | 候选最低置信度，0–1 |
| `ECHOMIND_MEMORY_TIMEOUT_SECONDS` | `10` | 每批单次提炼超时，SDK 自动重试关闭 |

这些配置不改变 Context Planner、意图识别或 RAG 的调用设置。显式偏好触发词仅决定何时尝试提炼，不保证一定会写入记忆。CLI 输入通过后台线程等待，用户停留在输入提示时记忆任务也能运行。

## 演示与只读接口

依次发送以下 `/chat` 请求，每次用 status 接口确认后台完成，再进行下一步：

```json
{"user_id":"memory-demo","conv_id":"demo-1","message":"以后退款优先支付宝"}
```

```json
{"user_id":"memory-demo","conv_id":"demo-1","message":"以后退款优先微信"}
```

```json
{"user_id":"memory-demo","conv_id":"demo-1","message":"请忘记我的退款方式偏好"}
```

查看状态及结果：

```text
GET /memory/memory-demo/status
GET /memory/memory-demo
GET /memory/memory-demo/events?limit=50
```

`status` 返回 `enabled`、`running`、`pending_count`、`last_success_at`、`last_error_category`。普通消息不足阈值时，`pending_count > 0` 且 `running=false` 是正常等待，不代表失败。

检查接口返回结构化记录、版本、来源和有效画像，事件接口按事件时间倒序返回，limit 限制为 1–200。返回相关证据摘录，不返回完整对话历史或待处理消息正文。它们是内部只读调试接口，本次未新增身份认证或用户权限系统，也没有写入或删除 HTTP 接口。

新增事件日志只记录操作、字段、来源 ID、版本、固定原因；错误仅记录异常类别，不打印模型输入、候选值或消息正文。

## 测试

```powershell
$env:PYTHONUTF8 = '1'
.\.venv-win\Scripts\python.exe -m pytest tests -q
```

测试覆盖候选证据校验、来源排序、更新与集合合并、撤回、旧画像兼容、触发降频、请求隔离、写入和确认失败、重启恢复、关闭取消、压缩期间追加、HTTP/CLI 接入，以及 Context Planner 回归。

自动化测试使用模拟模型、Redis 和 Chroma。真实模型、Redis、Chroma 联调需单独验证；此前本机 ONNX DLL 加载问题不由该改造处理。没有批量迁移命令，也不恢复工作区已有的 wiki 删除。
