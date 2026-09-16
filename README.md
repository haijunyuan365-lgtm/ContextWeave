# EchoMind

EchoMind 是一个面向客服/运营场景的多 Agent 智能系统。它不是单纯的聊天机器人，而是把以下能力串成闭环：

- 细粒度意图识别
- 规则优先、LLM 兜底的 Context Planner
- 路由驱动的多 Agent 编排
- 意图驱动 RAG 检索
- Redis + ChromaDB 分层记忆
- 动态 Skills 注入
- 在线监控与路由降权
- LLM-as-Judge 端到端评测

## 你可以先看什么

- [技术亮点](wiki/技术亮点.md)
- [重点代码](wiki/重点代码.md)
- [业务流程说明](wiki/业务流程说明.md)
- [完整使用指南](wiki/完整使用指南.md)

## 快速开始

### 1. 准备环境

- Docker
- Docker Compose
- `ANTHROPIC_API_KEY`

如果使用兼容 Anthropic 协议的第三方模型服务，也可以配置：

```env
ANTHROPIC_BASE_URL=https://api.deepseek.com/anthropic
ANTHROPIC_MODEL=deepseek-v4-pro
ANTHROPIC_API_KEY=your_key
```

### 2. 配置环境变量

复制示例配置：

```bash
cp .env.example .env
```

最少确认这些变量可用：

```env
ANTHROPIC_API_KEY=your_api_key
REDIS_PASSWORD=echomind123
```

### 3. 启动服务

推荐直接启动全栈：

```bash
docker compose up -d --build
```

查看状态：

```bash
docker compose ps
```

看日志：

```bash
docker compose logs -f echomind
```

### 4. 访问入口

- API: `http://localhost:8000`
- Swagger: `http://localhost:8000/docs`
- Nginx: `http://localhost`
- Health: `http://localhost:8000/health`

## 核心功能

### 对话主链路

`POST /chat`

流程是：

```text
轻量上下文 -> 意图识别 -> Context Planner -> 按需检索 -> ContextBundle
  -> Agent 路由 -> 回复生成 / 运行时补查 -> 写回记忆
```

最近 5 条消息和最多 800 字摘要参与意图识别，再规划是否读取情景记忆、画像、知识和 Skill。
同轮预检索和 Agent 补查共享缓存；多个 Agent 并发查询相同来源、query 和参数时只执行一次。
Experience 仅预留接口；画像更新与记忆压缩仍沿用原流程，后台更新不受读取规划限制。

调试请求示例：

```json
{
  "message": "退款多久到账？",
  "user_id": "demo-user",
  "conv_id": "demo-session",
  "debug_context": true
}
```

返回的 `context_debug` 展示规划来源、理由、各来源状态、数量、耗时和缓存命中次数，不返回上下文正文。
默认不输出该字段。`knowledge_used` 表示有效知识已提供给 Agent；预检索不计入 `tools_used`。

Planner 配置（可选）：

```env
ECHOMIND_PLANNER_LLM_ENABLED=true
# 留空时复用 ANTHROPIC_MODEL
ECHOMIND_PLANNER_MODEL=
ECHOMIND_PLANNER_CONFIDENCE_THRESHOLD=0.7
ECHOMIND_PLANNER_TIMEOUT_SECONDS=5
```

完整设计、演示场景和兼容边界见 [Context Planner 使用与设计](wiki/ContextPlanner.md)。

### 知识库

- `POST /search`
- `POST /knowledge/add`
- `POST /knowledge/upload`
- `GET /knowledge/stats`

### Skills

- `GET /skills`
- `POST /skills/reload`

### 监控与评测

- `GET /monitor`
- `POST /eval/run`

## 项目结构

```text
api/main.py                  FastAPI 入口
agents/agent_orchestrator.py 多 Agent 编排
core/intent_recognizer.py    三路融合意图识别
core/context_planner.py      规则与 LLM 上下文规划
core/context_runtime.py      Bundle、请求内检索缓存和共享准备流程
core/skill_loader.py         动态 Skills 加载
memory/conversation_memory.py  Redis + ChromaDB 记忆
mcp/tool_manager.py          工具层、缓存、熔断、重排
mcp/knowledge_base.py        ChromaDB 知识库
monitor/performance_monitor.py 在线监控
evaluation/evaluator.py      端到端评测
wiki/                       详细文档
skills/                     动态业务规则
data/                       持久化数据
```

## 运行时架构

```text
用户请求
  -> /chat
  -> MemoryManager 只读取近期消息与会话摘要
  -> IntentRecognizer 输出 intent / intent_group / urgency / entities
  -> Context Planner 决定记忆 / 画像 / 知识 / Skill 的预加载
  -> Selective Retrieval + ContextBundle
  -> AgentOrchestrator 路由到 General / Technical / Billing / Escalation
  -> 按角色注入 Skills、知识/记忆/画像运行时补查、回复生成
  -> 写回 Redis 和 ChromaDB
  -> Monitor 采集在线指标
  -> Evaluator 做意图识别和回复质量评测
```

## 主要端口

| 服务 | 端口 |
|---|---:|
| EchoMind API | 8000 |
| ChromaDB | 8001 |
| Redis | 6379 |
| Prometheus | 9090 |
| Nginx | 80 |

## 开发和调试

常用顺序：

```text
1. /health
2. /chat
3. /skills
4. /monitor
5. /eval/run
```

如果你只想看项目怎么工作，直接读：

- [EchoMind定位与技术亮点](wiki/EchoMind定位与技术亮点.md)
- [技术亮点](wiki/技术亮点.md)
- [重点代码](wiki/重点代码.md)

## 一句话概括

EchoMind 是一个可观测、可评测、可降级的多 Agent 客服运行时。
