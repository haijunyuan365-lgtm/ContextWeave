# 验证记录

各项结果以本轮实际运行输出为准。离线与真实服务验证分开记录。

## M0

- 当前分支：258 passed, 1 warning in 13.40s。
- 附件原样：5 collection errors；mcp 缺失、onnxruntime DLL 不可用。未计为通过。
- Python 3.10.9；附件 asyncio.timeout 需兼容或升级解释器，生产镜像使用 Python 3.12。

## M1

- pip check：No broken requirements found。
- concurrency/eval jobs/intent embedding：20 passed。
- 标准 MCP：7 passed，1 failed（依赖 M2 cache_versions 与 AgentCallResult 融合）；不删除失败用例。

- 原集与新增基础测试：277 passed，1 failed（AST 检查目录仍为 mcp）；已改为 tooling，针对失败项重跑 1 passed。

## M2

- 核心选择集：157 passed / 1 failed / 2 deselected；修复旧 mock 的 get_trace 协议后针对失败项 1 passed。
- 测试覆盖真实本地 MCP stdio 往返（fixture 服务）、fakeredis 锁/事务/回执和原有 Context/Memory/Trace。
- 两项异步评测/监控检查留待 M3，不宣称全量通过。

## M3

完整联合回归：389 passed, 1 warning in 20.01s。包含五项新增跨分支回归。标准 MCP 使用本地 SDK fixture 服务；fakeredis 是协议模拟，不代表真实 Redis/Chroma 部署验收。

## M4

- 391 passed, 1 warning in 19.78s。
- 教程检查 --run-offline：15 篇 / 281 文件链接 / 143 锚点 / 47 Shell / 9 Python / 34 JSON / 6 选择器 / 3 实验全部通过。
- 72 个 Python 模块 AST；pip check 通过。
- 两组 Compose config --quiet 均通过；不是部署验收。
- Docker info：HTTP 500；本机 Redis/Chroma/OTLP/Prometheus/Grafana 端口不可用。
