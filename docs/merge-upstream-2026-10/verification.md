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
