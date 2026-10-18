# 合并进度与恢复入口

更新：2026-10-18T18:18:05+08:00

当前：M0—M5 代码合并、离线验收与 GitHub 交付完成。

| 阶段 | 状态 | 检查点提交 |
| --- | --- | --- |
| M0 基线与保护 | 完成 | 8bfea1d |
| M1 目录与基础模块 | 完成 | 1b55a3d |
| M2 核心业务融合 | 完成 | b48b117 |
| M3 评测、观测与权限 | 完成 | aba8d9c |
| M4 部署与文档 | 完成 | 9b2c727 |
| M5 联合验收与发布 | 完成 | affc99f |

发布验收提交：`affc99fd4c04751f32dc146b04db649a8c75f945`，已正常推送并通过 ls-remote 核验。之后的日志收尾提交只更新交付状态，也同步到 trace；最终 SHA 以 `git rev-parse HEAD` 和远端 trace 为准。

GitHub 提示原 echoMind 仓库已迁移至 `haijunyuan365-lgtm/ContextWeave`，本次使用原 origin 地址正常重定向推送；未擅自修改 origin 配置。

验证摘要：392 passed、1 warning；pip check、74 Python AST、文档链接/锚点/代码与三个离线实验、两套 Compose 解析均通过。详情见 verification.md。

尚未完成的真实环境验收：Docker Desktop Linux Engine 返回 HTTP 500，本机 Redis/Chroma/OTLP/Prometheus/Grafana 端口不可用；没有运行真实计费模型、在线硅基向量或远程 Microsoft Learn MCP。离线通过不等于这些服务联调通过。

继续真实环境验收时，从 M5.md 的环境限制及 verification.md 开始；先恢复 Docker/服务并核对当前分支、HEAD、工作区，不重做已完成合并。已有密钥继续使用本地配置，禁止写入日志。

恢复保护引用：`codex/backup-trace-before-upstream-20261018` → `56e9671`。附件仍只读，输入清单见 source-manifest.json。发现用户新增修改时保留并整合，不直接重置工作区。
