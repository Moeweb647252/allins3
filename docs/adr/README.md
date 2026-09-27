# Architecture Decision Records

状态：生效中。对应任务 P0-06。更新日期：2026-09-27。

| 编号 | 标题 | 状态 |
| --- | --- | --- |
| [0001](0001-driver-api-boundary.md) | 对象 API 边界与模块依赖方向 | Accepted |
| [0002](0002-metadata-ownership.md) | 元数据所有权与单一提交协议 | Accepted |
| [0003](0003-staged-commit-outcome.md) | 分阶段提交与四态结果模型 | Accepted |
| [0004](0004-namespace-binding.md) | Bucket 到 namespace 的绑定 | Accepted |
| [0005](0005-toolchain-deps-ci.md) | Toolchain、依赖固定、CI 与字节上限 | Accepted |
| [0006](0006-config-defined-buckets.md) | 配置定义 bucket；生产状态存储待选型 | Accepted |
| [0007](0007-protocol-adapter.md) | 协议适配：封装 s3s 候选 | Accepted |
| [0008](0008-object-service-decomposition.md) | 对象服务的四个协作者拆分与缝隙保持内部 | Accepted |
| [0009](0009-service-decomposition-followups.md) | 对象服务拆分的两项后续：组合根与暂存账本 | Accepted |
| [0010](0010-capability-plan-drives-routing.md) | 能力规划驱动路由，并接入原生 multipart | Accepted |
| [0011](0011-driver-registration-and-native-route-coverage.md) | driver 注册以数据表表达，原生路由跨 HTTP 验证，双 feature 构建门 | Accepted |
| [0012](0012-per-instance-driver-configuration.md) | 每实例 driver 配置与受限阻塞执行器的所有权 | Accepted |
| [0013](0013-startup-recovery-and-readiness.md) | 启动恢复与 readiness 的顺序 | Accepted |
| [0014](0014-durable-multipart-coordinator-state.md) | multipart 协调器的 session/staging 持久化 | Accepted（取代 P4-01 偏离） |
| [0015](0015-local-filesystem-production-driver.md) | 本地文件系统作为第一个生产 driver | Accepted |
| [0016](0016-online-pending-completion-resolution.md) | 未决完成的在线解决 | Accepted |
| [0017](0017-read-only-yun139-driver.md) | 只读的 139 云盘 driver | Accepted |

格式：每条 ADR 记录背景、决策、后果与替代方案。修改已接受决策需新增 ADR 并在旧条目标注 Superseded。
