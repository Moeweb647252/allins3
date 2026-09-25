# Architecture Decision Records

状态：生效中。对应任务 P0-06。更新日期：2026-09-25。

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

格式：每条 ADR 记录背景、决策、后果与替代方案。修改已接受决策需新增 ADR 并在旧条目标注 Superseded。
