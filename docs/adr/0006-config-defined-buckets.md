# ADR-0006：配置定义 bucket；生产状态存储待选型

状态：Accepted。日期：2026-09-22。

## 背景

规范要求先定义通用接口、暂不限定生产后端。若首期就实现 `CreateBucket`/`DeleteBucket` 或选定某个数据库作为 multipart/staging 状态存储，会把尚未稳定的接口与某一种持久化方案绑定。

## 决策

1. 首期 bucket 由**只读配置**预先绑定到已有 namespace；`CreateBucket`/`DeleteBucket` 推迟，需先定义 namespace provisioning 与删除策略。
2. `BucketCatalog` 首期可从只读配置加载；它只保存 bucket 名、所有者、region、binding、配置版本，**不**保存对象内容或对象元数据的第二份权威副本。
3. `MultipartStore` 与 `StagingStore` 是独立接口，具体持久化实现待后端选择时确定。引用任意外部数据库或远端服务都不是默认前提。
4. 生产模式必须拒绝测试 driver / 易失状态用于持久写入；`NamespaceCapabilities::is_production_write_capable` 明确要求非易失持久性、非最终一致、排他写入。
5. 开发/测试模式可注册内存测试 driver，但它们只是测试工具，不代表生产后端选型，也不能用于满足 INV-10。

## 后果

- “接口工程预览通过”与“可用于生产”是两个不同结论：后者还需选定真实 driver、证明持久性/外部写入约束/故障恢复，并完成 P6 准入。
- 配置解析与校验（含 secret 引用、重复 ID、未知 kind、能力不匹配）在 P2 实现；P1 只固定接口与能力模型。
- 配置输出必须脱敏。

## 替代方案

- **首期实现 CreateBucket**：需要删除策略、namespace provisioning 与配额，范围过大，推迟。
- **首期选定某个数据库**：与“先定义通用接口”方向冲突，被否决。
