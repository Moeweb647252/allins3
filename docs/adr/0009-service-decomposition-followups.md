# ADR-0009：对象服务拆分的两项后续 —— 组合根与暂存账本

状态：Accepted。日期：2026-09-25。来源：架构深化评审（`improve-codebase-architecture`）候选 1「Composition 组合根」与候选 3「StagingLedger」；承接 [ADR-0008](0008-object-service-decomposition.md) 的「未决」清单，**不改变**其任何决策。

## 背景

ADR-0008 把 `ObjectService` 的四个关注点拆成协作者后，把两个正交候选记为未决：

> 未决：`StagingLedger` 与 `Composition`（组合根）仍是候选，留待真实痛点出现时再评估。

随后两项都落地了 —— 组合根收在 `src/compose.rs`，暂存账本收在 `src/service/staging.rs`。ADR-0008 已接受，其决策仍然生效，因此本 ADR 另记这两项后续，而不在 0008 正文里就地追加（就地编辑会让一条已接受 ADR 的后果随时间漂移，并与决策文字产生矛盾：决策写「本 ADR 不实现它」，后果却写「已实现」）。

## 决策

1. **组合根**：`src/compose.rs`（`pub(crate)`）持有启动接线 —— `startup_ctx`、`build_registry`、`instances_from_config`、`instance_from_factory`、`object_service`、`s3_service`。`server::run`、`testing::fixture::service`、`testing::ProtocolHarness::new` 都调用它，适配层标志与准入限制由同一份已解析的 limits 派生，不再各自维护一份可能漂移的启动顺序。对 crate 外仍是 `server::run` 与测试装置，不新增公开 trait 缝隙（沿用 ADR-0008 §2 的「缝隙保持内部」）。
2. **暂存账本**：`src/service/staging.rs` 的 `StagingLedger` 成为暂存配额、已暂存字节、在途（正在接收）字节与 part 表的**唯一所有者**。`MultipartState` 只保留 session、完成声明（claim）、完成记录，加上一个 `staging`。接口为 `reserve` / `release`（接收期计费）、`stage`（写入 part，施加配额与 part 数上限，并在同一步释放被替换的 part）、`take`（整体回收并返回释放字节）、`part` / `parts_of` / `is_idle`（只读）。
3. **part 表以 `UploadId` 为键**，不再由线上字符串重新推导身份；调用点复用 `UploadSession` 已持有的类型化 id，因此不新增分配。`PartChecksum` 与 `StoredPart` 同属 `staging`，依赖方向单向 `multipart → staging`。
4. **可见性**：`StagingLedger`、`StoredPart` 及仅由 crate 内适配层或测试装置调用的入口保持 `pub(crate)`；只在本模块使用的 `MAX_PARTS_PER_UPLOAD` 与仅供测试的字节访问器为私有。`PartChecksum` 经 `multipart` 重导出，保留 `service::multipart::PartChecksum` 这一公开路径（`PartSummary` 的公开字段需要命名它）。

## 后果

- ADR-0008 的四协作者决策与「缝隙保持内部」规则继续有效；本 ADR 不改写它们。
- 关键不变式 —— 已暂存 + 在途永不超配额，丢弃 part 与归还字节在同一步完成 —— 现在只在 `StagingLedger` 一处陈述并测试（`src/service/staging.rs` 的六个单元测试）。
- 证据：`cargo fmt --check` 与 `clippy -D warnings` 干净；128 个单元测试 + 64 个集成测试全绿。
- 组合根没有独立单元测试，其正确性由 `server::run`、`fixture::service`、`ProtocolHarness::new` 三个既有使用方共用同一段接线来保证。

## 替代方案

- **继续在 ADR-0008 正文里就地追加「后续」**：会让已接受 ADR 的后果与决策文字脱节（正是本 ADR 要消除的矛盾），也让「某条决策在何时成立」失去可追溯性。被否决；改为保持 ADR-0008 原文，新增本 ADR。
- **把 ADR-0008 标为 Superseded**：它的决策并未被推翻，「缝隙保持内部」等仍在生效，标 Superseded 会误表达为决策被替换。被否决。
- **组合根与暂存账本各开一条 ADR**：两者都是 ADR-0008 同一批「未决」候选，合记为一条后续 ADR 更贴合「一条 ADR 记录一次决策」的粒度。
