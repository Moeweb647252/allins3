# 0016 未决完成的在线解决

状态：Accepted（已实现）。日期：2026-09-27。对应 spec §7.2、§8.1、§9.2；INV-09、INV-10。承接 [ADR-0013](0013-startup-recovery-and-readiness.md)、[ADR-0014](0014-durable-multipart-coordinator-state.md)。

## 背景

ADR-0013 让启动恢复对每个未决 `write_id` 按 `CommitRecovery` 四态对账：`Committed` 退休该 upload、`NotCommittedFinal` 释放 claim 允许重试、`InFlight`/`Unknown` 保留未决记录且不重放。但**对账只发生在启动时**。一个在运行期进入 `InFlight`/`Unknown` 的完成尝试（提交回执丢失、后端记录当时还不可读、进程没重启）会永久停在未决：既不能被客户端重试（claim 把它拒绝为「already being completed」），也不会被清扫器回收（claim / `Unknown` 记录让它跳过）。它只能等到某次重启才可能被对账，或者由操作者按 `docs/operations.md` 的手工手册处理。

那份手册有三个问题：

1. **只覆盖重启。** 运行期产生的未决没有任何路径解决，除非重启进程。
2. **手工删文件脆弱且与运行进程竞争。** `DiskStore` 是写穿镜像，运行进程持有权威内存态；在进程仍在运行时删 `state_dir/claims/*.toml`，会被下一次 flush 覆盖，或让内存态与磁盘态分叉。手册也承认删除必须「在变更窗口内进行」。
3. **状态不可枚举。** 操作者能从启动日志看到 `pending=N`，却没有一个「哪些 upload 未决」的视图。

问题的实质不是「如何在重启后对账」——那是 ADR-0013 已经解决的——而是「如何在一个仍在运行的进程里，凭后端提交记录把结果未知的完成判定为已发布或可重试，并把它应用到协调器状态」。

## 决策

1. **在线对账与启动恢复同源。** 把 ADR-0013 恢复循环里「按四态施加副作用」的部分抽成 `settle_attempt`：`Committed` 记录并退休 upload，`NotCommittedFinal` 记录并释放 claim，`InFlight`/`Unknown` 记录为未决。启动恢复与在线对账都调用它，两条路径不可能对同一个状态给出不同处置。

2. **重试 `CompleteMultipartUpload` 时先对账。** 当 upload 已持有一个 claim 时，不再无条件拒绝，而是先按该 claim 的 `write_id` 重新查询后端提交记录：
   - `Committed` → 从已发布的 revision 还原完成的响应（丢失回执的标准恢复），退休 upload，**不再次发布**；
   - `NotCommittedFinal` → 释放 claim、记录该结论，本次重试继续，作为一次全新的尝试发布；
   - `InFlight` / `Unknown` → 记录仍非终态；按下面的保守读法核对目标对象，仍不能判定时维持拒绝。

3. **清扫器每轮先对账**，再按租约回收。这样一处未决一旦后端记录变为可判定，就会在没有任何客户端动作的情况下被退休（已发布）或释放（可重试）。对账**只读用户对象**：它从不发布，记录读取失败就保持未决。

4. **非终态记录的保守读法只在 ETag 完全一致时才成立。** `InFlight`/`Unknown` 本身不构成证据。一次重试知道「本次完成会产生的对象 ETag」（由 part 清单算出）；若目标 key 上已存在的对象带**完全相同**的 ETag，那就是「早先那次尝试确实发布了这个对象」的证明，可以按已发布处置。ETag 不一致、对象不存在、或读取失败都维持未决。**绝不覆盖**任何可能已发布的对象（INV-09）。

5. **不新增网络入口。** 解决入口是既有的、已鉴权的 S3 完成请求本身；操作者的动作就是把完成重试一次。这避免了新的管理端点及其鉴权面，也不引入「离线 CLI 直接改 `state_dir`」与运行进程竞争的问题。`ObjectService::pending_completions()` 提供一个只读视图，供操作者枚举未决。

## 后果

- 运行期的未决可以自愈：查得清就自动判定，查不清仍保守保留。
- `docs/operations.md` 的手工删文件手册被替换为「重试完成 / 等清扫器」；只有在对象 ETag 与本次完成不一致、且后端记录非终态这一真正无法判定的情形下才需要人工核对。
- native 路由只知道后端记录（它不持有服务侧的 part 表，算不出『本次会产生的 ETag』），因此只由终态记录解决；`InFlight`/`Unknown` 仍是未决。
- 对账在每个清扫周期对所有未决 claim 各查一次后端，未决数量通常为 0；对账是幂等的（同一结果重复记录不重写镜像）。

## 实现说明

1. **共享处置**：`src/service/multipart.rs` 的 `settle_attempt` 由 `recover` 与 `reconcile_pending_completions` 共用；`record_completion_in` 对「同一 `write_id` 已是同一状态」是空操作，所以每轮清扫重复对账不会改写持久镜像。
2. **重试路径**：`resolve_pending_claim(ctx, session, upload_id, write_id, expected_etag)` 决定 `Published` / `Retryable` / `Unresolved`；`complete_multipart`（协调器）在取 claim 之前调用它，`expected_etag` 是由 part 清单算出的 multipart ETag，用于第 4 条的保守读法；`complete_multipart_native` 以 `None` 调用，只认终态记录。
3. **清扫器**：`sweep_expired_uploads` 在挑选过期候选之前先 `reconcile_pending_completions(ctx)`；被释放的 claim / 被退休的 upload 会先 flush，再进入回收判定。
4. **可观测**：`ObjectService::pending_completions()` 返回 `PendingCompletion { upload_id, write_id, bucket, key, state, recorded_at }`（按 upload id 排序）。
5. **测试**：`src/testing/fault.rs` 新增 `FaultPlan::recovery_unknown_once`（首个 `CommitRecovery::query` 答 `Unknown`、其后委托真实记录），用于在运行期制造「记录先不可读、后变终态」的窗口而不重启。证据：`tests/service_multipart.rs::a_retry_resolves_a_pending_completion_the_backend_now_proves_published`、`::a_retry_releases_a_claim_the_backend_now_proves_not_published`、`::a_retry_resolves_a_pending_completion_by_matching_the_published_object`、`::the_sweeper_resolves_a_pending_completion_once_the_backend_can_answer`；既有 `::an_unresolvable_commit_outcome_blocks_replay_and_is_never_swept` 改成真正不可判定的 `FencedBeforePublish` + `hide_recovery`，继续证明不可判定时仍拒绝重放。

## 替代方案

- **新增管理 HTTP 端点（列出 / 触发对账）**：被否决。它引入新的、需要自行设计鉴权的攻击面；而 S3 完成请求本身已经鉴权、已经携带定位该 upload 所需的全部信息。
- **CLI 子命令离线对账 `state_dir`**：被否决。协调器的权威状态在运行进程的内存里，`DiskStore` 只是写穿镜像；离线改动要么被下一次 flush 覆盖，要么要求先停服——那就不是「在线应用」。
- **`InFlight`/`Unknown` 时只要目标 key 存在就判定已发布**：被否决。对象存在不是「本次尝试发布了它」的证明——它可能是另一次写入的、内容不同的对象；据此判定会覆盖或误报一个并非本次完成的修订。只有在 ETag 完全一致时，证据才等价于「本次完成已发布」。
