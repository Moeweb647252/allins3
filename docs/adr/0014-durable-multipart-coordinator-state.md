# 0014 multipart 协调器的 session/staging 持久化

状态：Accepted（已实现）。日期：2026-09-26。对应 spec §8.1、§8.2；INV-10。**取代 P4-01 的偏离记录。**

## 背景

P4-01 有意偏离并记录：`MultipartStore` / `StagingStore` **没有**被引入为 trait，协调器状态由 `src/service/multipart.rs` 的 `Mutex<MultipartState>`（含 `StagingLedger`）独占，理由是「暂存与 multipart session 的状态必须与单次发布、竞争仲裁、完成记录在同一个临界区内推进」，并注明「真实后端的持久化暂存由 P6 的 driver 在其 `StagedWrite`/`MultipartNamespace` 内实现」。

该偏离在**接口预览**阶段成立，但在 P6 的生产声明下不成立。spec §8.1 明确：*「生产启用 multipart 时，session 和 part 的持久性必须满足对客户端已确认的 upload/part 可恢复性。」* P6 清单第 1 项也要求「持久化 metadata / multipart session / staging 的实现，并证明提交与恢复协议在崩溃后仍成立」。

**矛盾点**：一个客户端收到 `UploadPart` 的 `200` 之后进程崩溃，重启后必须仍能 `ListParts` 并 `Complete`。若协调器的 session/暂存只在进程内，这条保证不存在——这不是范围偏好，而是 P6 第一行的违反。

## 决策

1. **把 spec §8.1 的两个接口落为 trait**：`MultipartStore`（session CRUD、按版本 CAS 转移状态、part 清单、marker 列举、租约与完成记录）与 `StagingStore`（按不透明引用写/读/删 part，声明持久性与配额）。协调器仍独占临界区语义，但状态通过接口读写。

2. **两个实现**：内存实现（现有行为，测试与开发模式）与持久实现（生产）。持久实现的根目录由新配置 `state_dir` 给出；配置了可持久 namespace 时**必填**，缺失即在启动前失败。

3. **持久实现零新依赖**：每条记录 `tmp → fsync → rename`，启动时扫描重建；part 按 upload 分目录。SQLite 等嵌入式库作为替代实现保留评估（见 ADR-0015 替代方案）。

4. **崩溃恢复与对账由 ADR-0013 的 `recover()` 承担**：载入持久 session/part/完成记录，对未决 `write_id` 按 `CommitRecovery` 四态对账，`InFlight`/`Unknown` 不重放。

5. **P4-01 的偏离记录标注 Superseded**：其「状态必须与协调器同一临界区」的论断仍然正确——trait 只是让状态的**存储**可替换，临界区与不变量仍归协调器；被取代的是「因此不引入 trait」。

## 后果

- 生产启用 multipart 时，对客户端已确认的 upload/part 在重启后仍可恢复（`INV-10`）。
- 协调器多一层间接：内存实现必须证明与现行为等价（现有 `tests/service_multipart.rs` 全绿即回归证据）。
- 恢复测试需要「丢弃 service、用同一 `state_dir` 重建」的进程内模拟，并在文档标注其局限（不替代真实进程崩溃验证）。

## 实现说明

落地的形状与上文略有出入，以本节为准；两者的保证相同。

1. **接口落为 trait，但只有一个维度**：`service::multipart::store::MultipartStore` 是「协调器状态的**存储**」这一个接口，方法全是单条记录的读写（`write_session`/`write_part`/`write_claim`/`write_completion`/对应删除），没有把「按版本 CAS 转移」「part 清单」「marker 列举」也拆成方法——那些仍归协调器的临界区（P4-01 的论断成立，本 ADR 只取代「因此不引入 trait」）。两个实现：`MemoryStore`（空操作，开发与测试默认）与 `DiskStore`（`state_dir` 下的文件）。
2. **写穿 + 边界 flush**：协调器在持锁的同步步骤里把变更记为 `PersistOp` 队列（廉价），在释放锁之后、写响应之前 `flush_pending()` 落盘（文件 I/O 不在锁内、不在 runtime 线程上无界阻塞）。一个必须被确认的操作（`CreateMultipartUpload`、`UploadPart` 的 `200`）只有 flush 成功才算完成，因此 `200` 蕴含记录已落盘（`INV-10`）。flush 失败时把失败项及其余项重新入队，再向客户端报错。
3. **提交尝试先落 claim**：`claim`（`upload_id -> write_id`）在 `begin_write`/原生 `complete` **之前**持久化，所以进程在发布中途死亡时，它可能发布所用的 write id 在盘上，启动恢复才能核对（`ADR-0013`）。
4. **零新依赖的布局**：`state_dir` 下 `sessions/`、`parts/<id>/`、`claims/`、`completions/`；每条记录 `tmp → fsync → rename`，part 先写字节、后写记录（记录是提交标记），id 经百分号转义后才是路径分量。残缺或不可解析的记录是启动失败，不是静默跳过。
5. **状态由配置决定**：`[server].state_dir`；生产模式（`development = false`）且至少一个可写 bucket 时**必填**，缺失即 `ConfigError::StateDirRequired` 在启动前失败；开发模式与只读部署可省略。
6. **内存实现等价性**：`MemoryStore` 下 `flush_pending` 是空转，协调器的状态机、完成记录、claim 仲裁与清扫与改造前逐字相同，`tests/service_multipart.rs` 全绿即回归证据。

真实进程证据：`scripts/e2e_fs.sh` 在客户端收到 `UploadPart` 的 `200` 之后 `SIGKILL` 进程，重启后用同一 `UploadId` `ListParts`（须列出该 part 与同一 ETag）并 `CompleteMultipartUpload`（须发布同样字节）。进程内模拟（`tests/service_multipart.rs` 的 `*_survives_a_restart_*`、`an_interrupted_completion_is_reconciled_but_never_replayed`）只覆盖协调器一侧，driver 侧仍是内存态。

## 替代方案

- **保持进程内状态、改由 driver 的 `MultipartNamespace` 承担责任**：需要每个真实 driver 实现原生 multipart 并自持跨重启语义，把同一套不变量复制到 N 个后端；被否决，因为协调器的等价性（原生与补齐同一套校验）会被削弱。
- **把 `state_dir` 做成每 driver 选项**：会与 ADR-0012 的「options 是 driver 私有 schema」冲突——协调器不是 driver，其存储是服务层部署配置。
