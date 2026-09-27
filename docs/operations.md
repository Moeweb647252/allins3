# 运维手册：部署、恢复与生产准入清单

状态：草稿（P6 进行中）。更新日期：2026-09-27。对应 spec §7.2、§10；INV-10。

**本手册的生产准入范围限于当前唯一的真实后端——本地文件系统配置（kind `fs`，[ADR-0015](adr/0015-local-filesystem-production-driver.md)）。** 远端后端（网络分区、远端凭据、后端限流）的准入项在引入远端 driver 前不适用，下文逐行标注为 N/A。

本手册回答两件事：**如何部署并验证一个配置**，以及**在把它标为「可用于生产」之前必须回填哪些证据**。它不能替代代码侧的待办（见下文「代码侧未完成」，那两项不完成就不能进入最后一步）。

## 1. 当前已就位的运行时能力

- 生产模式（`development = false`）下，可写 binding 必须通过 `is_production_write_capable()`（`write` + 非易失 + 非最终一致 + 排他写入），否则**启动即失败**并给出具体缺失项。
- driver 身份在启动日志可见：每实例输出 `id` / `kind` / `name` / `version`。
- `/healthz` 是存活；`/readyz` 汇总每实例 `health`，任一实例不服务即 `503`；排空期间 `503`。
- 退出顺序：停止接收 → 等待 `ObjectService::drain()`（在途操作清零，至多 10s）→ 对每个 driver 实例 `shutdown`（同样有界，失败只记日志）。`SIGINT` 与 `SIGTERM` 都会进入该路径；`SIGKILL` 不会（不可捕获）。
- 本地文件系统 driver（kind `fs`）是当前唯一的真实后端。其声明边界：**单节点、该节点的磁盘即持久域、本进程为唯一写入者**（[ADR-0015](adr/0015-local-filesystem-production-driver.md)）。
- multipart 协调器的状态是持久的：`[server].state_dir` 下写入 session、已确认的 part、完成记录与未决 claim，客户端收到 `200` 时记录已落盘（[ADR-0014](adr/0014-durable-multipart-coordinator-state.md)）。生产模式（`development = false`）且至少一个可写 bucket 时 `state_dir` **必填**，缺失则 `check-config`/启动即失败。
- 启动时在 `warmup` 之后、readiness 之前执行恢复：对每个未决 `write_id` 按 `CommitRecovery` 对账，恢复失败即拒绝启动（[ADR-0013](adr/0013-startup-recovery-and-readiness.md)）。启动日志有一行 `multipart recovery: sessions=… parts=… committed=… retryable=… pending=… reclaimed=…`。
- 运行期的未决完成可以**在线**解决，不必重启、不必手改 `state_dir`：重试 `CompleteMultipartUpload` 会按后端提交记录对账，清扫器每轮也会对账（[ADR-0016](adr/0016-online-pending-completion-resolution.md)）。

## 2. 部署前的静态检查

```bash
# 只校验，不打开监听、不创建目录、不连接后端
allins3 check-config /etc/allins3/config.toml
```

它会检查：结构、重复 ID、无效 bucket 名、未注册 kind、缺失凭据 secret、每实例 `secret_refs` 可解析、以及每个 driver 工厂对其私有 `options` 的校验。退出码 `0` 表示可启动，`2` 表示配置错误（消息不含 secret 值）。

## 3. 数据目录与备份

`fs` 的 `options.root` 下每个 namespace 一个目录：

```text
<root>/ns-<hex namespace>/objects/    已发布对象（一个对象一个文件）
<root>/ns-<hex namespace>/staging/    进行中的暂存写
<root>/ns-<hex namespace>/recovery/   提交结果记录
```

`[server].state_dir` 是协调器的持久状态，布局固定：

```text
<state_dir>/sessions/      进行中的 multipart session（一 session 一文件）
<state_dir>/parts/<id>/    该 upload 的已确认 part（data-* 字节 + meta-*.toml 记录）
<state_dir>/claims/        未决的完成尝试（upload_id -> write_id）
<state_dir>/completions/   完成记录（供重试与恢复查询）
```

备份**必须同时包含 `state_dir` 与 `root`**：只有对象字节而没有 recovery/完成记录，恢复时就无法区分「已发布」与「未知结果」；丢弃 `state_dir` 则客户端已知的 in-progress upload 全部不可恢复。备份要一个一致的时间点（先停服再拷贝，或在拷贝前短暂排空）。升级必须能读旧格式，或提供显式迁移步骤；不得在升级时静默丢弃 recovery/claim 记录。`state_dir` 与 `root` 应在同一个持久域（最好是同一卷），否则两者的 fsync 不构成一个时间点。

## 4. 生产准入清单（逐项回填证据）

> **代码侧已完成**：multipart 协调器的 session/staging 持久化（[ADR-0014](adr/0014-durable-multipart-coordinator-state.md)）、启动恢复/readiness 顺序（[ADR-0013](adr/0013-startup-recovery-and-readiness.md)）与未决完成的在线解决（[ADR-0016](adr/0016-online-pending-completion-resolution.md)）均已实现。第 1、2 项在代码与进程内的证据已就位（见下行），但**全部项目仍需在真实环境回填环境信息并执行**后，才能满足第 6 节的完成定义。标 N/A 的行只对本地 `fs` 配置不适用。

| # | 项目 | 命令 / 方法（`fs` 本地配置） | 证据（环境、版本、结果） |
| --- | --- | --- | --- |
| 1 | 持久化协议：崩溃后提交/恢复仍成立 | `bash scripts/e2e_fs.sh`（客户端收到 `UploadPart` 的 `200` 后 `SIGKILL`，重启后同一 `UploadId` 仍 `ListParts` 并 `Complete`）；进程内：`tests/service_multipart.rs::an_acknowledged_part_survives_a_restart_and_can_be_completed` | 本仓库已证：6/6；**待回填真实机器的内核/文件系统类型** |
| 2 | 启动恢复与 readiness | 恢复在 `warmup` 后、监听前执行，失败即拒绝启动；重启后 `pending` 计入启动日志。进程内：`tests/service_multipart.rs::an_interrupted_completion_is_reconciled_but_never_replayed`（未决尝试 `pending == 1`，第二次完成被拒、不重放）。在线解决：`::a_retry_resolves_a_pending_completion_the_backend_now_proves_published`、`::a_retry_releases_a_claim_the_backend_now_proves_not_published`、`::a_retry_resolves_a_pending_completion_by_matching_the_published_object`、`::the_sweeper_resolves_a_pending_completion_once_the_backend_can_answer`（[ADR-0016](adr/0016-online-pending-completion-resolution.md)） | 本仓库已证；**待回填真实机器** |
| 3 | 进程崩溃（`SIGKILL`）后数据完整 | `bash scripts/e2e_fs.sh`（本仓库已证：6/6，含重启读回与未完成 upload 恢复） | **待回填：机器/容器型号、内核、文件系统类型** |
| 4 | 机器重启 | 写对象 → 重启主机 → 读回；比对 size/ETag/checksum。对 `fs` 还需确认 `state_dir` 与 `root` 位于同一持久域：重启后重启服务，`multipart recovery:` 行的 `sessions`/`parts` 与重启前一致 | _待填：主机型号、内核、重启耗时、重启后读回结果_ |
| 5 | 网络分区（远端后端） | **N/A（本地 `fs`）**。`fs` 不经网络，无端点可断。留给远端 driver：断开端点观察 `/readyz` 转 `503`、恢复后转回 `200`、无对象损坏 | N/A |
| 6 | 空间耗尽 | 把 `root`（及 `state_dir`）所在卷填满：写对象应得 `500`/`503` 且不留半对象（`fs` 先在暂存收字节、`rename` 才发布，失败不产生对象）；释放空间后应能继续写。可用一个受限容量的卷或 `fallocate` 占位复现 | _待填：卷类型、填满方式、错误码、释放后结果_ |
| 7 | 凭据失效（服务侧） | 令某 `secret_ref` 不可解析（例如 `env:` 指向未设置的变量）：`allins3 check-config <config>` 应 `exit 2` 且消息不含 secret 值。对 `fs` 这是**唯一**相关的凭据路径 | _待填：参考配置、退出码、消息_ |
| 7b | 凭据失效（后端） | **N/A（本地 `fs`）**。`fs` 不持有后端凭据。留给远端 driver：运行中后端凭据失效应表现为临时错误而非能力丢失 | N/A |
| 8 | 后端限流 | **N/A（本地 `fs`）**。磁盘 I/O 的背压由 `max_blocking_operations` 与连接层准入承担，不是后端限流。留给远端 driver：注入限流确认返回 `503` 而非 `500`，且不改变声明能力 | N/A |
| 9 | 备份/恢复/格式升级 | 停服 → 同时备份 `root` 与 `state_dir`（见第 3 节，两者必须同一时间点）→ 清空 → 恢复 → 启动并读回全部对象与完成记录；再验证升级：用旧版本写出的目录由当前版本读取（格式未变） | _待填：备份工具、对象数、校验方式、升级路径_ |
| 10 | 真实基准 | 固定硬件/文件系统/并发/对象分布/checksum/持久性设置，记录 PUT/GET 吞吐与延迟。注意 `fs` 的已知代价：每次提交多一次 staging→target 拷贝（[ADR-0015](adr/0015-local-filesystem-production-driver.md) 未决项） | _待填：硬件、fs 类型、并发、对象分布、吞吐/延迟_ |
| 11 | 兼容矩阵标为「已验证」 | 更新 [兼容矩阵](compatibility-matrix.md) 第 5 节，写明 `fs` 本地配置、环境与版本 | _待填：环境与版本_ |

## 5. 未决完成（`pending > 0`）的处理

启动日志出现 `pending=` 非零，或一次完成的回复丢失且无法证明结果时，该 upload 会被**保留且不重放**：它既不能被重试完成，也不会被清扫回收（`INV-09`、ADR-0013）。**不要手工删除 `state_dir` 里的文件**：协调器的权威状态在运行进程的内存里，`DiskStore` 只是写穿镜像，手工改动会被下一次 flush 覆盖或造成内存态与磁盘态分叉。正确的路径是在线对账（[ADR-0016](adr/0016-online-pending-completion-resolution.md)）。

1. **看有哪些未决**：`ObjectService::pending_completions()` 返回每个未决 upload 的 `upload_id`、`write_id`、`bucket`、`key`、`state`、`recorded_at`（嵌入者可在自己的运维脚本里调用；这也是按未决筛选的只读视图）。
2. **重试完成即在线判定**：用同一 `UploadId` 与同一 part 清单再发一次 `CompleteMultipartUpload`。服务会重新读取后端提交记录（`fs` 在 `<root>/ns-<hex>/recovery/<write_id>`）：
   - 记录为 `Committed`，或记录非终态但目标 key 上已有**完全相同 ETag** 的对象 → 判定**已发布**，直接返回该对象（丢失回执的标准恢复），upload 退休，**不会再次发布**。
   - 记录为 `NotCommittedFinal` → 判定**未发布**，释放 claim，本次重试作为一次全新的尝试发布。
   - 记录为 `InFlight` / `Unknown` 且目标对象 ETag 不一致或不存在 → 仍**不可判定**，重试被拒为「already being completed」，upload 保留。
3. **无需客户端动作时，清扫器会自动对账**：`multipart_sweep_interval_secs` 非零时，每轮清扫先对未决 claim 按后端记录对账，再按租约回收。因此一处未决一旦后端记录变为可判定，就会被自动退休或释放。若清扫被禁用（间隔 `0`），则靠重试触发对账。
4. **真正无法判定的情形**：后端记录非终态，且目标 key 上的对象 ETag 与本次完成不一致（或对象不存在）。这通常意味着崩溃窗口让后端记录停在 `InFlight`（例如 `fs` 在 rename 与写 `Committed` 之间被杀），或原 part 清单已不可重建。此时：
   - 用 `HeadObject` 核对目标 key 的 size/ETag，判断它是否是本次期望的对象；是则按业务当作已发布使用该对象；
   - 只有在确认该 attempt 已不可能再发布（进程已不在、后端也不再持有该 attempt）后，才把它作为业务层的一次失败处理（客户端重新发起一次全新的 multipart）。
   任何这类人工判断都记录到第 4 节表的证据栏。**不得直接编辑 `state_dir`。**

## 6. 完成定义

**发布标签不因本手册的更新而改变。** 当前发布物仍是**工程预览**：接口稳定、单一真实后端（`fs`）可运行、崩溃后协调器状态可恢复。

要把 `fs` 本地配置声明为「可用于生产」，**操作者**必须在下表回填真实环境证据——本仓库只能提供代码、进程内证据与可执行步骤，不能代替目标机器上的部署验证：

- **必须回填**：第 1–3 项的真实机器/文件系统信息（第 3 项已有仓库内证据，只差环境字段）；第 4、6、9、10 项在目标环境跑过并回填；第 9 项应包含一次真实的备份/恢复演练。
- **必须记录为 N/A**：第 5、7b、8 项，且注明其属远端 driver 准入；第 7 项（服务侧凭据）对 `fs` 适用，须回填。
- **随之前置**：第 11 项在 [兼容矩阵](compatibility-matrix.md) 标为已验证并写明环境。

全部回填之前，任何「可用于生产」的声明都不成立。这是一项部署准入动作，不是代码待办。
