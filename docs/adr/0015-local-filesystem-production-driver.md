# 0015 本地文件系统作为第一个生产 driver

状态：Accepted。日期：2026-09-26。对应 spec §2、§3、§5、§7、§9；INV-01、INV-03、INV-09；ADR-0002、ADR-0003。

## 背景

ADR-0006 把「生产状态存储待选型」留作未决项，P6 又以「选定真实后端」为前提。要让 driver 接缝从「接口预览」变为可证明的真实性，需要一个真实（非内存、跨重启）的后端——否则 `is_production_write_capable()`、`Persistence::Durable`、`recovery` 句柄、契约 harness 与恢复协议全都只有测试替身的证据。

## 决策

1. **第一个生产 driver 是本地文件系统**（`src/drivers/fs/`，kind `"fs"`），在 `driver::builtins` 以 `test: false` 注册，因此**每个构建都包含它**，默认构建无需 feature 即可运行。

2. **生产声明是窄的，且写进声明本身**：`persistence = Durable` 指「进程退出后仍存在」，`consistency = Strong`，`exclusive_writer = true`。后者不是装饰：条件的提交/删除在本进程内用每 namespace 的 `tokio::sync::Mutex` 串成一个临界区，这**只在单一写入进程**的前提下成立。运行该配置意味着：单节点、该节点的磁盘即持久域、本进程是唯一写入者。一旦允许外部修改，元数据/revision/列举/条件语义必须重新证明（spec §5.2）。

3. **一个对象一个文件，发布是一次原子 `rename`。** 文件格式为 `u64` 小端元数据长度 + TOML 元数据 + 原始字节。元数据与内容**不能**分成两个文件：那会留下「一个已落地、另一个没有」的崩溃窗口，正是 ADR-0002 禁止的元数据/内容分裂。提交时先在暂存文件收字节，再把头+元数据+内容组装进目标临时文件并 `fsync`，最后 `rename`——`rename` 是唯一的发布点。

4. **提交结果记录随尝试走**（ADR-0003）：`begin_write` 记 `InFlight`，`abort` 或发布前校验失败记 `NotCommittedFinal`，`rename` 之后记 `Committed`。记录缺失（含 `rename` 与记录之间的崩溃）**返回 `Unknown`，不是终态失败**。`commit` 在临界区内先查记录，`NotCommittedFinal` 的尝试即使迟到也不能发布（围栏）。

5. **revision 与 ETag 分离**：revision 由（纳秒时间戳，进程内计数器）生成并在重启后仍唯一，元数据更新即使 ETag 不变也产生新 revision（spec §5.4）。

6. **能力声明与句柄一致**：声明 `multipart = false`（由服务层协调器以 staged write 覆盖）与 `native_copy = false`（由服务层读流补齐），`recovery = true` 且句柄存在，`is_self_consistent()` 在打开时成立。

## 后果

- `check-config examples/fs-config.toml`（`development = false`）通过；真实进程下 PUT/GET、协调器 multipart、以及 **SIGKILL 重启后的读回** 由 `scripts/e2e_fs.sh` 证明（4/4）。
- 契约 harness 对 `fs` 全绿（`tests/contract_fs.rs`），包括暂存不可见、原子发布、abort 围栏、条件提交拒绝且不留对象、revision 稳定读、重启后恢复记录仍为 `Committed`、未知 write id 为 `Unknown`。
- `options` 是 driver 私有 schema（`root` 必填，`max_object_size` / `max_single_write_size` 可选），未知键被拒绝；核心不解释它（ADR-0012）。
- 每次提交组装整文件需要一次 staging→target 的拷贝；发布仍为原子。以固定大小头重写可省掉这次拷贝，属后续优化，不影响语义。

## 未决（留给后续 P6 工作）

- **原生 multipart**：当前由协调器覆盖。声明 `multipart = true` 需要后端自持 upload 生命周期，属可选后续。
- **metadata/session/staging 的持久化与崩溃恢复**：本 ADR 只保证 driver 的 staged write 与对象持久；协调器的 session/暂存仍随进程消失，这是 P6 清单第 1 项，见 ADR-0014（Proposed）。
- **外部故障域**（机器重启、分区、空间耗尽、凭据失效、限流）与基准：以操作者手册交付，不由本仓库造环境（见 `docs/operations.md`）。

## 替代方案

- **远端 S3 兼容后端**：更能证明真实网络/凭据/限流路径，但需要凭据与可用端点，CI 无法默认运行。本 ADR 的方向（先本地 FS 打通接缝与恢复协议）与它不互斥，远端后端可在同一接缝上追加。
- **SQLite 等嵌入式库**：会引入新依赖与格式升级问题；当前 journal 设计零新依赖。若未来需要更强的并发/查询语义，可作为 ADR-0014 的替代实现重新评估。
