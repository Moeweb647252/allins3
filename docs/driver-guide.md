# 接入真实 driver

本指南说明如何为具体后端实现一个 driver，并把它接入到服务层。目标：**只新增 driver，不修改 service/protocol 代码**（`INV-01`）。

## 1. 必须实现的接口

| 接口 | 位置 | 义务 |
| --- | --- | --- |
| `StorageDriver` | `src/driver/traits.rs` | `descriptor`、`health`、`shutdown`、`open_namespace`；一个实例对应一份配置（连接、凭据、根路径） |
| `ReadNamespace` | `src/driver/traits.rs` | `stat` 返回「内容 + 元数据」一致的**完整** metadata 与当前 revision；`read` 必须只接受 `expected_revision` 匹配的对象；`list` 必须按 key 升序且分页稳定 |
| `WriteNamespace` | `src/driver/traits.rs` | `begin_write` 返回**不可见**的暂存写；`commit` 一次原子发布；`delete` 原子且遵守条件 |
| `StagedWrite` | `src/driver/traits.rs` | `write_chunk` 追加且背压由调用方施加；`commit(self)` 与 `abort(self)` 都消费自身；`abort` 必须使后续发布不可能 |
| `CommitRecovery`（可选） | `src/driver/extensions.rs` | `query(write_id)`：`Committed` / `NotCommittedFinal` / `InFlight` / `Unknown`；**查不到记录必须返回 `Unknown`**，不能当作终态失败 |
| `NativeCopy`（可选） | `src/driver/extensions.rs` | 仅在同一原生复制域内；只接受 registry 解析出的 `CopySource`（namespace + key + revision），不接受任意 URL |
| `MultipartNamespace`（可选） | `src/driver/extensions.rs` | 后端原生 multipart；声明 `multipart = true` 就会被服务层路由到这里（ADR-0010），不声明则由协调器用 staged write 补齐 |
| `DriverFactory` + `SecretResolver` | `src/driver/factory.rs` | 注册 kind、解析 `env:NAME` 之类引用；secret 值不得进入日志或错误消息 |

## 1.5 注册 kind（不改组合根）

本构建可构造的 kind 是一张数据表，不是组合根里的 `match`：在 `src/driver/builtins.rs` 的 `builtins()` 里加一条 `Builtin` 条目即可。`compose::build_registry` 只按配置里的 kind 查表，未知 kind 的报错会列出本构建可用的 kind；`check-config` 也会在启动前拒绝未注册的 kind。

```rust
// src/driver/builtins.rs
#[cfg(feature = "my-backend")]
table.push(Builtin {
    name: "my-backend",
    test: false,
    ctor: |kind| Arc::new(my_backend::MyBackendFactory::new(kind)),
});
```

- 构造函数收到配置里的 kind 字符串，不要在同一处再硬写一遍。
- 若 driver 在独立 feature 下，用 `#[cfg(feature = "...")]` 包住表项；表为空时默认构建仍然干净（ADR-0011）。
- `test` 标记（不是名字前缀）决定配置校验是否把它当作测试 driver：`test: true` 的 kind 只在 `development = true` 下放行，因此真实后端把 `test` 写 `false` 即可，即便名字碰巧以 `test-` 开头也不会被误判。
- 真实后端的现成例子见 `src/drivers/fs/`（kind `"fs"`，`test: false`，每个构建均注册）与其配置 `examples/fs-config.toml`；只读的真实后端见 `src/drivers/yun139/`（kind `"yun139"`，在 `yun139` feature 下注册，`examples/yun139-config.toml`）。

## 1.6 每实例私有配置（`options`）与 secret 引用

核心不解释你的配置：它把 `[[drivers]]` 里的 `options` 表原样交给工厂（`FactoryConfig::options`），并只证明 `secret_refs` 里的每个引用可解析。因此：

- **私有 schema 由你校验**。在 `DriverFactory::validate` 里解析 `config.options`，对缺失或未知键返回 `DriverError::invalid_argument`。这样 `check-config` 能在启动前拒绝坏配置——它只调 `validate`，不调 `create`，不会创建任何目录或连接。
- **`validate` 必须无副作用**：不得写用户数据，也不得创建目录（`check-config` 会在只读环境下被调用）。创建连接/目录放到 `create`。
- **`secret_refs` 是引用，不是值**。用 `config.secret_refs` 与自己 `options` 里的约定决定解析哪个引用（`secrets.resolve(reference)`）；secret 值不得进入日志或错误消息。
- `src/drivers/fs/mod.rs` 的 `FsOptions::parse` 是一个可参照的最小实现（必填 `root`，拒绝未知键）。
- **并非所有后端都用 `env:` 引用**：`yun139`（ADR-0017）按操作者选择把会话凭据直接放在 `options` 里，以 `Secret` 持有（`Debug` 打码），因此部署文件必须排除版本控制。凭据值不得进入日志或错误消息这一条仍然适用。

## 1.7 阻塞 I/O

`spec §10` 要求阻塞 I/O 不在异步运行时线程上执行，且必须受限。`FactoryConfig::blocking` 是组合根独占的 `BlockingExecutor`（限额 `limits.max_blocking_operations`，默认 64；`0` 表示不设服务侧上限），按实例交给你：

- 包装**同步 SDK** 的后端，把每次调用放进 `config.blocking.run(|| sdk_call()).await`，不要直接在 async 方法里做阻塞操作。
- **异步 I/O** 的后端（如 `fs` 使用 `tokio::fs`，其本身即受限阻塞池）可以直接 await；把执行器留给你确实需要同步执行的部分（`fs` 用它做全 namespace 的排序/折叠）。
- `0` 意味着没有服务侧上限：只在后端自己保证并发受限时才这么配。

## 2. 能力声明必须与句柄一致

`NamespaceCapabilities` 中的每个能力都必须与实际提供的句柄一致。`NamespaceHandle::is_self_consistent()` 在契约测试里检查，**服务层打开 namespace 时也会检查**——不一致的驱动在启动/打开时即失败，而不是被路由静默降级：

- `write` ⇔ `write` 句柄存在，`multipart` ⇔ `multipart`，`native_copy` ⇔ `native_copy`，`recovery` ⇔ `recovery`。
- `persistence = Volatile`、`consistency = Eventual`、`exclusive_writer = false` 中任意一项都会让 `is_production_write_capable()` 为假。
- `limits` 必须反映真实上限（单次写入、对象大小、part 数、part 最小大小、metadata 数量与字节）。服务层用这些值做规划与拒绝，写小了只是保守，写大了会在真实后端失败。

档位（bucket 配置的 `profile`）在启动时校验并在 `warmup` 阶段对每个 namespace 实际打开一次：能力不足会让服务器启动失败，而不是运行时降级。

生产模式（`development = false`）下这条线更严：任何非 `CoreRead` 的 bucket 在打开 namespace 时都必须通过 `is_production_write_capable()`，否则服务器启动失败并给出具体缺失项。标记为 `test: true` 的 kind 只能在 `development = true` 下通过配置校验（`config` 校验），而 `is_production_write_capable()` 覆盖真实 driver 声明易失存储或非排他写入的情况。

打开 namespace 时服务层还会用 `features_for_profile(profile)` 调用 `plan()` 并把结果缓存下来，之后的路由（原生 copy、原生 multipart、补齐）只读这份规划。因此**声明即路由**：声明了 `multipart = true` 就必须真的能用原生 multipart 完成一次上传（契约 harness 的 `multipart_native_semantics` 会验证），否则应在声明里写 `false` 并让服务层协调器兜底。

原生 multipart 的几处契约细节：`create_upload` 会收到该 upload 固定的 checksum 方案（`MultipartChecksumScheme`），`list_parts` 上报的每个 part 摘要必须按它计算；`complete` 会在发布**之前**用传入的 `expected` 校验对象级 checksum，校验不过必须不发布且不消耗 upload（契约 harness 现在两段都验：错的声明值被拒后可重试，对的声明值被接受）；`list_uploads` 的驱动契约不带 prefix（由服务层过滤）。

原生 upload 的生命周期归驱动：服务侧只保留一条绑定记录。两条由此而生的契约：

- **一个已消失的原生 upload，`list_parts` 必须返回 `NotFound`**。清理器就是靠这个判断服务侧记录能不能退休：`NotFound` 表示驱动已忘记它（租约、TTL 或另一个副本 abort 过），记录随之回收；其他任何错误都按「仍在」处理，把记录留在下一轮。若驱动用别的错误表示「不存在」，服务侧记录会一直保留。
- **`list_uploads` 上报的每个 upload 都必须可驱动**。服务对没有记录的原生 upload 会就地收养成 session（checksum/content/metadata 留空，由你持有），因此后续的 `list_parts` / `begin_part` / `complete` / `abort` 都按它自己的绑定工作。

完成清单的校验（part 是否存在与重复、ETag 一致、升序、非末段 5 MiB、part 声明的 checksum）由服务层一份实现完成，原生路径与协调器路径跑的是同一个函数，驱动不需要再判断一遍：同一条 S3 请求不会在一条路径上被接受、在另一条上被拒绝。其中 part 的 checksum 声明必须与该 upload 的 scheme 算法一致，算法不一致的声明一律拒（驱动只按 scheme 计算摘要，没有可比较的值），因此驱动未上报某个 part 的摘要时会得到「拒绝」而不是「放行」。

## 3. 语义红线

1. **暂存不可见**：`commit` 之前，`stat`/`read`/`list` 都不得看到新对象。
2. **revision 稳定**：`read` 只接受 `expected_revision`；本地不可读时必须返回 `RevisionUnavailable`，由服务层重走 `stat → 条件 → 区间` 流程，绝不能偷偷读一个新版本。
3. **单次发布**：`commit` 只能发布一次；重复或迟到的提交不得覆盖后续对象。放弃的尝试必须无法在之后发布（`abort` 之后 `commit` 必须失败）。
4. **提交结果分类**：丢失回执时必须返回 `DriverError::commit_outcome_unknown`（或带 `with_commit_state`），不得用「查不到记录」推断失败。
5. **元数据归属 driver**：driver 保存并返回完整 metadata（content 属性、用户 metadata、checksum、revision），服务层不维护第二份元数据副本。
6. **错误只用 `DriverError`**：带 `kind`（稳定分类）与安全消息；后端细节放进 `cause`，它不会出现在客户端响应里。

## 4. 验证一个 driver

`src/drivers/fs/` 是一个完整、可运行的参照实现（含契约测试 `tests/contract_fs.rs` 与真实进程 E2E `scripts/e2e_fs.sh`），可在写自己的后端前先读一遍。

```bash
# 1. 编译期：Arc<dyn Trait> / Box<dyn StagedWrite> / Send future 约束
cargo check --all-targets --all-features

# 2. 契约测试（同一套 harness，新增 driver 不改 harness）
#    在 tests/contract_reference.rs 的模式下接入你的 factory：
#    run_contract(driver, ContractNamespaces { primary, secondary })
cargo test --all-features

# 3. 协议层：把 driver 写进配置并跑真实 SDK
cargo run --features <你的 feature> -- examples/your-config.toml
python3 scripts/e2e_boto3.py
```

`scripts/e2e_cli.sh` / `e2e_drain.sh` / `e2e_timeouts.py` 会自建服务器；未用 `ALLINS3_BIN` 指定二进制时它们先 `cargo build --features testing`，因此不要用默认构建的二进制去跑它们（测试 driver 只在 `testing` 下注册）。声明了 `multipart = true` 的后端可参照 `examples/dev-config.toml` 里的 `test-native`（`example-c`）。

需要外部凭据的后端把契约测试放在自己的 feature 下，并用环境变量提供会话、缺失即打印原因跳过：见 `tests/contract_yun139.rs`（`yun139`，ADR-0017）。因此 `cargo test --all-features` 在没有凭据时仍然通过，而真实校验由可选运行的 `scripts/e2e_yun139.sh` 承担。

契约 harness 会验证：CRUD、暂存不可见、原子发布与 abort、revision 稳定读、元数据更新产生新 revision、有序分页、delimiter 折叠、条件写、namespace 隔离、提交恢复四态，以及**原生复制与流式补齐的等价性**（`copy_native_matches_emulation`）。能力不足的检查会被跳过并在报告中列出原因，不会静默通过。

## 5. 生产准入（P6 清单）

运行时已就位的部分：生产模式下可写 binding 必须 `is_production_write_capable()`（启动即拒绝）；退出时对每个 driver 实例调用 `StorageDriver::shutdown`（有 bounded deadline，失败只记日志）；`/readyz` 汇总每实例 `health`，任一实例不服务即返回 `503`。

本地文件系统 driver（`fs`）已完成的运行时部分：真实（跨重启）对象存储、每实例配置缝、受限阻塞执行器、提交结果记录与围栏、`descriptors()` 暴露每实例身份；证据为 `tests/contract_fs.rs` 与 `scripts/e2e_fs.sh`（含 SIGKILL 重启后读回）。

只读后端可进生产而无需 `state_dir`：`yun139`（ADR-0017）声明 `write = false`，bucket 配 `profile = "core-read"` 且 `readonly = true`，因此既不触发 `is_production_write_capable()`，也不要求协调器状态目录；证据为 `tests/contract_yun139.rs`（需真实会话）与 `scripts/e2e_yun139.sh`（4/4，含 range 读取与 PUT 被拒）。

两个与 driver 直接相关的后端义务：

- **`CommitRecovery` 是可恢复性的前提**：服务层在启动时以及运行期的重试/清扫时用 `query(write_id)` 对账未决的完成尝试（[ADR-0013](adr/0013-startup-recovery-and-readiness.md)、[ADR-0016](adr/0016-online-pending-completion-resolution.md)）。实现了它，一个回复丢失的 `CompleteMultipartUpload` 才能在重启后、或在线上重试时被判定为已发布/可重试；不实现（或对查不到的 id 报错而非 `Unknown`）会让此类 upload 只能靠「对象 ETag 与本次完成一致」这一保守读法判定，否则永远停在「未决」。
- **`state_dir` 不是 driver 的选项**：协调器的持久状态（session/part/claim/完成记录）属于服务层部署配置（`[server].state_dir`），不由 driver 的私有 `options` 承担，也不应被 driver 读取（[ADR-0014](adr/0014-durable-multipart-coordinator-state.md)）。

在标记某配置可用于生产前，仍必须补充（代码侧已无缺口，以下全部由操作者在真实环境执行并回填证据）：

- [x] 持久化 multipart session / staging，并证明提交与恢复协议在崩溃后成立（[ADR-0014](adr/0014-durable-multipart-coordinator-state.md)；`scripts/e2e_fs.sh` 在收到 `UploadPart` 的 `200` 后 `SIGKILL`，重启后同一 `UploadId` 仍可 `ListParts`/`Complete`）。
- [x] 启动恢复与 readiness 的顺序：`warmup` 后、监听前按 `CommitRecovery` 对账，失败即拒绝启动（[ADR-0013](adr/0013-startup-recovery-and-readiness.md)）。
- [x] 未决完成的在线解决：重试 `CompleteMultipartUpload` 与每轮清扫按 `CommitRecovery` 对账未决 claim（[ADR-0016](adr/0016-online-pending-completion-resolution.md)）。
- [ ] 进程崩溃、机器重启、网络分区、空间耗尽、凭据失效、后端限流的实测（手册见 `docs/operations.md`）。
- [ ] 索引/元数据与对象内容的备份、恢复与格式升级，不能只备份字节内容。
- [ ] 真实后端基准（含硬件、网络、并发、对象分布、checksum 与持久性设置）。
- [ ] 更新兼容矩阵为「已验证」，并写明测得的环境与版本。
