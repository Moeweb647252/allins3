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
| `MultipartNamespace`（可选） | `src/driver/extensions.rs` | 后端原生 multipart；不实现时由服务层协调器用 staged write 补齐 |
| `DriverFactory` + `SecretResolver` | `src/driver/factory.rs` | 注册 kind、解析 `env:NAME` 之类引用；secret 值不得进入日志或错误消息 |

## 2. 能力声明必须与句柄一致

`NamespaceCapabilities` 中的每个能力都必须与实际提供的句柄一致，`NamespaceHandle::is_self_consistent()` 会在契约测试里检查：

- `write` ⇔ `write` 句柄存在，`multipart` ⇔ `multipart`，`native_copy` ⇔ `native_copy`，`recovery` ⇔ `recovery`。
- `persistence = Volatile`、`consistency = Eventual`、`exclusive_writer = false` 中任意一项都会让 `is_production_write_capable()` 为假。
- `limits` 必须反映真实上限（单次写入、对象大小、part 数、part 最小大小、metadata 数量与字节）。服务层用这些值做规划与拒绝，写小了只是保守，写大了会在真实后端失败。

档位（bucket 配置的 `profile`）在启动时校验并在 `warmup` 阶段对每个 namespace 实际打开一次：能力不足会让服务器启动失败，而不是运行时降级。

## 3. 语义红线

1. **暂存不可见**：`commit` 之前，`stat`/`read`/`list` 都不得看到新对象。
2. **revision 稳定**：`read` 只接受 `expected_revision`；本地不可读时必须返回 `RevisionUnavailable`，由服务层重走 `stat → 条件 → 区间` 流程，绝不能偷偷读一个新版本。
3. **单次发布**：`commit` 只能发布一次；重复或迟到的提交不得覆盖后续对象。放弃的尝试必须无法在之后发布（`abort` 之后 `commit` 必须失败）。
4. **提交结果分类**：丢失回执时必须返回 `DriverError::commit_outcome_unknown`（或带 `with_commit_state`），不得用「查不到记录」推断失败。
5. **元数据归属 driver**：driver 保存并返回完整 metadata（content 属性、用户 metadata、checksum、revision），服务层不维护第二份元数据副本。
6. **错误只用 `DriverError`**：带 `kind`（稳定分类）与安全消息；后端细节放进 `cause`，它不会出现在客户端响应里。

## 4. 验证一个 driver

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

契约 harness 会验证：CRUD、暂存不可见、原子发布与 abort、revision 稳定读、元数据更新产生新 revision、有序分页、delimiter 折叠、条件写、namespace 隔离、提交恢复四态，以及**原生复制与流式补齐的等价性**（`copy_native_matches_emulation`）。能力不足的检查会被跳过并在报告中列出原因，不会静默通过。

## 5. 生产准入（P6 清单）

在标记某配置可用于生产前，必须补充：

- [ ] 持久化 metadata / multipart session / staging 的实现，并证明提交与恢复协议在崩溃后仍成立。
- [ ] 进程崩溃、机器重启、网络分区、空间耗尽、凭据失效、后端限流的实测。
- [ ] 索引/元数据与对象内容的备份、恢复与格式升级，不能只备份字节内容。
- [ ] 真实后端基准（含硬件、网络、并发、对象分布、checksum 与持久性设置）。
- [ ] 更新兼容矩阵为「已验证」，并写明测得的环境与版本。
