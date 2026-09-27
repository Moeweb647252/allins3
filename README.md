# allins3

多 driver S3 服务器。当前处于**工程预览**阶段，但已有一个真实（跨重启）后端：本地文件系统 driver（kind `fs`）在每个构建中注册，能在 `development = false` 的生产模式下启动并持久化对象。其生产声明是窄的：**单节点、该节点的磁盘即持久域、本进程为唯一写入者**（[ADR-0015](docs/adr/0015-local-filesystem-production-driver.md)）。multipart 协调器的 session/staging 持久化与启动恢复均已实现（[ADR-0013](docs/adr/0013-startup-recovery-and-readiness.md)、[ADR-0014](docs/adr/0014-durable-multipart-coordinator-state.md)）：客户端已确认的 upload/part 跨重启仍可恢复，启动时对未决完成尝试按 driver 的提交记录对账且从不重放。生产模式的可写部署必须配置 `[server].state_dir`。在真实环境证据（崩溃/重启/分区/空间/凭据/限流/基准，见[运维手册](docs/operations.md)的准入清单）回填之前，仍不应将任何配置标为可用于生产。

- [实现规范](docs/implementation-spec.md)：架构边界、通用 driver 接口、能力模型、S3 语义和验收不变量。
- [实现计划](docs/implementation-plan.md)：阶段任务、依赖、交付物和测试标准（含当前进展）。
- [兼容矩阵](docs/compatibility-matrix.md)：每个操作的真实状态、payload/checksum 范围、错误映射与验证记录。
- [driver 接入指南](docs/driver-guide.md)：如何新增真实后端并取得生产准入。
- [运维手册](docs/operations.md)：部署/备份步骤与生产准入证据清单（含仍未完成的代码侧事项）。
- [P0 协议适配评估](docs/p0-protocol-adapter-assessment.md)：`s3s` 候选评估与未决项。
- [ADR](docs/adr/)：架构决策记录。

## 快速开始

```bash
# 1. 校验配置（不监听端口，退出码 0/2）
ALLINS3_DEV_SECRET=devsecret cargo run --features testing -- check-config examples/dev-config.toml

# 2. 启动服务器（内存测试 driver 需要 development = true）
ALLINS3_DEV_SECRET=devsecret cargo run --features testing -- examples/dev-config.toml

# 2b. 真实后端：本地文件系统 driver（每个构建均注册，无需 feature）
ALLINS3_FS_SECRET=devsecret cargo run -- check-config examples/fs-config.toml
bash scripts/e2e_fs.sh   # 生产模式端到端 + SIGKILL 重启后读回（6/6）

# 3. 用真实客户端走一遍完整闭环（boto3 / AWS CLI / 手工 aws-chunked trailer 请求）
ALLINS3_SECRET_KEY=devsecret python3 scripts/e2e_boto3.py     # 需要 boto3
bash scripts/e2e_cli.sh                                       # 需要 .venv/bin/aws（pip install awscli）
ALLINS3_SECRET_KEY=devsecret python3 scripts/e2e_streaming_trailer.py
bash scripts/e2e_drain.sh   # 排空时序：真实 SIGINT/SIGTERM + 真实慢速上传（自管服务器）
python3 scripts/e2e_timeouts.py  # 请求边界：header/body idle/总 deadline（自管服务器）

# 4. 内存预算基准（默认 100 MiB；ALLINS3_BENCH_FULL=1 跑 100 MiB/1 GiB/10 GiB）
ALLINS3_BENCH_FULL=1 cargo test --release --all-features --test resource_budget -- --nocapture
```

服务器端点：`/healthz`（liveness）、`/readyz`（readiness；排空时或任一 driver 实例不健康时 `503`）、`/metrics`（计数器），以及 path-style 的 S3 API。示例配置见 [`examples/dev-config.toml`](examples/dev-config.toml)。

## 当前能力

- `src/domain/`：强类型 ID、`ObjectKey`（key 字节原样保留）、`ObjectMetadata`/`ObjectRevision`、checksum、`DriverError`（含 `CommitState` 四态）、`DriverContext`（超时与取消）、`ResolvedRange`、列举请求/游标。
- `src/driver/`：`StorageDriver`、`ReadNamespace`、`WriteNamespace`、`StagedWrite`、`NativeCopy`、`MultipartNamespace`、`CommitRecovery`、工厂/注册表、能力与限制模型及 Native/Emulated/Unsupported 规划；本构建可构造的 kind 由 `driver/builtins.rs` 的数据表声明，组合根只查表（新增 driver 不改组合根，见 ADR-0011）。
- `src/config/` + `src/catalog.rs`：TOML 配置校验、凭据 secret 引用、每实例 `options`/`secret_refs`（核心不解释私有 schema）、bucket→namespace 不可变绑定、全局与每 principal、每 driver 并发限制与阻塞操作上限、暂存配额。
- `src/drivers/`：具体后端实现。当前有本地文件系统 driver（`drivers::fs`，kind `fs`）：单文件原子 `rename` 发布、提交结果记录与围栏、revision 稳定读、有序列举；契约与持久性测试见 `tests/contract_fs.rs`。
- `src/service/`：GET/HEAD/PUT/DELETE/LIST/COPY、条件与 Range 的 `stat → 条件 → 区间 → 携带 revision 读取` 流程、原子发布、列举 token 的作用域绑定；multipart 两条路径共用同一套语义（固定清单与 scheme、单次发布、竞争仲裁、完成记录、过期暂存回收）—— 原生 namespace 由驱动持有 part 与完成，其余由通用协调器在 staged write 上补齐；两条路径共用的 marker 解析、scheme 定案与清单校验在 `service/multipart/rules.rs`；协调器状态的持久镜像（`MultipartStore` 与文件后端）在 `service/multipart/store.rs`。路由由 namespace 打开时物化的能力规划决定（`copy_route` / `native_multipart`，见 ADR-0010）。
- `src/protocol/s3/`：基于 `s3s` 的 path-style HTTP/S3 适配、SigV4（header 与预签名 query）、默认拒绝的授权、请求 ID、错误映射、每 principal 准入控制。`mod.rs` 是 handler 与操作实现，字节流桥接（`body.rs`）、校验和头（`checksum.rs`）与 DTO 转换（`convert.rs`）各自独立成模块。
- `src/server.rs`：启动顺序（配置 → driver → namespace 能力校验 → readiness）、优雅退出（排空后逐个 `StorageDriver::shutdown`）、健康与指标端点（`/readyz` 汇总每 driver 实例健康）。
- `src/testing/`（`testing` feature）：内存参考 driver（`test-reference`，协调器 multipart）、排序向量独立 driver（`test-vec`）、声明原生 multipart 的参考 driver（`test-native`）、故障注入包装器、计数 sink（度量写入而不物化数据）、可复用契约 harness、服务级 fixture、进程内协议 harness（构建二进制同款 `S3Service`，可由性质测试、协议用例与三个模糊测试目标共用，并含原生 multipart 变体）。

## 构建与检查

```bash
# 一个入口，同时检查默认与 testing 两种 feature 组合（CI 也调用它）
scripts/check.sh

# 等价的手工命令
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --all-features

# P0 协议适配 spike（独立 workspace，不影响主 crate 依赖图）
cd spikes/protocol-adapter && cargo test

# P5-05 模糊测试（独立 workspace + nightly + cargo-fuzz；默认三个目标各 20000 轮）
scripts/fuzz.sh
```

## 已知限制（与兼容矩阵一致）

- 仅 path-style；`addressing = "virtual"` 在校验阶段明确失败（未实现）。
- 不支持 Versioning、SSE、ACL、Object Lock、生命周期、通知、复制、网站托管、Select、SigV2/SigV4a；这些参数会明确报错而不是被忽略。
- multipart 支持 FULL_OBJECT 与 COMPOSITE checksum（线格式 `base64(digest)-<partcount>`）；`ETag` 始终按规范发布 `md5(parts)-N`。
- multipart 暂存由后台租约清理器回收（`multipart_lease_secs`，默认 3600 秒无 part 活动；`multipart_sweep_interval_secs = 0` 可禁用），在途完成、结果未知以及仍在收 part 的上传不会被误清。
- `aws-chunked` 请求体、checksum trailer 与签名分块（`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`）已由运行中的服务器验证（含 trailer 值不匹配、声明未送达、算法与 upload scheme 不符、单块或 trailer 签名被改，`PutObject` 与 `UploadPart` 两条入口）；CLI 与 botocore 1.42 对 HTTP endpoint 不发出这些形式，因此该路径只有手工构造请求的证据。
- 主要兼容证据：boto3/botocore 1.42.97（`scripts/e2e_boto3.py` 67/67）、AWS CLI 1.44.87（`scripts/e2e_cli.sh` 29/29）、手工 `aws-chunked` 请求（`scripts/e2e_streaming_trailer.py` 34/34，含签名分块与 trailer 签名被改的拒绝）、请求边界（`scripts/e2e_timeouts.py` 21/21）、内存预算基准（`tests/resource_budget.rs`）。
- 内置 driver 均为内存实现，重启数据丢失；生产准入见 [driver 接入指南](docs/driver-guide.md)。示例配置含三个内存 driver（`test-reference`、`test-vec`、`test-native`），其中 `test-native`（bucket `example-c`）声明原生 multipart，`test-reference` / `test-vec` 由服务层协调器补齐 multipart。
- multipart 协调器的状态是持久的（`[server].state_dir`）：`CreateMultipartUpload`/`UploadPart` 的 `200` 蕴含 session 与 part 已落盘，启动时重建并对未决完成尝试对账（`Committed` 只恢复记录、`NotCommittedFinal` 可重试、`InFlight`/`Unknown` 保留且不重放）。运行期产生的未决也能**在线**解决：重试完成或由清扫器按后端提交记录对账（[ADR-0016](docs/adr/0016-online-pending-completion-resolution.md)），不必重启、不必手改 `state_dir`。`bash scripts/e2e_fs.sh` 在客户端收到 `UploadPart` 的 `200` 后 `SIGKILL` 进程，重启后用同一 `UploadId` `ListParts` 并 `Complete`（6/6）。
