# allins3

多 driver S3 服务器。当前处于**工程预览**阶段：服务器可以启动、签名并可被真实 SDK 端到端操作；持久化后端尚未选定（P6），因此**不可用于生产**。

- [实现规范](docs/implementation-spec.md)：架构边界、通用 driver 接口、能力模型、S3 语义和验收不变量。
- [实现计划](docs/implementation-plan.md)：阶段任务、依赖、交付物和测试标准（含当前进展）。
- [兼容矩阵](docs/compatibility-matrix.md)：每个操作的真实状态、payload/checksum 范围、错误映射与验证记录。
- [driver 接入指南](docs/driver-guide.md)：如何新增真实后端并取得生产准入。
- [P0 协议适配评估](docs/p0-protocol-adapter-assessment.md)：`s3s` 候选评估与未决项。
- [ADR](docs/adr/)：架构决策记录。

## 快速开始

```bash
# 1. 校验配置（不监听端口，退出码 0/2）
ALLINS3_DEV_SECRET=devsecret cargo run --features testing -- check-config examples/dev-config.toml

# 2. 启动服务器（内存测试 driver 需要 development = true）
ALLINS3_DEV_SECRET=devsecret cargo run --features testing -- examples/dev-config.toml

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
- `src/driver/`：`StorageDriver`、`ReadNamespace`、`WriteNamespace`、`StagedWrite`、`NativeCopy`、`MultipartNamespace`、`CommitRecovery`、工厂/注册表、能力与限制模型及 Native/Emulated/Unsupported 规划。
- `src/config/` + `src/catalog.rs`：TOML 配置校验、secret 引用、bucket→namespace 不可变绑定、全局与每 principal、每 driver 并发限制、暂存配额。
- `src/service/`：GET/HEAD/PUT/DELETE/LIST/COPY、条件与 Range 的 `stat → 条件 → 区间 → 携带 revision 读取` 流程、原子发布、列举 token 的作用域绑定；multipart 两条路径共用同一套语义（固定清单与 scheme、单次发布、竞争仲裁、完成记录、过期暂存回收）—— 原生 namespace 由驱动持有 part 与完成，其余由通用协调器在 staged write 上补齐。路由由 namespace 打开时物化的能力规划决定（`copy_route` / `native_multipart`，见 ADR-0010）。
- `src/protocol/s3/`：基于 `s3s` 的 path-style HTTP/S3 适配、SigV4（header 与预签名 query）、默认拒绝的授权、请求 ID、错误映射、每 principal 准入控制。
- `src/server.rs`：启动顺序（配置 → driver → namespace 能力校验 → readiness）、优雅退出（排空后逐个 `StorageDriver::shutdown`）、健康与指标端点（`/readyz` 汇总每 driver 实例健康）。
- `src/testing/`（`testing` feature）：内存参考 driver、排序向量独立 driver、故障注入包装器、计数 sink（度量写入而不物化数据）、可复用契约 harness、服务级 fixture、进程内协议 harness（构建二进制同款 `S3Service`，由性质测试与三个模糊测试目标共用）。

## 构建与检查

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
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
- 内置 driver 均为内存实现，重启数据丢失；生产准入见 [driver 接入指南](docs/driver-guide.md)。
