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

# 3. 用真实 SDK 走一遍完整闭环
ALLINS3_SECRET_KEY=devsecret python3 scripts/e2e_boto3.py     # 需要 boto3
```

服务器端点：`/healthz`（liveness）、`/readyz`（readiness，排空时 `503`）、`/metrics`（计数器），以及 path-style 的 S3 API。示例配置见 [`examples/dev-config.toml`](examples/dev-config.toml)。

## 当前能力

- `src/domain/`：强类型 ID、`ObjectKey`（key 字节原样保留）、`ObjectMetadata`/`ObjectRevision`、checksum、`DriverError`（含 `CommitState` 四态）、`DriverContext`（超时与取消）、`ResolvedRange`、列举请求/游标。
- `src/driver/`：`StorageDriver`、`ReadNamespace`、`WriteNamespace`、`StagedWrite`、`NativeCopy`、`MultipartNamespace`、`CommitRecovery`、工厂/注册表、能力与限制模型及 Native/Emulated/Unsupported 规划。
- `src/config/` + `src/catalog.rs`：TOML 配置校验、secret 引用、bucket→namespace 不可变绑定、全局与每 principal 并发限制、暂存配额。
- `src/service/`：GET/HEAD/PUT/DELETE/LIST/COPY、条件与 Range 的 `stat → 条件 → 区间 → 携带 revision 读取` 流程、原子发布、列举 token 的作用域绑定、通用 multipart 协调器（单次发布、竞争仲裁、完成记录、过期暂存回收）。
- `src/protocol/s3/`：基于 `s3s` 的 path-style HTTP/S3 适配、SigV4（header 与预签名 query）、默认拒绝的授权、请求 ID、错误映射、每 principal 准入控制。
- `src/server.rs`：启动顺序（配置 → driver → namespace 能力校验 → readiness）、优雅退出、健康与指标端点。
- `src/testing/`（`testing` feature）：内存参考 driver、排序向量独立 driver、故障注入包装器、可复用契约 harness、服务级 fixture。

## 构建与检查

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features

# P0 协议适配 spike（独立 workspace，不影响主 crate 依赖图）
cd spikes/protocol-adapter && cargo test
```

## 已知限制（与兼容矩阵一致）

- 仅 path-style；`addressing = "virtual"` 在校验阶段明确失败（未实现）。
- 不支持 Versioning、SSE、ACL、Object Lock、生命周期、通知、复制、网站托管、Select、SigV2/SigV4a；这些参数会明确报错而不是被忽略。
- multipart 不合成 COMPOSITE/FULL_OBJECT checksum，只发布 `ETag = md5(parts)-N`。
- `aws-chunked` 请求体与 checksum trailer 由 `s3s` 解码，但本仓库尚未用真实客户端构造该用例，因此不作为已验证能力。
- 内置 driver 均为内存实现，重启数据丢失；生产准入见 [driver 接入指南](docs/driver-guide.md)。
