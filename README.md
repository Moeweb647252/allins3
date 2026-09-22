# allins3

多 driver S3 服务器项目。当前处于**接口工程预览**阶段：领域模型、driver 接口、能力模型与契约测试已实现；HTTP 服务、签名验证与具体生产后端尚未实现。

- [实现规范](docs/implementation-spec.md)：架构边界、通用 driver 接口、能力模型、S3 语义和验收不变量。
- [实现计划](docs/implementation-plan.md)：阶段任务、依赖、交付物和测试标准（含当前进展）。
- [兼容矩阵](docs/compatibility-matrix.md)：操作、参数、payload/checksum、错误映射与官方引用（目标矩阵，尚未实现）。
- [P0 协议适配评估](docs/p0-protocol-adapter-assessment.md)：`s3s` 候选评估与未决项。
- [ADR](docs/adr/)：架构决策记录。

## 当前能力

- `src/domain/`：强类型 ID、`ObjectKey`、`ObjectMetadata`/`ObjectRevision`、checksum、`DriverError`（含 `CommitState` 四态）、`DriverContext`、`ResolvedRange`、列举请求/游标。
- `src/driver/`：`StorageDriver`、`ReadNamespace`、`WriteNamespace`、`StagedWrite`、`NativeCopy`、`MultipartNamespace`、`CommitRecovery`、工厂/注册表、能力与限制模型及 Native/Emulated/Unsupported 规划。
- `src/testing/`（`testing` feature）：内存参考 driver、排序向量独立 driver、故障注入包装器、可复用契约测试入口。

`src/main.rs` 仍为占位程序，不启动服务器。

## 构建与检查

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features

# P0 协议适配 spike（独立 workspace，不影响主 crate 依赖）
cd spikes/protocol-adapter && cargo test
```

配置、接口和功能均为实现目标；除已在契约测试中验证的行为外，尚不代表已有能力。测试 driver 不是生产后端，也不能用于满足生产持久性验收。
