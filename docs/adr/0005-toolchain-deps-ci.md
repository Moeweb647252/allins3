# ADR-0005：Toolchain、依赖固定、CI 与字节上限

状态：Accepted。日期：2026-09-22。对应 P0-05。

## 背景

接口工程预览需要可复现的构建与检查命令，避免依赖漂移导致契约测试结果不可信。

## 决策

1. **Edition**：2024。**MSRV**：`rust-version = "1.96"`（由候选 `s3s` 0.16.1 的 MSRV 约束；也满足 let-chains 所需的 ≥1.88）。
2. **直接依赖精确固定**（`=` 版本）：
   - `async-trait = 0.1.92`（规范要求）
   - `bytes = 1.12.1`（规范要求）
   - `futures-core = 0.3.34`
   - `thiserror = 2.0.20`
   - `tokio = 1.53.1`
   - `tokio-util = 0.7.19`
   - dev：`futures-util = 0.3.34`、`tokio`（`full`）
3. **许可证**：直接依赖需为 `Apache-2.0`/`MIT`/`MIT-0`/`ISC`/`BSD-*` 等宽松许可；`s3s` 为 `Apache-2.0`。CI 需加入 `cargo deny`（或等价）做许可证与重复依赖检查（工具引入见 P0 后续）。当前仓库暂无该工具。
4. **CI 命令**（所有任务完成条件）：
   - `cargo fmt --check`
   - `cargo clippy --all-targets --all-features -- -D warnings`
   - `cargo test --all-features`
   - 独立 spike：`cd spikes/protocol-adapter && cargo test`
5. **协议 profile 字节上限**（服务配置取协议、服务配置、driver 限制的交集）：
   - 单对象 key：1–1024 UTF-8 字节
   - 单次非分片写：≤5 GiB
   - 单个对象（首期目标上限）：≤48.8 TiB（协议层，用 `u64`；不得把旧 5 TiB 写成永久上限）
   - part number：1–10000；非最后 part 最小 5 MiB，最大 5 GiB；单页最多 1000
   - XML body：默认 10 MiB（`s3s` 默认）；用户元数据：≤100 条、≤8 KiB（见 `domain::metadata`）
6. **Toolchain 固定状态**：当前工作环境为 `nightly-aarch64-apple-darwin`，`rustc 1.100.0-nightly (215a8af4b 2026-09-15)`。本仓库暂**不**添加 `rust-toolchain.toml`，以免在未安装该日期工具链的机器上触发自动下载；CI 必须固定一个带日期的 nightly（或稳定版 ≥1.96）并回填本文件的哈希。

## 后果

- 依赖升级必须显式修改本 ADR 并重新跑契约测试与兼容矩阵。
- 若 CI 无法提供 ≥1.96 的稳定工具链，需在 P0 结束前固定带日期 nightly。

## 未决项

- `cargo deny`/许可证扫描尚未接入。
- 具体 CI 平台的运行时版本需在 P0 收尾时固定并回填。
