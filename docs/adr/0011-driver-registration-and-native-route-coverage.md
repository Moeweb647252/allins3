# ADR-0011：driver 注册以数据表表达，原生路由跨 HTTP 验证，双 feature 构建门

状态：Accepted。日期：2026-09-26。来源：`/audit` 后的全量修复。审查发现「组合根的 kind 注册是一个带 `#[cfg]` 的 `match`，默认构建因此有死代码（3 个 warning、clippy 4 个 error 而失败）；未知 kind 的错误不说明本构建到底有哪些 kind；原生 multipart 没有任何配置入口能在真实进程里到达，进程内 HTTP harness 也只构建协调器路径；E2E 脚本静默依赖一个恰好带 `testing` feature 的预编译二进制；仓库没有任何入口会同时检查两种 feature 组合」。承接 [ADR-0010](0010-capability-plan-drives-routing.md) 的「声明即路由」，补齐它的可到达性与验证面。

## 背景

- `compose::build_registry` 用 `match kind.as_str()` 逐 kind 注册，两个臂都带 `#[cfg(feature = "testing")]`。没有该 feature 时 `match` 的所有臂都 diverge，`for` 循环因此永不循环、之后的注册语句不可达、`registry` 无需 `mut`：默认构建编译出 3 个 warning，`clippy --no-default-features -D warnings` 直接失败。新增一个 driver 意味着再改组合根里的这一段。
- 未知 kind 只报 `driver kind \`x\` is not registered in this build`，不说这个构建注册了什么；把 `test-reference` 放进默认构建得到的也是同一句话，指向性很弱。
- `full_memory()` 声明 `multipart = false`，`test-reference` 与 `test-vec` 都不是原生 multipart；`ProtocolHarness::new` 也用 `ReferenceDriverFactory::new`。因此「原生 multipart」的路线从未跨过 HTTP/SigV4——`CreateMultipartUpload` 的适配层编码、签名校验与 XML 输出只在协调器路径上被证过。
- E2E 脚本直接执行 `target/debug/allins3`，不构建、不校验 feature。默认构建的二进制会启动后立刻退出（未注册的 kind），脚本把它呈现为 `server became ready` 失败或 `Could not connect to the endpoint URL`，真正原因埋在临时 `server.log` 里。
- 仓库没有 `.github/workflows`，也没有一个脚本同时跑两种 feature 组合，因此上述默认构建的问题不会被任何自动化发现。

## 决策

1. **kind 注册是数据，不是组合根里的 `match`**：`src/driver/builtins.rs` 的 `builtins()` 返回 `Builtin { name, test, ctor }` 表，`compose::build_registry` 只查表。新增 driver = 加一行表项；feature 门（如 `testing`）只包住表项，不再影响控制流。因此没有 feature 组合会产生死代码或不可达语句。
2. **未知 kind 的错误列出本构建的 kind**：`unregistered_kind()` 用 `registered_kinds()` 拼出 `available: ...`；本构建一个 kind 都没有时明确说「this build registers no driver kinds」，而不是把「默认构建」和「拼错 kind」混为一谈。
3. **`instance_from_factory` 收进 `testing` feature**：它只被 fixture、契约 harness 与 `handles` 的测试使用；公开给默认构建只会是死代码。
4. **原生 multipart 必须有配置入口**：新增 `ReferenceDriverFactory::native_multipart`（在 `full_memory` 上声明 `multipart = true`）与内置 kind `test-native`，并在 `examples/dev-config.toml` 增加 `instance-c` / `example-c`。这样真实进程也能走到原生路线，与协调器路径并存对照。
5. **原生路线跨 HTTP/SigV4 被验证**：`ProtocolHarness::new_native_multipart()` 在同一套真实 dispatch 上用原生能力构建服务；新增用例 `the_native_multipart_route_is_reached_through_the_signed_wire` 用签名请求完成 create → part → complete，并断言状态与内容。
6. **路线判定只有一个所有者**：`upload_route` 是「原生 session 的 namespace 不再提供原生 multipart」这一条件的唯一报错点，且现在把打开后的 `Namespace` 一并返回；`complete_multipart_native` 与 `abort_multipart` 改用它，不再各自复制一段 `native_multipart().cloned().ok_or_else(Internal)`。为让测试能证明路线，`ObjectService::upload_is_native` 暴露 session 上已记录的归宿（`None` 表示该 upload 不在这里，不等于「协调器」）。
7. **`CapabilityPlan` 按枚举下标存储**：`entries: [FeatureSupport; PlannedFeature::ALL.len()]`，`support` 是一次索引而不是线性扫描；未规划的功能自然读作 `Unsupported`。删除无人调用的 `all_provided` 与 `iter`（脚手架）——前者只是 `support` 的另一层表达，后者零调用且会把「按枚举顺序」的语义偷偷改掉。
8. **E2E 脚本把 feature 依赖显式化**：自建服务器的脚本（`e2e_cli.sh` / `e2e_drain.sh` / `e2e_timeouts.py`）在未由 `ALLINS3_BIN` 指定二进制时先 `cargo build --features testing`；服务器未就绪时打印服务器日志尾部，而不是只报「never became ready」。
9. **一个入口检查两种 feature 组合**：`scripts/check.sh` 跑 fmt、两种 feature 的 clippy、两种 feature 的测试与 spike；`.github/workflows/ci.yml` 只调用它。CI 与贡献者因此跑同一组命令。脚本的注释如实说明默认 `cargo test` 只跑库单元测试（`tests/` 目标都要求 `testing` feature），并说明尚未接入 `cargo deny`（ADR-0005 的未决项），不把它自称成「全部验证门」。
10. **测试 driver 的判定归表，不靠名字前缀**：`Builtin.test` 是「哪些 kind 是测试 driver」的唯一来源，配置的生产守卫（`development = false` 时拒绝测试 driver）改用它。这样真实后端即使叫 `test-*` 也不会被误判，非 `test-` 命名的测试 driver 也不会漏过。该判定只在 `testing` feature 下有测试 driver 可测，对应单测随之 gate 在该 feature 下。
11. **`check-config` 对未注册 kind 在启动前失败**：新增 `server::preflight`（委托 `compose::build_registry`），`check-config` 调用它后再打印 config ok。`kind` 是否可构造是本构建的属性而非文件的属性，此前只有 `run` 会撞上它；现在 `check-config` 即 `§7.1` 所说的启动前失败。
12. **固定的 CI 工具链**：`.github/workflows/ci.yml` 从浮动的 `dtolnay/rust-toolchain@stable` 改为固定 `@nightly-2026-09-15`，与本 ADR 记录的本地环境一致，并在 ADR-0005 回填。
13. **去掉一个纯转发**：`protocol/s3/checksum.rs` 的 `decode_base64` 只是转发 `integrity::decode_base64`，删掉并在调用点直接引用，不新增 `pub(super)` 缝隙。
14. **拆开两个最大文件里可命名的关注点**：逐次纯搬移，不改逻辑。`src/protocol/s3/` 从单个 2352 行的 `mod.rs` 变为四个模块：`mod.rs`（`S3Handler`、`impl S3`、`PrincipalLimiter`、auth、`build_service`）与 `body.rs`（驱动/协议字节流桥接与请求体错误归类）、`checksum.rs`（校验和头解析与渲染）、`convert.rs`（DTO ↔ domain 转换、时间格式、metadata、条件、range、列举编码），共 1524 + 173 + 343 + 354 行。`src/service/multipart/` 增加 `rules.rs`（marker、scheme 定案、part 摘要、两条路径共用的唯一清单校验），`multipart.rs` 从 2187 行降至 1917 行。模块的公开面不变（仍是 `protocol::s3` / `service::multipart`），子模块项用 `pub(super)` 暴露，验证仍由既有测试完成。

## 后果

- 新增 driver 的步骤写进 [driver 接入指南](../driver-guide.md)：实现接口 → 在 `builtins()` 加一行 → 配置里引用 kind。组合根、服务与协议层都不改动（`INV-01`）。
- 「声明即路由」不再只是服务层内部的等价性：原生 multipart 现在有真实进程入口与 HTTP/SigV4 证据，与协调器路径在 `tests/service_multipart.rs` 和新的协议用例下同套断言。
- 默认构建在 `-D warnings` 下干净；两种 feature 组合都由 CI 守住，此前「默认构建有告警但无人跑它」的空档被 `scripts/check.sh` 填上。
- `test-native` 与 `test-reference` / `test-vec` 同属内存测试 driver，仍受 `development = true` 与 `Builtin.test` 标记约束，不构成生产准入；`check-config` 也会拒绝本构建未注册的 kind。
- 证据：`cargo fmt --check`、两种 feature 的 `clippy -D warnings` 干净；`scripts/check.sh` 全绿（含 `tests/property_protocol.rs` 新增的原生 HTTP 用例），132 个单元测试 + 77 个集成测试全绿。

## 替代方案

- **只给 `match` 加 `#[allow]` 或以 `#[cfg]` 分支包裹注册循环**：能让 lint 闭嘴，但「新增 driver 要改组合根」和「错误不说明可用 kind」两个问题都还在。被否决。
- **把 `full_memory()` 直接改成 `multipart = true`**：一处即让所有内存 driver 走原生路线，但会同时改变契约 harness 与协调器路径的既定覆盖，等于用一次语义翻转换取入口。改为新增 `native_multipart()` 构造与独立 kind，两条路径各自可指。被否决。
- **不加 `test-native`，只靠进程内 harness 验证原生路线**：进程内 harness 已足够证明 HTTP/SigV4 层，但不留配置入口意味着人工/脚本无法在真实进程里复现原生路线。两者都做。被否决。
- **为 `upload_is_native` 加一个测试专用 feature 或 `#[cfg(test)]`**：这是集成测试（`tests/`）而非单元测试，`#[cfg(test)]` 不可见；该访问器本身也是有意义的运维诊断（「这个 upload 走哪条路线」），保留为公开方法。被否决。
