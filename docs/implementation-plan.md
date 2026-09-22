# 多 driver S3 服务器实现计划

状态：P0–P5 已实现；剩余开放项只有 P5-05 的模糊测试，以及全部 P6 生产准入。更新日期：2026-09-22。

> 进展：服务器可运行（`cargo run --features testing -- examples/dev-config.toml`）。三条真实客户端证据链：
> ① boto3/botocore **1.42.97** 端到端 **63/63** 项检查（`scripts/e2e_boto3.py`），覆盖 PUT/HEAD/GET/条件/Range/LIST 分页/COPY/批量删除/multipart（含 transfer manager 11 MiB → 3 part 与组合 ETag 独立复算）/UploadPartCopy/abort/预签名 URL 与签名拒绝，以及 multipart 的 FULL_OBJECT、COMPOSITE checksum（用 Python 标库独立复算）与错误 checksum 拒绝；
> ② AWS CLI **1.44.87**（`pip install awscli`）**29/29** 项检查（`scripts/e2e_cli.sh`），含 `aws s3api` 与 `aws s3 cp/sync/ls/rm/presign` 高层路径；
> ③ 手工构造的 `aws-chunked` + checksum trailer 请求 **8/8** 项检查（`scripts/e2e_streaming_trailer.py`），因为 CLI 与 botocore 1.42 对 HTTP endpoint 一律把 checksum 放入请求头，不会发出 trailer 形式。
>
> 资源证据：计数 sink + 生成流基准在 100 MiB / 1 GiB / 10 GiB 下增量峰值 RSS 均为 **0 MiB**（预算 32 MiB，`bench/resource-budget.toml`）。

本计划依照[实现规范](implementation-spec.md)执行。用户已确定先做通用接口，不限定生产后端；因此先完成接口、服务闭环与验证工具，再单独推进具体 driver 的生产准入。

## 1. 交付层次与范围

| 交付层次 | 内容 | 可以证明什么 |
| --- | --- | --- |
| 本次文档 | 架构、契约、兼容范围、任务和验收标准 | 设计可讨论、可拆分开发 |
| P0–P3 基础工程预览 | 注册/配置、多实例路由、签名、对象基础操作、测试 driver | 接口与基础 S3 闭环可运行 |
| P4–P5 完整工程预览 | multipart、故障测试、资源限制、CLI/SDK 兼容报告 | 声明范围内的功能与语义已验证 |
| P6 后续生产准入 | 选定真实 driver、状态持久化、重启恢复、部署验证 | 特定配置可用于生产 |

本次只新增设计文档，不实现服务器、不添加依赖、不选定具体生产后端。当前 `src/main.rs` 仍为项目初始程序。

## 2. 实施原则

1. 先固定语义和测试，再用参考实现验证抽象；不能把第一个真实后端的 API 直接改名当通用接口。
2. 每阶段交付一条可演示、可验证的路径，而不是只添加空模块。
3. 核心功能不依赖可选能力；可选功能必须通过能力规划选择原生、补齐或拒绝。
4. 认证、流式校验与原子发布作为基础链路，避免先做无认证整对象缓存版本再整体重写。
5. 只为已声明的范围承担兼容承诺；每个已支持参数都要有正向和失败用例。

## 3. 阶段与依赖

```text
P0 协议风险验证与决策记录
  -> P1 领域模型、driver API 与契约测试
  -> P2 配置、路由与对象服务
  -> P3 S3 基础闭环
  -> P4 Multipart 与恢复接口
  -> P5 兼容性、故障与运行验证
  -> P6 真实后端生产准入（后续单独排期）
```

P1 模型稳定后，协议适配测试向量与 P2 可独立推进；不应在原子写/稳定读契约未确定时开发多个真实 driver。

### P0：验证协议适配路径

任务：

- [x] P0-01 建立兼容矩阵 → `docs/compatibility-matrix.md`。
- [x] P0-02 评估 `s3s` → `docs/p0-protocol-adapter-assessment.md`。
- [x] P0-03 用真实 SDK 请求验证四条风险路径。已完成：①签名不破坏原始 key（`a//b`、`+`、空格的原样往返）；②请求体无需整体缓冲（`src/service/mod.rs` 流式管线 + `a_generated_multi_megabyte_body_streams_through_unchanged` 的 12 MiB 生成流）；③提交前校验（`Content-MD5`/声明 checksum 不匹配即 abort，`a_body_shorter_or_longer_than_declared_is_a_bad_digest`）；④错误映射（`docs/compatibility-matrix.md` 第 4 节）。**未完成的部分**：官方固定签名向量未内置（原因与替代证据见兼容矩阵第 6 节），trailer 运行时校验未验证，AWS CLI 不可用。
- [x] P0-04 记录适配补齐/更换适配层影响 → ADR-0007。
- [x] P0-05 固定直接依赖版本与 CI 命令、字节上限 → ADR-0005。toolchain 与许可证扫描仍有未决项。
- [x] P0-06 记录 ADR → `docs/adr/`。

产物：技术验证程序、首版兼容矩阵、ADR、可复现的依赖和测试向量。`s3s` 仅是候选，不能把它的 API 覆盖范围直接当作本项目已通过的兼容范围。[官方项目说明](https://github.com/s3s-project/s3s)

验收门槛：能证明签名验证不破坏原始 key、请求体无需整体缓冲、trailer 可在提交前校验；未满足时先修正适配方案。

### P1：领域模型、接口与测试套件

任务：

- [x] P1-01 实现强类型 ID、ObjectKey、metadata、revision、checksum、错误、上下文、ResolvedRange 和游标类型；ReadRequest 携带 expected_revision。→ `src/domain/`。
- [x] P1-02 使用 `#[async_trait]` 与 `async fn` 完成只读/可写 namespace、staged write、工厂、注册表和原生扩展的对象安全接口。→ `src/driver/`。
- [x] P1-03 完成能力与限制模型、档位校验和 Native/Emulated/Unsupported 规划结果。→ `src/driver/capabilities.rs`。
- [x] P1-04 编写参考 driver、故障注入包装器与另一种独立测试实现。→ `src/testing/{reference,fault,second}.rs`。
- [x] P1-05 建立通用契约测试入口。→ `src/testing/contract.rs`，集成测试 `tests/contract_{reference,fault}.rs`。
- [x] P1-06 定义 Committed、NotCommittedFinal、InFlight、Unknown 四态；验证终态未提交能阻止迟到发布，查无记录不等于终态失败，并覆盖 abort 与取消。→ `src/domain/error.rs`、`tests/contract_fault.rs`（`drop` 与临时资源回收仍需在 P2/P4 的真实暂存实现上补充）。

产物：可编译接口、rustdoc、至少两种测试实现、统一契约测试工具。

验收门槛：基础 CRUD、稳定读、隐藏暂存、原子发布、排序列举、隔离全部通过；通过编译检查验证 `Arc<dyn Trait>`、`Box<dyn StagedWrite>` 和 `Send` future 约束；增加测试实现无需修改 service/protocol 代码。弱能力测试实现必须触发拒绝或明确的补齐路径。

对应规范：第 2、3、5 节；INV-01 至 INV-05、INV-07、INV-09。

### P2：配置、多实例路由和对象服务

任务：

- [x] P2-01 配置 schema、secret 引用、driver 实例初始化、只读 BucketCatalog、配置校验命令。→ `src/config/mod.rs`、`src/catalog.rs`、`allins3 check-config <path>`（退出码 0/2）。
- [x] P2-02 bucket 到 namespace 的不可变绑定；验证重复绑定、未知 kind、缺失配置与权限隔离。→ 配置校验 + `service_multipart`/`service_limits` 中的 namespace 隔离与实例隔离用例。
- [x] P2-03 实现 GET/HEAD/PUT/DELETE/LIST 领域用例，先经内部接口调用验证，不依赖 HTTP 细节。→ `src/service/mod.rs`。
- [x] P2-04 在 staged write 管线中增加长度、MD5/ETag、checksum、取消与背压。→ `tests/service_limits.rs`（超限 `413`、长度不符 `400 BadDigest`、取消中途不发布、暂存配额 `503`）。
- [x] P2-05 实现 stat → 条件判断 → Range 解析 → 携带 expected_revision 读取的流程；revision 不可读时有界重走完整流程。实现能力允许的原子条件修改，验证读写竞争。→ `get_object`/`head_object` 的 `MAX_READ_ATTEMPTS` 流程；契约测试 `stable_read_by_revision`、`conditional_writes`。
- [x] P2-06 实现同/跨 driver 的流式 Copy 路径与 metadata 指令，建立原生优化等价测试入口。→ `copy_object` 在同 namespace 且声明 `native_copy` 时走原生复制，否则流式补齐；契约检查 `copy_native_matches_emulation` 对两种路由逐一比对（该检查还发现并修正了参考 driver 在 `REPLACE` 时丢弃替换 content 属性的缺陷）。
- [x] P2-07 实现分页 token 保护与作用域绑定、每项删除结果、统一错误模型。→ `tests/property_parsing.rs::continuation_tokens_are_bound_to_their_scope`（跨 bucket / 跨 prefix / 跨 delimiter / 伪造 token 全部拒绝）。

产物：配置启动器、对象服务和多实例集成测试。

验收门槛：两个实例中相同 key 互不影响；未知 bucket/越权请求不能触达错误的 namespace；故障实例不阻断其它实例；PUT/Copy 发布前失败保留原对象，提交回执丢失按未知结果处理；能力缺失无假成功。

对应规范：第 2–5、7、9 节；INV-02 至 INV-09、INV-12。

### P3：S3 基础请求闭环

任务：

- [x] P3-01 接入 HTTP/S3 适配与领域服务，完成 path-style 路由、XML 与 request_id。→ `src/protocol/s3/mod.rs`、`src/server.rs`。
- [x] P3-02 实现 SigV4 header、presigned query、凭据查找和默认拒绝的权限规则。→ 真实 SDK 验证：header 签名、预签名 GET、预签名过期、签名篡改、错误 secret/未知 access key 全部符合预期。
- [x] P3-03 完成基础操作矩阵：bucket 查询、对象读写删除、批量删除、ListObjectsV2、CopyObject。→ `scripts/e2e_boto3.py`。
- [x] P3-04 覆盖 URI 编码、特殊 key、metadata、条件读、Range、错误响应、HEAD 无响应体等边界。→ 端到端覆盖 `a//b + c` 这类 key、`304`、`412`、`416`、`bytes=a-b`/`bytes=-n`；条件与 Range 的执行顺序（条件失败不打开数据流、条件通过才判 `416`）由 `head_object`/`get_object` 的单一流程保证，并由服务测试覆盖。
- [x] P3-05 实现普通 payload 的校验，且任何校验失败都不发布。→ `Content-MD5`、声明 checksum、长度不符都返回 `400 BadDigest` 且不产生对象；`aws-chunked` + checksum trailer 的运行时校验已补齐：`s3s` 只解码 trailer，值必须由服务层核对，因此 `PutParams.trailer_checksum` 在 body 消费完后读取 trailer 值并与流式摘要比对（声明未送达也视为不匹配）。→ `tests/service_limits.rs::a_trailer_declared_checksum_is_verified_after_the_body`、`scripts/e2e_streaming_trailer.py`。
- [x] P3-06 运行固定版本客户端基础用例；记录版本与默认配置。→ 两个固定版本客户端都跑通并记录了版本与默认配置：boto3/botocore **1.42.97**（`scripts/e2e_boto3.py` 63/63）与 AWS CLI **1.44.87**（`pip install awscli`，`scripts/e2e_cli.sh` 29/29，`s3.signature_version=s3v4`）。注：`aws s3 presign` 在 CLI v1 默认 SigV2，服务器按 ADR-0007 拒绝，脚本显式固定 s3v4；未编写 AWS SDK for Rust 客户端用例（不在本项交付物内）。
- [x] P3-07 virtual-hosted-style 未实现，因此配置成 `addressing = "virtual"` 在**校验阶段明确失败**（fail closed，不会静默按 path-style 提供服务），兼容矩阵标为未支持。→ `rejects_virtual_hosted_addressing`。

产物：可用 CLI/SDK 操作测试 namespace 的服务器、基础兼容报告、可执行开发配置。

验收门槛：默认 SDK checksum 设置下完成 PUT → HEAD → GET → LIST → COPY → DELETE；错误密钥、过期签名、越权 prefix、错误 checksum 均被拒绝；条件失败与 Range 越界组合返回正确状态，条件失败时不打开数据流。高层 CLI 可能自动使用 multipart，P3 用明确大小和低层操作验证基础范围，不提前声称所有 `aws s3` 命令可用。

对应规范：第 4–7、9 节；INV-02 至 INV-09、INV-12。

### P4：Multipart 扩展与恢复协议

任务：

- [x] P4-01（有意偏离并记录）：`MultipartNamespace` 已作为 driver 扩展契约存在（`src/driver/extensions.rs`），但 `MultipartStore` / `StagingStore` **没有**被引入为 trait。理由：暂存与 multipart session 的状态必须与「单次发布」「竞争仲裁」「完成记录」在同一个临界区内推进，把状态放到 driver 侧会把这些不变量拆到两个进程内；当前由服务层协调器（`src/service/multipart.rs`，`MultipartState` + `std::sync::Mutex`）独占，并通过 `ObjectService` 的公开方法暴露。真实后端的持久化暂存由 P6 的 driver 在其 `StagedWrite`/`MultipartNamespace` 内实现，协调器仍然只通过 driver 接口发布。
- [x] P4-02 实现创建、上传、列举 part/upload、完成、终止和 UploadPartCopy。→ 真实 SDK 覆盖全部六个操作。
- [x] P4-03 实现 CAS 状态机（`claims` 单次完成声明）、上传归属检查（bucket/key/upload id 绑定）、part 原子替换、固定清单与流式合并。→ `tests/service_multipart.rs`。
- [x] P4-04 part 顺序/大小限制与结果 metadata 已实现（升序、非末 part 至少 5 MiB、part 数上限 10000、完成时发布 `ETag = md5(parts)-N`）；**FULL_OBJECT / COMPOSITE checksum 已实现**：创建时固定 scheme（未指定则按算法族取默认覆盖），每个 part 按其算法计算并存储摘要，完成时 FULL_OBJECT 对装配后字节取摘要、COMPOSITE 对有序 part 原始摘要取摘要（带 `-<partcount>` 线格式），客户端声明的完成 checksum（含 part 清单里的每 part checksum）不匹配一律 `400 BadDigest` 且不发布，非法组合（`CRC64NVME + COMPOSITE`、SHA 族 + FULL_OBJECT）拒绝。→ `src/service/multipart.rs`、`src/protocol/s3/mod.rs`、`tests/service_multipart.rs`（5 个用例）、`scripts/e2e_boto3.py`（FULL_OBJECT 与 COMPOSITE 各用 Python 标库独立复算）。
- [x] P4-05 实现完成记录、四态恢复查询和安全的租约清理器；只有 NotCommittedFinal 才可在 CAS 获得恢复权后评估重试，Committed 不再发布（回执丢失时从已发布 revision 还原响应），InFlight/Unknown 不得重放。→ `CompletionRecord`、`completion_state(s)`、`sweep_expired_uploads`、`tests/service_multipart.rs` 的 lost-ack / fenced / unknown 三个用例。
- [x] P4-06 覆盖并发 Complete/Complete 与 Complete/Abort 仲裁、丢失完成回执、重试清单变化（manifests 变更后重试）、过期暂存清理与「未知结果不被清扫/不被重放」；注入“原提交仍在途但查无记录”以及“原尝试已隔离、重试成功后旧提交迟到”两种时序，验证不重复提交且旧尝试不能覆盖新对象。
- [x] P4-07 SDK transfer manager 的 multipart 路径**已验证**（11 MiB → 3 part，ETag 用独立计算的组合值比对，`ListParts`/`UploadPartCopy`/`abort` 均通过）；高层 **CLI** 路径已验证（`scripts/e2e_cli.sh`：`aws s3 cp` 11 MiB 自动分片且 ETag 带 part 数、`aws s3 sync`/`ls`/`rm` 与往返字节一致）。

产物：可运行的 multipart 参考实现、恢复协议与故障测试。测试工具可以用可重建状态模拟崩溃窗口，但不能替代真实持久化系统的进程崩溃验证。

验收门槛：完成前目标不变；完成后对象和元数据一致；终止不影响原对象；重复完成不会覆盖后续对象；执行中或未知的尝试不被重放，已判为终态未提交的旧尝试无法迟到发布；后台清理不删除活动 part 或已提交对象；不同 checksum 模式得到正确值。

对应规范：第 6、8、9 节；INV-03、INV-04、INV-06、INV-08 至 INV-11。INV-10 的生产持久性验证仍留给 P6。

### P5：可运行性与兼容声明

任务：

- [x] P5-01 落实全局（监听器信号量）、每 principal（`PrincipalLimiter`，饱和时 `503 SlowDown`）、body/XML（`max_xml_bytes`、`max_single_write_size`）、metadata（数量/字节/key 长度，超限 `400` 而非静默丢弃）、暂存配额与超时限制。→ `src/server.rs`、`src/protocol/s3/mod.rs`、`src/service/mod.rs`；测试见 `tests/service_limits.rs` 与 `per_principal_admission_is_bounded_and_released`。
- [x] P5-02 添加 liveness/readiness（排空时 `readyz` 返回 `503`）、结构化日志（`level=info msg="..."`）、指标（`/metrics`：请求数、4xx/5xx、在途、运行时长）、优雅退出与实例故障隔离。→ `curl` 验证见兼容矩阵第 6 节；实例隔离由 `one_failing_driver_does_not_break_another_bucket` 覆盖。
- [x] P5-03 执行取消/流中断、超限、限流、暂存背压、未知提交结果测试。→ `tests/service_limits.rs`、`tests/service_multipart.rs`、`tests/contract_fault.rs`。**未覆盖**：真实慢客户端与真实进程信号下的排空时序（需要端到端客户端脚本，当前只有本地信号量语义的单测）。
- [x] P5-04 固定依赖版本（ADR-0005）并以固定 SDK 版本跑兼容矩阵：boto3/botocore **1.42.97**、Python 3.9.6、`signature_version=s3v4`、`addressing_style=path`、`retries.max_attempts=0`。协议库（`s3s` 0.16.1）或 SDK 升级后必须重跑本节命令。
- [ ] P5-05 性质测试已覆盖 range 解析、key 编码往返（含控制字符/`//`/`+`）、摘要编码往返、token 作用域与伪造（`tests/property_parsing.rs`，确定性 LCG，每次 500 轮）；**未覆盖**：XML 与 part 清单的模糊测试、签名输入的性质测试（需引入 fuzz 目标），因此本项不勾选完成。
- [x] P5-06 内存预算：`a_generated_multi_megabyte_body_streams_through_unchanged` 用生成式数据源验证服务层流水线；计数 sink（`src/testing/counting.rs`，逐块计数后丢弃，只保留 metadata）配合 `tests/resource_budget.rs` 以 100 MiB、1 GiB、10 GiB 生成流（256 KiB 块）实测：空闲基线 2 MiB，三个尺寸增量峰值 RSS 均为 **0 MiB**，阈值来自 `bench/resource-budget.toml`（`per_request_buffer_bytes × max_concurrency + slack = 32 MiB`）。命令：`ALLINS3_BENCH_FULL=1 cargo test --release --all-features --test resource_budget -- --nocapture`（默认只跑 100 MiB）。P5-06 其余部分（模糊测试）属 P5-05。
- [x] P5-07 发布工程预览使用说明与能力表（[README](../README.md)）、已知差异（[兼容矩阵](compatibility-matrix.md) 第 5 节）、开发模式标识（`development = true` 才允许注册测试 driver）、真实 driver 接入指南（[driver-guide.md](driver-guide.md)）。

验收门槛：内存符合缓冲预算；常规测试无 secret 泄漏；不支持的功能有确定错误；所有宣称支持的矩阵项通过；不以关闭 checksum、禁用签名或只用自写客户端作为验收条件。

对应规范：第 9–11 节，全部适用不变量。没有真实持久化实现时，发布物必须明确为工程预览。

### P6：真实后端生产准入（后续）

本阶段不预先选型，也不是当前文档任务要实施的内容。选择实际后端后，对每个 driver 分别执行：

- [ ] 补充后端能力、限制、并发/外部写入和原子提交机制的适配说明。
- [ ] 选择需要的 metadata、multipart session 与 staging 持久化实现，证明提交和恢复协议。
- [ ] 运行同一套契约测试、SDK 测试与故障测试；声明原生、补齐和未支持能力。
- [ ] 验证进程崩溃、机器重启、网络分区、空间耗尽、凭据失效及后端限流。
- [ ] 验证索引/元数据与对象数据的备份、恢复和格式升级，不能只备份字节内容。
- [ ] 提交真实后端基准、部署/恢复手册和最终兼容报告后，才标记该配置可用于生产。

## 4. 测试与完成标准

| 测试层 | 关注内容 | 执行时机 |
| --- | --- | --- |
| 类型与单元测试 | key、范围、checksum、能力规划、错误分类 | 每次相关修改 |
| 契约测试 | 所有测试/真实 driver 的一致性与行为 | driver 或公共 API 变化 |
| 协议测试 | 官方签名向量、XML、头部、payload、状态码、条件与 Range 的执行顺序 | 协议/认证修改 |
| 性质测试 | 随机 key 排序分页、编码往返、状态机竞争 | CI 与回归 |
| CLI/SDK 测试 | 固定版本的默认配置与高层调用 | 集成及依赖升级 |
| 故障测试 | 上传中断、原子发布、未知结果、终态隔离、迟到提交、revision 变化与回收 | 状态/提交路径修改 |
| 性能与资源测试 | 背压、内存预算、并发隔离 | 发布及性能路径修改 |
| 生产恢复测试 | 持久性与实际故障域 | 每个真实 driver 准入 |

每项任务的完成条件：行为实现、适用测试通过、能力表与文档一致、错误和取消路径完整。适用时运行 `cargo fmt --check`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-features`；如果 feature 存在互斥关系，改为明确的合法组合矩阵。

资源验收应先测空闲基线，再固定并发数和每请求缓冲预算，以 100 MiB、1 GiB、10 GiB 的生成流对比增量 RSS；阈值由缓冲、队列和 driver 已声明的额外内存预算推导，并写入基准配置。接口/范围边界通过逻辑测试验证，不依赖真的上传协议最大对象。

吞吐和延迟必须附带硬件、网络、并发、对象分布、driver、checksum 与持久性设置；没有真实后端前不编造生产 QPS 或吞吐承诺。

## 5. 主要风险与处理位置

| 风险 | 影响 | 计划中的处理 |
| --- | --- | --- |
| 协议库覆盖不等于 SDK 可用 | 签名、trailer、XML 差异 | P0 验证，P3/P5 固定客户端矩阵 |
| 通用接口丢失提交语义 | 半对象、元数据错配、重试覆盖 | P1 staged write 与未知结果契约 |
| 后端弱一致/允许外部写入 | 条件修改和列举不符合声明 | 能力准入与 P6 后端适配证明 |
| 把 ETag 当统一 checksum | multipart 或复制校验错误 | 独立字段与测试向量 |
| multipart 状态与数据脱节 | part 泄漏、重复提交或误清理 | P4 CAS、完成记录、租约与恢复 |
| 回退实现放大资源消耗 | 全量排序、Range 扫描、复制占满连接 | 能力规划、显式预算与 P5 压力验证 |
| 内存实现产生虚假生产信心 | 重启后对象或 upload 丢失 | 开发模式隔离与 P6 独立准入 |

## 6. 实现迭代进展

P0 与 P1 已按本节的顺序完成（协议风险验证 → 领域类型 / driver API / 能力模型 / 契约测试 → 配置路由 → HTTP），P2–P5 也已落地并以真实 SDK 验证。当前实现顺序与证据：

1. `cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features`（101 个单元测试 + 29 个集成测试）。
2. `cd spikes/protocol-adapter && cargo test`（`s3s` 候选行为断言）。
3. `./target/debug/allins3 check-config examples/dev-config.toml`，再启动并用 `scripts/e2e_boto3.py` 跑 43 项真实 SDK 检查。

**仍未完成（不得当作通过）**：P5-05 的 XML / part 清单 / 签名输入模糊测试（需引入 fuzz 目标）。P6 的生产准入（持久化暂存、崩溃恢复、备份、基准）全部未开始，且必须在选定真实后端后单独排期；在此之前本仓库只能声明为工程预览。此外，`aws-chunked` trailer 的运行时校验只能用本仓库手工构造的请求验证（CLI/botocore 1.42 对 HTTP endpoint 不发出 trailer 形式），因此该路径没有第三方客户端证据。
