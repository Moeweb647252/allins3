# 多 driver S3 服务器实现计划

状态：P0–P5 已实现（含 P5-05 的性质测试与模糊测试）；P6 由文档自身界定为不在本阶段实施范围，已按下文 P6 一节标记为 not-required。更新日期：2026-09-23。

> 进展：服务器可运行（`cargo run --features testing -- examples/dev-config.toml`）。三条真实客户端证据链：
> ① boto3/botocore **1.42.97** 端到端 **67/67** 项检查（`scripts/e2e_boto3.py`），覆盖 PUT/HEAD/GET/条件/Range/LIST 分页/COPY/批量删除/multipart（含 transfer manager 11 MiB → 3 part 与组合 ETag 独立复算）/UploadPartCopy/abort/预签名 URL 与签名拒绝，以及 multipart 的 FULL_OBJECT、COMPOSITE checksum（用 Python 标库独立复算）与错误 checksum 拒绝；
> ② AWS CLI **1.44.87**（`pip install awscli`）**29/29** 项检查（`scripts/e2e_cli.sh`），含 `aws s3api` 与 `aws s3 cp/sync/ls/rm/presign` 高层路径；
> ③ 手工构造的 `aws-chunked` 请求 **34/34** 项检查（`scripts/e2e_streaming_trailer.py`）：checksum trailer（含 trailer 值与字节不符、声明未送达、`UploadPart` 同规则），以及 **签名分块**（`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`，含单块签名被改、trailer 块签名被改均返回 `403 SignatureDoesNotMatch` 且不发布）。CLI 与 botocore 1.42 对 HTTP endpoint 一律把 checksum 放入请求头，不会发出这两种形式。
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
- [x] P0-03 用真实 SDK 请求验证四条风险路径。已完成：①签名不破坏原始 key（`a//b`、`+`、空格的原样往返）；②请求体无需整体缓冲（`src/service/mod.rs` 流式管线 + `a_generated_multi_megabyte_body_streams_through_unchanged` 的 12 MiB 生成流）；③提交前校验（`Content-MD5`/声明 checksum 不匹配即 abort，`a_body_shorter_or_longer_than_declared_is_a_bad_digest`）；④错误映射（`docs/compatibility-matrix.md` 第 4 节）。**未完成的部分**：官方固定签名向量未内置（原因与替代证据见兼容矩阵第 6 节），仅此一项。trailer 运行时校验与 AWS CLI 已不再属于未完成项：`ALLINS3_SECRET_KEY=devsecret .venv/bin/python scripts/e2e_streaming_trailer.py` 用裸 HTTP 构造 `aws-chunked` 请求并逐项校验 trailer（签名 trailer 块、声明缺失、值不匹配、`400 BadDigest` 后不留对象），34/34；`bash scripts/e2e_cli.sh` 用 aws-cli 1.44.87 对同一服务做 CLI 级往返，29/29。
- [x] P0-04 记录适配补齐/更换适配层影响 → ADR-0007。
- [x] P0-05 固定直接依赖版本与 CI 命令、字节上限 → ADR-0005。CI 工具链已固定为 `nightly-2026-09-15`（ADR-0005 决策 6）；许可证扫描（`cargo deny` 或等价）仍有未决项。
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
- [x] P1-06 定义 Committed、NotCommittedFinal、InFlight、Unknown 四态；验证终态未提交能阻止迟到发布，查无记录不等于终态失败，并覆盖 abort 与取消。→ `src/domain/error.rs`、`tests/contract_fault.rs`。`drop` 与临时资源回收已在 P2/P4 的真实暂存实现上补齐：`upload_part` 只在整段 body 收全后才写入暂存，取消、body 中断、超限、校验不符等每条退出路径都丢弃本地缓冲，因此不存在部分暂存（`src/service/multipart.rs`）；`StagedWrite` 约定“drop 不保证清理，调用方必须显式 `abort`，driver 必须登记临时资源”（`src/driver/traits.rs`），并由 `src/testing/contract.rs` 验证 abort → `NotCommittedFinal`、迟到提交被拒、abort 不留对象；part 与 `staged_bytes` 由 `abort_multipart`/`finish_upload`/`sweep_expired_uploads` 回收。回归覆盖：`tests/service_limits.rs::a_part_cancelled_mid_body_stages_nothing_and_frees_the_quota`（取消在途 part → 无 part、配额可被下一次写入使用、未发布）与 `tests/service_multipart.rs::an_aborted_upload_returns_its_staged_bytes_to_the_quota`（abort 后 `multipart_is_idle`，回收的字节可供下一次 upload 使用）。

产物：可编译接口、rustdoc、至少两种测试实现、统一契约测试工具。

验收门槛：基础 CRUD、稳定读、隐藏暂存、原子发布、排序列举、隔离全部通过；通过编译检查验证 `Arc<dyn Trait>`、`Box<dyn StagedWrite>` 和 `Send` future 约束；增加测试实现无需修改 service/protocol 代码。弱能力测试实现必须触发拒绝或明确的补齐路径。

对应规范：第 2、3、5 节；INV-01 至 INV-05、INV-07、INV-09。

### P2：配置、多实例路由和对象服务

任务：

- [x] P2-01 配置 schema、secret 引用、driver 实例初始化、只读 BucketCatalog、配置校验命令。→ `src/config/mod.rs`、`src/catalog.rs`、`allins3 check-config <path>`（退出码 0/2）。
- [x] P2-02 bucket 到 namespace 的不可变绑定；验证重复绑定、未知 kind、缺失配置与权限隔离。→ 配置校验 + `service_multipart`/`service_limits` 中的 namespace 隔离与实例隔离用例。
- [x] P2-03 实现 GET/HEAD/PUT/DELETE/LIST 领域用例，先经内部接口调用验证，不依赖 HTTP 细节。→ `src/service/mod.rs`。
- [x] P2-04 在 staged write 管线中增加长度、MD5/ETag、checksum、取消与背压。→ `tests/service_limits.rs`（超限 `400 EntityTooLarge`、长度不符 `400 BadDigest`、取消中途不发布、暂存配额 `503`）。**暂存配额在缓冲之前按 chunk 计入**（`MultipartState::reserved_bytes` 与 `StagingReservation`）：配额不足时请求在 body 仍在流式传输时即被拒，而不是先把整段（可达 `max_single_write_size`）收进内存再检查；每条退出路径（暂存成功、拒绝、取消、body 出错）都归还该预留，替换同号 part 时把被替换 part 的已暂存大小作为 credit，因此替换不会被自己的旧 part 挤掉。证据：`tests/service_limits.rs::the_staging_quota_refuses_a_part_while_its_body_is_streaming`（4 MiB 配额下 32 MiB body 只被读取 ≤ 5 MiB 即 `503 SlowDown`，被拒的 part 不入暂存且不占额，随后的 3 MiB part 正常入暂存）。
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
- [x] P3-05 实现普通 payload 的校验，且任何校验失败都不发布。→ `Content-MD5`、声明 checksum、长度不符都返回 `400 BadDigest` 且不产生对象；`aws-chunked` + checksum trailer 的运行时校验已补齐：`s3s` 只解码 trailer，值必须由服务层核对，因此 `PutParams.trailer_checksum` 在 body 消费完后读取 trailer 值并与流式摘要比对（声明未送达也视为不匹配）；`UploadPart` 走同一规则，且 part 声明的算法若与 upload scheme 不同，在读 body 之前即拒绝。→ `tests/service_limits.rs::a_trailer_declared_checksum_is_verified_after_the_body`、`tests/service_multipart.rs::a_part_trailer_checksum_is_verified_after_the_body`、`scripts/e2e_streaming_trailer.py`（34/34，含签名分块与 trailer 签名两类）。
- [x] P3-06 运行固定版本客户端基础用例；记录版本与默认配置。→ 两个固定版本客户端都跑通并记录了版本与默认配置：boto3/botocore **1.42.97**（`scripts/e2e_boto3.py` 67/67）与 AWS CLI **1.44.87**（`pip install awscli`，`scripts/e2e_cli.sh` 29/29，`s3.signature_version=s3v4`）。注：`aws s3 presign` 在 CLI v1 默认 SigV2，服务器按 ADR-0007 拒绝，脚本显式固定 s3v4；未编写 AWS SDK for Rust 客户端用例（不在本项交付物内）。
- [x] P3-07 virtual-hosted-style 未实现，因此配置成 `addressing = "virtual"` 在**校验阶段明确失败**（fail closed，不会静默按 path-style 提供服务），兼容矩阵标为未支持。→ `rejects_virtual_hosted_addressing`。

产物：可用 CLI/SDK 操作测试 namespace 的服务器、基础兼容报告、可执行开发配置。

验收门槛：默认 SDK checksum 设置下完成 PUT → HEAD → GET → LIST → COPY → DELETE；错误密钥、过期签名、越权 prefix、错误 checksum 均被拒绝；条件失败与 Range 越界组合返回正确状态，条件失败时不打开数据流。高层 CLI 可能自动使用 multipart，P3 用明确大小和低层操作验证基础范围，不提前声称所有 `aws s3` 命令可用。

对应规范：第 4–7、9 节；INV-02 至 INV-09、INV-12。

### P4：Multipart 扩展与恢复协议

任务：

- [x] P4-01（有意偏离并记录）：`MultipartNamespace` 已作为 driver 扩展契约存在（`src/driver/extensions.rs`），但 `MultipartStore` / `StagingStore` **没有**被引入为 trait。理由：暂存与 multipart session 的状态必须与「单次发布」「竞争仲裁」「完成记录」在同一个临界区内推进，把状态放到 driver 侧会把这些不变量拆到两个进程内；当前由服务层协调器（`src/service/multipart.rs`，`MultipartState` + `std::sync::Mutex`）独占，并通过 `ObjectService` 的公开方法暴露。真实后端的持久化暂存由 P6 的 driver 在其 `StagedWrite`/`MultipartNamespace` 内实现，协调器仍然只通过 driver 接口发布。
- [x] P4-02 实现创建、上传、列举 part/upload、完成、终止和 UploadPartCopy。→ 真实 SDK 覆盖全部六个操作。
- [x] P4-03 实现 CAS 状态机（`claims` 单次完成声明）、上传归属检查（bucket/key/upload id 绑定）、part 原子替换、固定清单与流式合并。→ `tests/service_multipart.rs`。
- [x] P4-04 part 顺序/大小限制与结果 metadata 已实现（升序、非末 part 至少 5 MiB、part 数上限 10000、完成时发布 `ETag = md5(parts)-N`）；**FULL_OBJECT / COMPOSITE checksum 已实现**：创建时固定 scheme（未指定则按算法族取默认覆盖），每个 part 按其算法计算并存储摘要，完成时 FULL_OBJECT 对装配后字节取摘要、COMPOSITE 对有序 part 原始摘要取摘要（带 `-<partcount>` 线格式），客户端声明的完成 checksum（含 part 清单里的每 part checksum）不匹配一律 `400 BadDigest` 且不发布，非法组合（`CRC64NVME + COMPOSITE`、SHA 族 + FULL_OBJECT）拒绝。→ `src/service/multipart.rs`、`src/protocol/s3/mod.rs`、`tests/service_multipart.rs`（6 个用例）、`scripts/e2e_boto3.py`（FULL_OBJECT 与 COMPOSITE 各用 Python 标库独立复算）。另：完成时只声明 `x-amz-checksum-type`（不带值）是合法请求，按 upload scheme 计算并返回；与 scheme 矛盾（或在无 scheme 的 upload 上单独声明）则 `400 InvalidRequest`，且被拒的完成不消耗 upload。
- [x] P4-05 实现完成记录、四态恢复查询和安全的租约清理器；只有 NotCommittedFinal 才可在 CAS 获得恢复权后评估重试，Committed 不再发布（回执丢失时从已发布 revision 还原响应），InFlight/Unknown 不得重放。→ `CompletionRecord`、`completion_records_for_upload` / `latest_completion_state`、`sweep_expired_uploads`、`tests/service_multipart.rs` 的 lost-ack / fenced / unknown 三个用例。**完成声明（claim）只在结果确实可能发布时才保留**：打开 namespace、取可写句柄、`begin_write` 这三步失败时都还没有任何可发布路径，失败即释放 claim（原先 claim 会留下，同一 upload id 的重试被永久拒绝为「already being completed」）；`write_chunk` 失败且 abort 已确认（记为 `NotCommittedFinal`、允许替代尝试）时同样释放，只有 abort 都无法确认的 `Unknown` 才保留 claim。证据：`tests/service_multipart.rs::a_completion_that_cannot_open_the_backend_is_retryable` 与 `::a_part_write_failure_that_aborts_finally_allows_the_retry`（一次性注入 `begin_write` / `write_chunk` 故障，首次完成失败后重试成功发布，part 与暂存全程保留）。**清理器由服务器调度**：`src/server.rs` 按 `limits.multipart_lease` / `multipart_sweep_interval`（配置项 `multipart_lease_secs` / `multipart_sweep_interval_secs`，后者为 0 时禁用）启动后台任务，关闭时先取消并 join 再排空（spec §10 的「所有者 / 取消路径 / shutdown 等待」）；租约按 session 的 `last_activity` 计算，仍在收 part 的上传不会被误清，有 completion claim 或 `Unknown` 记录的上传一律跳过（spec §8.2）。证据：`tests/service_multipart.rs::the_background_sweeper_reclaims_abandoned_staging_and_stops_on_request` 与 `::a_part_upload_resets_the_reclaim_lease`。
- [x] P4-06 覆盖并发 Complete/Complete 与 Complete/Abort 仲裁、丢失完成回执、重试清单变化（manifests 变更后重试）、过期暂存清理与「未知结果不被清扫/不被重放」；注入“原提交仍在途但查无记录”以及“原尝试已隔离、重试成功后旧提交迟到”两种时序，验证不重复提交且旧尝试不能覆盖新对象。
- [x] P4-07 SDK transfer manager 的 multipart 路径**已验证**（11 MiB → 3 part，ETag 用独立计算的组合值比对，`ListParts`/`UploadPartCopy`/`abort` 均通过）；高层 **CLI** 路径已验证（`scripts/e2e_cli.sh`：`aws s3 cp` 11 MiB 自动分片且 ETag 带 part 数、`aws s3 sync`/`ls`/`rm` 与往返字节一致）。

- [x] P4-08 把能力规划接到路由上，并实现原生 multipart 路径（[ADR-0010](../docs/adr/0010-capability-plan-drives-routing.md)）。`NamespaceHandles` 打开 namespace 时用 `features_for_profile(profile)` 调用 `plan()` 并把结果与句柄一起缓存；`copy_object` 由 `copy_route()` 选择原生复制或读流补齐，`handle.multipart` 不再零消费者。multipart 的归宿在创建时记录在 session 上（原生走驱动，其余走协调器），两条路径共用清单校验、scheme 定案、part 摘要校验、单次完成声明与四态对账；驱动契约按 spec §3.4 补上 `CreateUploadRequest.checksum` 与 `MultipartNamespace::complete(scheme, expected)`，使声明值在发布前被校验。参考 driver 实现 `MultipartNamespace`，契约 harness 增加 `multipart_native_semantics`（INV-05），服务层测试含原生/协调器等价用例。→ `src/service/handles.rs`、`src/service/multipart.rs`、`src/driver/extensions.rs`、`src/testing/reference.rs`、`src/testing/contract.rs`、`tests/service_multipart.rs`、`tests/contract_reference.rs`。

产物：可运行的 multipart 参考实现、恢复协议与故障测试。测试工具可以用可重建状态模拟崩溃窗口，但不能替代真实持久化系统的进程崩溃验证。

验收门槛：完成前目标不变；完成后对象和元数据一致；终止不影响原对象；重复完成不会覆盖后续对象；执行中或未知的尝试不被重放，已判为终态未提交的旧尝试无法迟到发布；后台清理不删除活动 part 或已提交对象；不同 checksum 模式得到正确值。

对应规范：第 6、8、9 节；INV-03、INV-04、INV-06、INV-08 至 INV-11。INV-10 的生产持久性验证仍留给 P6。

### P5：可运行性与兼容声明

任务：

- [x] P5-01 落实全局（`GlobalLimiter`，服务层单一所有者，值取 `max_concurrent_requests`）、每 principal（`PrincipalLimiter`）、每 driver（`DriverLimiter`，同一 driver 实例的并发操作数有上限，即便它承载多个 bucket）；三个范围饱和时都是 `503 SlowDown` 而非排队。哪些操作占这个 slot：收发 driver 请求或消费请求体的操作（`PutObject`/`GetObject`/`DeleteObject(s)`/`ListObjectsV2`/`CopyObject`/`UploadPart(Copy)`/`CompleteMultipartUpload`/提交查询）各占一个，同时计入全局与 driver 份额；只查目录或服务层状态的操作（`HeadBucket`/`ListBuckets`/创建、列举、终止 upload、列 parts）不占。`UploadPart` 曾漏于此列——既不限额也不被排空等待（`SIGINT` 后 0.0s 即退出并截断在途 part）——现已在 `ObjectService::upload_part` 入口取得同一个 slot，`upload_part_copy` 经同一公开入口，故不嵌套占两槽；实测饱和时 part 上传返回 `503 SlowDown`（另一 driver 实例不受影响），排空时服务器等待在途 part 完成（信号后 4.4s 退出、该 part 得 `200`）；由 `tests/service_limits.rs::a_part_upload_is_refused_when_the_global_scope_is_saturated` 与 `draining_waits_for_an_in_flight_part_upload` 回归覆盖。**多步操作只占一个 slot**：`CopyObject` 与 `UploadPartCopy` 在自己的入口准入一次，其内部步骤走不再准入的 `*_in` 辅助方法（`get_object_in`/`put_object_in`/`upload_part_in`），因此同一 scope 不会被同一操作占两次。修复前 `CopyObject` 走模拟路径时会二次占用目标 scope（`copy_object` 自身一槽 + 内部 `put_object` 再一槽），在 `max_concurrent_requests = 1` 或 `max_concurrent_requests_per_driver = 1` 下必然被判 `503 SlowDown`（即合法请求永远无法完成）；原生路径与单步操作本来就正确。由 `tests/service_limits.rs::an_emulated_copy_completes_at_a_limit_of_one` 回归覆盖。另有 body/XML（`max_xml_bytes`、`max_single_write_size`）、metadata（数量/字节/key 长度，超限 `400` 而非静默丢弃）、暂存配额。**超时（§9.3 的四个边界各自只有一个所有者）**：header timeout 由 HTTP/1 连接构建器的 `header_read_timeout` 执行（`src/server.rs`）——原先它包住的是**整条连接**的 future，等于用一个默认 30 s 的总超时掐断任何更长的传输，正式违反 §9.3 的「不能用一个很短的总超时阻断所有大对象」，现已在 `scripts/e2e_timeouts.py` 中证明 4 s 的传输在 2 s header 上限下仍得 `200`；body idle timeout 由包裹每个请求体的 `IdleTimeoutBody` 执行（`src/timeouts.rs`，值取 `body_idle_timeout_secs`）——之所以包住 body 而非连接，是因为大对象**下载**期间读方向会长时间无事件，只有包住 body 才不会误杀大对象；空闲错误由 `classify_request_body_error` 按来源链识别并归为客户端错误 `400 InvalidRequest`，不计为 500；总 deadline 由 `HttpService::call` 中围绕整次 S3 调用的 `tokio::time::timeout` 执行（`request_timeout_secs`，0 关闭，超时返回 `400 RequestTimeout`）；backend operation deadline 即总 deadline——driver 调用不得比发起它的请求活得更久，因此请求上下文携带同一上限（`S3Handler::scope` 的 `DriverContext::with_timeout`），不另设配置项。`scripts/e2e_timeouts.py` 以三种配置实测 **21/21**：半截 header 在 2.0 s 被断开且 2 s header 上限下 4 s 的传输仍得 `200`（即原先的整连接超时缺陷已消失）；2 s body idle 下停滞的 PUT 在 2.0 s 得 `400` 且未发布，停滞的控制体（`DeleteObjects` XML，由协议库自行读取）也由 `IdleTimeoutBody` 的观察标志把协议库的 500 改写为 `400`）；3 s 总 deadline 下持续发送的请求在 3.3 s 得 `400 RequestTimeout` 且未发布；三种配置下普通 PUT/GET/404 均不受影响。单元回归见 `src/timeouts.rs` 的 3 个用例与 `protocol::s3::tests::an_idle_request_body_is_a_client_error`。→ `src/timeouts.rs`、`src/server.rs`、`src/protocol/s3/mod.rs`、`src/service/mod.rs`；测试见 `tests/service_limits.rs` 的 `a_saturated_server_rejects_globally_across_drivers`、`a_saturated_driver_is_rejected_without_starving_another`、`draining_waits_for_the_last_operation`，以及 `src/protocol/s3/mod.rs` 的 `per_principal_admission_is_bounded_and_released`。§9.3 的「队列」一项按**上限（ceiling）**落实，不单独建队列：原文只要求「全局、每 principal、每 driver 的并发与队列均有上限」，而「后台任务必须有所有者、取消路径和 shutdown 等待；禁止无限队列或遗失 join handle 的任务」只禁止无界队列、并未要求队列必须存在；三个范围饱和即拒绝、队列上限为 0，因此无需额外的队列机制（not-required）。全局范围原先落在监听器信号量上，而它在 `accept` 之后才取槽，等待集（已接受但未准入的连接）本身无上限；现与 per-driver 用同一准入点取槽、饱和即拒绝，等待集恒为 0，`src/server.rs` 不再持有准入信号量，优雅退出改为等待 `ObjectService::drain()`（即在途操作清零）。§9.3 的 XML 一项：规范写「请求体、XML 深度/节点数、header、metadata、token、part 数和暂存空间分别限额；XML 禁用外部实体」，而同一份规范把 XML 归入 `protocol/s3/`（该层即 `s3s` 适配，模块文档写的就是 routing（via `s3s`）、XML），P0 评估亦记录「文档明确要求实现方自行做长度限制、限流与背压」。本系统里唯一的 XML 限制就是协议库的 `xml_max_body_size`（`s3s` 配置字段，由我方 `max_xml_bytes` 写入，即 §9.3 的「XML 上限」）；深度与节点数没有独立开关，而是由该字节上限派生并已被证明受限：请求体在解析前先按该上限收集，`s3s` 跳过未知子树的 `skip_element_content` 是迭代实现（depth 计数器循环、不递归），quick-xml 只解析五个预定义实体、不处理 DTD/外部实体。实测（`max_xml_bytes` = 10 MiB）：100 万层嵌套 7.0 MB → `400` 0.96s；200 万节点 8.0 MB → `400` 2.13s；用 schema 自身元素名的 50 万层 `<Part>` 嵌套 6.5 MB → `400` 0.06s；`<Object>` 30 万层 5.1 MB → `400` 0.04s；外部实体 → `400 MalformedXML`；超上限 44 MB 请求体在收集阶段即被切断。四种恶意体之后普通 PUT 仍 `200`，服务无 panic。因此不另设深度/节点数开关：那需要在协议库之前重新遍历同一份 XML（`s3s` 在调用我方 handler 之前就已消费 XML 请求体，本仓库自身不含任何 XML 解析器），属于重复的解析器护栏，且没有可测的风险可供修补（not-required）。
- [x] P5-02 添加 liveness/readiness（排空时 `readyz` 返回 `503`）、结构化日志（`level=info msg="..."`）、指标（`/metrics`：请求数、4xx/5xx、在途、运行时长）、优雅退出与实例故障隔离。→ `curl` 验证见兼容矩阵第 6 节；实例隔离由 `one_failing_driver_does_not_break_another_bucket` 覆盖；优雅退出在停止接收后等待 `ObjectService::drain()`（全局准入槽清零，至多 10s），由 `draining_waits_for_the_last_operation` 覆盖；`SIGINT`/`SIGTERM` 都会进入该路径（见 P5-03）。排空期间已建立连接上到达的新 S3 请求会被立即拒绝为 `503 SlowDown`（因为 `drain()` 占住全部全局槽，`try_acquire` 不再成功），停止 `accept` 后新连接不再被服务——实测两种信号下行为一致（矩阵第 4 节）。
- [x] P5-03 执行取消/流中断、超限、限流、暂存背压、未知提交结果测试。→ `tests/service_limits.rs`、`tests/service_multipart.rs`、`tests/contract_fault.rs`。真实慢客户端与真实进程信号下的排空时序由 `scripts/e2e_drain.sh` 覆盖（与 `scripts/e2e_cli.sh` 同风格的端到端脚本：启动服务器 → 用 botocore 签名的 PUT 由脚本自行按块慢速发送 body → 在途时发真实进程信号）。**`SIGINT` 与 `SIGTERM` 两种信号各跑两种时序**，因为原先的 `shutdown_signal()` 只 `ctrl_c()`（即 SIGINT）：在 `SIGTERM`（进程管理器/容器运行时实际发送的信号）下进程被内核默认处置直接终止，在途请求被丢弃且排空日志根本不出现，§7.2 的关闭顺序从未达成；现已同时注册 `SignalKind::interrupt` 与 `SignalKind::terminate`。两种信号的两种时序都已回归验证：① 约 4 s 的 body、信号在 0.5 s 时发出 → 服务器等到该在途上传结束才退出（实测等待 3–4 s）且该请求仍得 `200`，无 deadline 消息；② 约 19 s 的 body（超出 10 s 预算）→ 记 `drain deadline exceeded; exiting with in-flight requests` 并在 deadline 后才退出（实测 10 s），该请求未完成（客户端 `transport:BrokenPipeError`）；两种时序下退出码均为 0（非信号终止），日志顺序均为 `shutdown signal received; draining` → `shutdown complete`。共 20 项检查，连续 5 次运行均 20/20 通过。
- [x] P5-04 固定依赖版本（ADR-0005）并以固定 SDK 版本跑兼容矩阵：boto3/botocore **1.42.97**、Python 3.9.6、`signature_version=s3v4`、`addressing_style=path`、`retries.max_attempts=0`。协议库（`s3s` 0.16.1）或 SDK 升级后必须重跑本节命令。
- [x] P5-05 性质测试：range 解析、key 编码往返（含控制字符/`//`/`+`）、摘要编码往返、token 作用域与伪造（`tests/property_parsing.rs`，确定性 LCG，每次 500 轮）；协议面的性质测试 `tests/property_protocol.rs`（10 个用例、每次 500 轮）覆盖签名空间的单字节篡改与伪造、预签名 query、payload 摘要/长度不符、XML 与 part 清单变异，每个用例都断言「不得 500、不得发布」。**模糊测试**：三个 libFuzzer 目标（`fuzz/fuzz_targets/`）——`xml-manifest`（`CompleteMultipartUpload` 的 XML body 逐字节）、`part-manifest`（任意 part 编号/ETag/顺序的清单校验）、`signature-inputs`（Authorization 头与预签名 query 输入）；三者共用 `src/testing/protocol.rs` 的进程内 `ProtocolHarness`，它构建二进制同款 `S3Service`（同一 handler/凭据/授权/SigV4 开关）并用 `s3s-sigv4` 签名，因此驱动的是真实分发路径（路由、验签、XML 反序列化、错误编码）而非重写实现。不变式：不出现服务端错误、未签名的请求不得被接受、失败的请求不得发布对象。命令与结果：`scripts/fuzz.sh`（默认每个目标 20000 轮；`fuzz/` 是独立 workspace，根目录的 fmt/clippy/test 不会编译它），本次实测三个目标各 2000 轮无 crash，artifact 目录为空。为让「不得出现服务端错误」这条不变式成立并补齐 INV-08 的线上证据，`aws-chunked` 请求体错误被按类归位（`classify_request_body_error`）：分块/trailer 签名不符 → `403 SignatureDoesNotMatch`、payload 摘要与声明不符 → `400 BadDigest`、分帧不合法 → `400 InvalidArgument`，库内部细节只作为 cause 保留、不出现在响应里；错误映射见兼容矩阵第 4 节，线上证据见 `scripts/e2e_streaming_trailer.py`（34/34）。
- [x] P5-06 内存预算：`a_generated_multi_megabyte_body_streams_through_unchanged` 用生成式数据源验证服务层流水线；计数 sink（`src/testing/counting.rs`，逐块计数后丢弃，只保留 metadata）配合 `tests/resource_budget.rs` 以 100 MiB、1 GiB、10 GiB 生成流（256 KiB 块）实测：空闲基线 2 MiB，三个尺寸增量峰值 RSS 均为 **0 MiB**，阈值来自 `bench/resource-budget.toml`（`per_request_buffer_bytes × max_concurrency + slack = 32 MiB`）。命令：`ALLINS3_BENCH_FULL=1 cargo test --release --all-features --test resource_budget -- --nocapture`（默认只跑 100 MiB）。P5-06 其余部分（模糊测试）属 P5-05。
- [x] P5-07 发布工程预览使用说明与能力表（[README](../README.md)）、已知差异（[兼容矩阵](compatibility-matrix.md) 第 5 节）、开发模式标识（`development = true` 才允许注册测试 driver）、真实 driver 接入指南（[driver-guide.md](driver-guide.md)）。

验收门槛：内存符合缓冲预算；常规测试无 secret 泄漏；不支持的功能有确定错误；所有宣称支持的矩阵项通过；不以关闭 checksum、禁用签名或只用自写客户端作为验收条件。

对应规范：第 9–11 节，全部适用不变量。没有真实持久化实现时，发布物必须明确为工程预览。

### P6：真实后端生产准入（后续）

**标记：not-required（不在本实现范围内，不作为待办项）**。依据文档原文：

> 本阶段不预先选型，也不是当前文档任务要实施的内容。选择实际后端后，对每个 driver 分别执行：

下面列出的每一项都以「选定真实后端之后」为前提，且没有一项是当前代码库可以先行的代码或测试工作——其最后一项本身就是部署准入动作：

> 提交真实后端基准、部署/恢复手册和最终兼容报告后，才标记该配置可用于生产。

因此本阶段不为 P6 发明工作：下列勾选项保持未勾选，仅作为后续准入清单保留，当前发布物据此只能声明为工程预览。

后续任务（选定真实后端后逐个 driver 执行）：

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

1. `scripts/check.sh`（唯一入口：fmt、默认与 `--all-features` 两种 feature 的 clippy `-D warnings` 与测试，以及独立的协议 spike `cd spikes/protocol-adapter && cargo test`；CI 调用同一脚本）。当前 132 个单元测试 + 78 个集成测试。
2. `scripts/fuzz.sh`（P5-05 的三个模糊测试目标，默认每个 20000 轮；`fuzz/` 是独立 workspace，因此根目录的 fmt/clippy/test 不会编译它，必须由本脚本或显式命令执行）。
3. `cargo run --features testing -- check-config examples/dev-config.toml`（含 kind 注册预检），再启动并用 `scripts/e2e_boto3.py` 跑 67 项真实 SDK 检查。
4. `scripts/e2e_drain.sh`（P5-03 的排空时序：真实 `SIGINT`/`SIGTERM` × 真实慢速上传，20 项检查；与 `scripts/e2e_cli.sh` 一样自管服务器，因此这两个脚本需串行执行）。
5. `.venv/bin/python scripts/e2e_timeouts.py`（P5-01 的四个请求边界：header timeout、body idle、总 deadline 与不受影响的普通流量，21 项检查；同样自管服务器，需串行执行）。

**仍未完成（不得当作通过）**：P6 的生产准入（持久化暂存、崩溃恢复、备份、基准）全部未开始，必须在选定真实后端后单独排期，且文档自身已把它界定为「不是当前文档任务要实施的内容」（见 P6 一节的 not-required 标记与引文）；在此之前本仓库只能声明为工程预览。此外，`aws-chunked` trailer 的运行时校验只能用本仓库手工构造的请求验证（CLI/botocore 1.42 对 HTTP endpoint 不发出 trailer 形式），因此该路径没有第三方客户端证据。
