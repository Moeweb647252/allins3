# P0 协议适配评估：s3s

状态：已完成源码级与编译级评估；**未**完成真实 SDK/CLI 运行时验收。更新日期：2026-09-22。

对应任务：P0-02、P0-03、P0-04。决策记录见 [`adr/0007-protocol-adapter.md`](adr/0007-protocol-adapter.md)。

## 1. 评估对象

- crate：`s3s` 版本 **0.16.1**，许可证 **Apache-2.0**，`rust-version = 1.96.0`，仓库 `https://github.com/Nugine/s3s`（规范引用的 `s3s-project/s3s` 为其前身组织）。
- 评估方式：
  1. 读取 0.16.1 源码（`s3_auth`、`access`、`config`、`stream`、`protocol`、`ops/signature`、`service`、`s3_trait`）。
  2. 编译级验证程序：`spikes/protocol-adapter/`（独立 crate，不进入主 crate 依赖图），`cargo test` 通过。

> 该验证程序只证明“我们依赖的 API 面可以编译并接通”，**不**证明真实客户端请求可用。见第 4 节未决项。

## 2. 风险路径逐一核对

| 风险路径 | 结论 | 证据 |
| --- | --- | --- |
| 原始签名输入是否保留 | **部分是**。`HttpRequest`/`S3Request` 暴露 `method`、`uri`（hyper `Uri`）、`headers`、`credentials`、`trailing_headers`。s3s 自己解析并校验 SigV4/SigV2，凭据查询通过 `S3Auth::get_secret_key` 注入。无法替换签名算法本身。 | `spikes/protocol-adapter/src/lib.rs::raw_parts`；`src/ops/signature.rs` |
| 请求体是否整体缓冲 | **否**。`Body` 是流式；`put_object`/`upload_part` 通过 `DynByteStream` 暴露。文档明确要求实现方自行做长度限制、限流与背压。 | `s3s/src/lib.rs` 安全章节；`src/stream/upload_stream.rs` |
| trailer 能否在提交前校验 | **可以读到，但校验归属实现方**。`S3Request::trailing_headers: Option<TrailingHeaders>`，`TrailingHeaders::{is_ready,take,read}` 在 body 读完后提供**未校验**的 trailer 值；s3s 只在 `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` 下校验 trailer 签名，`x-amz-checksum-*` 的数值需实现方自行比对。 | `src/protocol.rs`；`src/stream/aws_chunked_stream.rs` 模块文档 |
| 取消语义 | **依赖 hyper/tower**，s3s 不做业务层取消协调。实现方需自建 deadline/取消与清理。 | `s3s/src/lib.rs` |
| 错误映射 | **可映射**。`s3_error!` 宏 + `S3ErrorCode` 生成 S3 XML 错误；我们的 `DriverError` 需在协议层映射到 `S3ErrorCode`。 | `src/error/mod.rs`、`generated.rs` |

## 3. 关键配置默认值（我方依赖）

| 配置 | 默认 | 与规范的匹配 |
| --- | --- | --- |
| `enable_sig_v2` | **false** | ✅ 首期只接受 SigV4，无需额外关闭 |
| `normalize_forward_slash_path` | **false** | ✅ 不会合并 `//`，符合 §4.2 |
| `presigned_url_max_expires_secs` | **604800** | ✅ 与 §6.1 的 1–604800 一致 |
| `allow_presigned_url` | true | ✅ 支持 presigned query |
| `operation_id_routing` | true | 需评估对错误路由的影响 |
| `normalize_content_length` | true | ⚠️ 会**回填** `Content-Length`，`S3Request.headers` 未必等于线上原始头；对签名域通常无影响，但需纳入测试 |

`sig_v4_allowed_services` 默认 `["s3","sts"]`，规范要求只接受 `s3` scope，需显式收窄。

## 4. 未决项（必须在 P3 前关闭）

1. **真实 SDK/CLI 运行时验收未执行**（P0-03）：本环境无 AWS CLI/Boto3/AWS SDK for Rust 客户端，也未搭建可运行服务。需要用官方签名向量与真实 SDK 覆盖普通 PUT、presigned URL、特殊 key、checksum trailer 四条路径。
2. **trailer 校验时序**：需在真实客户端下确认 trailer 可在 `commit` 前读取并比对。
3. **`normalize_content_length` 与原始头**：需确认回填不影响签名校验与日志中的“原始请求”假设。
4. **服务 scope 收窄**、`operation_id_routing` 的安全影响需专项测试。
5. **`ListObjectsV2` 响应整体序列化**：s3s 一次性序列化列举响应，内存随条目数增长，必须由我方分页约束（与 §9.3 一致）。

## 5. P0-04 结论：继续候选，但必须封装

`driver` API **不随 s3s 改变**。若后续运行时验收暴露缺口，退路是：

- 用一个薄适配层把 `S3Request`/`S3Error` 与我们自己的领域类型互转，协议层只依赖该适配层；
- 若缺口不可补齐（例如需要替换签名算法），则自建 HTTP/SigV4 层，`driver`/`service` 接口保持不变。

两种退路都不改动 `domain`/`driver`（`INV-01`）。
