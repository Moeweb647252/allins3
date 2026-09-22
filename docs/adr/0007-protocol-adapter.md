# ADR-0007：协议适配：封装 s3s 候选

状态：Accepted。日期：2026-09-22。对应 P0-02、P0-03、P0-04。

## 背景

需要 HTTP/S3 协议适配（路由、XML、头部、签名原始输入、S3 错误编码）。候选库 `s3s` 官方将其定位为实验性适配器，请求大小、限流与背压需应用实现。评估细节见 [../p0-protocol-adapter-assessment.md](../p0-protocol-adapter-assessment.md)。

## 决策

1. **继续以 `s3s` 0.16.1 为 P3 协议适配候选**，但必须封装在 `protocol/s3` 适配层内；`driver`/`service` 不得依赖 `s3s`。
2. **driver API 不随候选库改变**。若运行时验收暴露缺口，退路是薄适配层（`S3Request`/`S3Error` ↔ 领域类型），或自建 HTTP+SigV4 层；两条退路都不改动 `domain`/`driver`。
3. 依赖的默认值必须显式固定并在测试中断言：`enable_sig_v2 = false`（SigV4 only）、`normalize_forward_slash_path = false`（保留 `//`）、`presigned_url_max_expires_secs = 604800`。签名域需收窄为 `s3`。
4. **trailer 校验归属实现方**：`s3s` 只校验 trailer 签名，`x-amz-checksum-*` 数值必须由我们在 `commit` 前比对，否则 `STREAMING-UNSIGNED-PAYLOAD-TRAILER` 等于接受未认证数据。
5. 长度限制、限流、背压由应用实现；`s3s` 默认 5 GiB 的单次 PUT 上限只作下限，不能替代部署配额。
6. 列举响应由 `s3s` 整体序列化，内存随条目数增长，必须靠我方分页约束。

## 证据与未决项

- 编译级验证程序：`spikes/protocol-adapter/`（独立 crate，`cargo test` 通过）。它是**独立 workspace**，不会把 `s3s` 引入主 crate 依赖图。
- 未决（P3 前必须关闭）：真实 AWS CLI/Boto3/SDK 的运行时验收（P0-03 尚未执行）、trailer 提交前读取时序、`normalize_content_length` 与原始头假设、`operation_id_routing` 的安全影响。

## 后果

- P0-03 的运行时验收未完成，因此**不得**声称任何操作“已验证兼容”。
- 若未来替换协议库，只需替换适配层与兼容矩阵，不动 driver。
