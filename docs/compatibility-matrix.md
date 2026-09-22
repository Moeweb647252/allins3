# 兼容矩阵

状态：**工程预览**。更新日期：2026-09-22。对应任务 P0-01 / P3-05 / P3-06 / P4-04 / P4-07 / P5-04 / P5-06。

图例：

- **已验证（SDK）**：由真实 AWS SDK（boto3/botocore 1.42.97）对着运行中的服务器跑通，命令见第 6 节。
- **已实现（测试）**：由 `cargo test --all-features` 中的单元/契约/服务测试覆盖，但未用真实 SDK 走协议层。
- **未支持**：明确拒绝（`501`/`400`），不会假装成功（`INV-12`）。
- **未验证**：代码路径存在但本环境没有可用客户端或时序，无法给出结论。

## 1. 操作矩阵

| 操作 | 关键参数/语义 | 官方引用 | 状态 |
| --- | --- | --- | --- |
| `ListBuckets` | 仅返回 principal 可见的配置 bucket | [ListBuckets](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html) | 已验证（SDK） |
| `HeadBucket` / `GetBucketLocation` | 逻辑 catalog 存在性、region | [HeadBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html) | 已验证（SDK） |
| `HeadObject` | 条件读、无响应体、Range 头规则、长度与 content-type | [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html) | 已验证（SDK） |
| `GetObject` | 条件先于 Range；单区间；`206`/`416`；部分响应不携带整体 checksum | [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) | 已验证（SDK），含 `bytes=a-b`、`bytes=-n` 与越界 `416` |
| `PutObject` | 流式校验、`Content-MD5`、checksum、原子发布、`If-None-Match: *` | [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) | 已验证（SDK），含条件写 `412` |
| `DeleteObject` / `DeleteObjects` | 不存在也成功；≤1000 key；逐项结果；quiet | [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html) | 已验证（SDK） |
| `ListObjectsV2` | prefix/delimiter/分页/`start-after`/`encoding-type=url`/token 作用域 | [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html) | 已验证（SDK），伪造 token 被拒 |
| `CopyObject` | 源 revision 固定、`COPY`/`REPLACE`、原生/补齐等价 | [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html) | 已验证（SDK）；原生与流式补齐等价由契约检查 `copy_native_matches_emulation` 保证 |
| 条件写/条件删 | 原子条件能力 | [条件写](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) | 已验证（SDK）`If-None-Match: *`；`If-Match: *` 明确 `501` |
| `CreateMultipartUpload` | 不修改现有目标对象 | [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html) | 已验证（SDK） |
| `UploadPart` | 同一 part number 原子替换、配额背压、part checksum 返回与校验 | [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html) | 已验证（SDK） |
| `UploadPartCopy` | 范围校验、服务端复制 | [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html) | 已验证（SDK）；范围复制为已实现（测试） |
| `ListParts` / `ListMultipartUploads` | S3 marker 语义（≠ V2 token） | [ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html) | 已验证（SDK） |
| `CompleteMultipartUpload` | 原子完成、单次发布、竞争仲裁、未知结果分类、checksum 合成与声明校验 | [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html) | 已验证（SDK），含 11 MiB/3 part 的 transfer manager 路径与 FULL_OBJECT/COMPOSITE checksum |
| `AbortMultipartUpload` | 与在途 part/Complete 仲裁、回收 | [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html) | 已验证（SDK）；与 Complete 竞争由服务测试覆盖 |
| 预签名 GET（SigV4 query） | 有效期、篡改 key 拒绝 | [presigned](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html) | 已验证（SDK），含过期与篡改拒绝 |
| `ListObjects` v1、浏览器 POST、CORS、`CreateBucket`/`DeleteBucket` | bucket 由配置定义 | [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html) | 未支持（`501`），首期不作兼容声明 |

## 2. Payload 与签名模式

| 模式 | 状态 | 说明 |
| --- | --- | --- |
| 普通 SHA-256 payload | 已验证（SDK） | `x-amz-content-sha256` 为内容哈希 |
| `UNSIGNED-PAYLOAD` | 已验证（SDK） | 仍校验签名 |
| SigV4 header（`Authorization`） | 已验证（SDK） | 错误 access key/密钥被拒 |
| SigV4 query（预签名） | 已验证（SDK） | 含过期（`X-Amz-Expires`，过期后 `403`）与篡改拒绝 |
| 错误 secret / 未知 access key | 已验证（SDK） | 已知 access key + 错误 secret → `403 SignatureDoesNotMatch`；未知 key → `403 InvalidAccessKeyId` |
| 签名被篡改（预签名 `X-Amz-Signature` 或 key 被改） | 已验证（SDK） | `400/403`，不会命中其他对象 |
| SigV2 / SigV4a | 未支持 | `enable_sig_v2 = false`；无 SigV4a（`s3s` 候选行为见 ADR-0007） |
| `aws-chunked`（signed/unsigned）与 checksum trailer | 已验证（手工请求） | `s3s` 只解码 trailer，值必须由服务层核对：声明值与流式摘要不符（或声明未送达）返回 `400 BadDigest` 且不发布。CLI 与 botocore 1.42 对 HTTP endpoint 一律把 checksum 放入请求头（debug 显示 `'in': 'header'`，非可寻址 body 直接报 `io.UnsupportedOperation: seek`），因此该路径只能用 `scripts/e2e_streaming_trailer.py` 手工构造的 `STREAMING-UNSIGNED-PAYLOAD-TRAILER` 请求验证（8/8） |

## 3. Checksum 支持范围

| 模式 | 状态 |
| --- | --- |
| `Content-MD5` 校验（单次 PUT） | 已验证（SDK）：不匹配返回 `400 BadDigest` 且不发布 |
| 客户端声明 checksum（`x-amz-checksum-*`）与算法回显 | 已验证（SDK）：CRC32/CRC32C/CRC64NVME/SHA1/SHA256 |
| 单对象 FULL_OBJECT | 已验证（SDK）：PUT、HEAD、GET 回显一致 |
| Multipart FULL_OBJECT（CRC32/CRC32C/CRC64NVME） | 已验证（SDK）：对装配后字节取摘要，与 Python 标库独立复算一致（`ck/full-object.bin`） |
| Multipart COMPOSITE（CRC32/CRC32C/SHA1/SHA256） | 已验证（SDK）：对有序 part 原始摘要取摘要，线格式 `base64(digest)-<partcount>`，与 Python 标库独立复算一致（`ck/composite.bin`） |
| 每个 part 的 checksum（`UploadPart` / `ListParts` / part 清单） | 已验证（SDK）：返回、存储、校验一致；不匹配 `400 BadDigest` 且该 part 不入暂存 |
| 完成时声明的整体 checksum 不匹配 | 已验证（SDK）：`400 BadDigest`，目标对象不出现；修正后重试可发布 |
| checksum trailer（`x-amz-trailer`） | 已验证（手工请求）：值与流式摘要不符返回 `400 BadDigest` 且不发布 |
| 非法组合（如 `CRC64NVME + COMPOSITE`、SHA 族 + FULL_OBJECT） | 已验证（SDK）：`400 InvalidArgument`，创建 multipart 时即拒绝 |

依据：[完整性保护](https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html)。

## 4. 错误映射（已实现）

| 内部情况 | S3 结果 |
| --- | --- |
| bucket 不存在 | `404 NoSuchBucket` |
| key 不存在 | `404 NoSuchKey` |
| 权限拒绝/只读 bucket | `403 AccessDenied` |
| 条件不成立 | `412 PreconditionFailed` |
| 条件写竞争 | `409 ConditionalRequestConflict` |
| 范围越界 | `416 InvalidRange` |
| 已检查 revision 不可读（重读预算耗尽） | `503 ServiceUnavailable` |
| 校验不符 / 长度不符 | `400 BadDigest` |
| 超过配置或声明的对象大小 | `413 EntityTooLarge` |
| 暂存配额/限流/暂时不可用 | `503 SlowDown` |
| 每 principal 并发超限 | `503 SlowDown` |
| 能力未支持 | `501 NotImplemented` |
| 参数/XML 不合法 | `400 InvalidArgument` / `InvalidRequest` / `MalformedXML` |
| 未知提交结果/内部错误 | `500 InternalError` |

## 5. 已知兼容差异（必须记录）

- **virtual-hosted-style 未实现**：配置成 `addressing = "virtual"` 会在校验阶段失败（fail closed），不会静默按 path-style 提供。兼容矩阵因此标为未支持。
- **列举中的 XML 1.0 不可表示字符**：未请求 `encoding-type=url` 时返回 `InvalidRequest`，不输出破损 XML、不改写 key（key 字节始终原样保留，含 `a//b`、`+`、空格）。
- **首期不支持**：Versioning、Object Lock、ACL、完整 IAM、STS、SSE（`ServerSideEncryption` 等参数返回 `501`）、生命周期、通知、复制、网站托管、Select、Directory Buckets、SigV2、SigV4a。
- **multipart checksum**：已实现两种模式（见第 3 节）。`ETag` 仍按规范 §5.4 发布 `md5(parts)-N`（与 checksum 是两个独立字段），不随 checksum 模式变化。
- **checksum trailer 无第三方客户端证据**：CLI/botocore 1.42 对 HTTP endpoint 使用请求头形式的 checksum，不发出 `aws-chunked` trailer，因此该路径只用本仓库手工构造的请求验证（第 2 节）；`mc`/`s3cmd`/`rclone` 未安装、未验证。
- **持久性**：`test-reference` / `test-vec` 为内存测试 driver，重启即丢；生产准入（P6）需要真实 driver，见 [driver 接入指南](driver-guide.md)。
- 不支持的功能必须明确失败，不得忽略有业务含义的参数后返回成功（`INV-12`）。

## 6. 验证记录

```bash
# 单元 + 契约 + 服务测试
cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features

# 协议适配 spike（独立 workspace）
cd spikes/protocol-adapter && cargo test

# 真实 SDK 端到端（失败会在第一条断言处退出）
cargo build --features testing
ALLINS3_DEV_SECRET=devsecret ./target/debug/allins3 examples/dev-config.toml &   # 监听 127.0.0.1:9000
ALLINS3_SECRET_KEY=devsecret .venv/bin/python scripts/e2e_boto3.py             # 63/63 checks passed
ALLINS3_SECRET_KEY=devsecret .venv/bin/python scripts/e2e_streaming_trailer.py # 8/8 checks passed
bash scripts/e2e_cli.sh                                                        # 29/29 checks passed（自行启停服务器）
ALLINS3_BENCH_FULL=1 cargo test --release --all-features --test resource_budget -- --nocapture
curl -sS http://127.0.0.1:9000/healthz   # 200 ok
curl -sS http://127.0.0.1:9000/readyz    # 200 ready
curl -sS http://127.0.0.1:9000/metrics   # 计数器
./target/debug/allins3 check-config examples/dev-config.toml   # 退出码 0
```

客户端版本（P5-04/P3-06）：boto3/botocore **1.42.97**（Python 3.9.6，`signature_version=s3v4`，`addressing_style=path`，`retries.max_attempts=0`）与 AWS CLI **1.44.87**（`aws-cli/1.44.87 Python/3.9.6 botocore/1.42.97`，`s3.signature_version=s3v4`，`AWS_MAX_ATTEMPTS=1`）。

资源预算（P5-06）：`bench/resource-budget.toml` 给出 256 KiB 单请求缓冲、并发 1 与 32 MiB slack（推导式 `per_request_buffer_bytes × max_concurrency + slack`）。实测空闲基线峰值 RSS 2 MiB；100 MiB / 1 GiB / 10 GiB 生成流（256 KiB 块、400 / 4496 / 45456 块）经计数 sink 写入后增量峰值 RSS 均为 **0 MiB**。

关于“官方签名向量”：本仓库没有内置 AWS 文档中的固定签名十六进制向量作为断言，而是用两条独立证据替代：①真实 SDK（botocore，独立实现）对运行中服务器完成 header/query 两种 SigV4 签名；②签名被重新计算/篡改/过期后的请求必须被拒。若需要固定向量回归，应在 `spikes/protocol-adapter` 中接入 AWS `aws-sig-v4-test-suite` 的用例（该测试套件的 service 为 `service`，需要显式放开服务白名单）。

**CLI 结论（P3-06 / P4-07 高层路径）**：`scripts/e2e_cli.sh` 用 AWS CLI 1.44.87 跑通 29/29 项，覆盖 `s3api put/head/get/list-objects-v2/copy-object/delete-objects`、条件写 `412`、Range、`create/upload-part/list-parts/complete/abort-multipart-upload`（含 SHA256 COMPOSITE 独立复算）、`aws s3 cp`（11 MiB 自动分片）、`sync`、`ls`、`rm`、`presign`。唯一需要显式配置的是 `s3.signature_version=s3v4`：CLI v1 的 `aws s3 presign` 默认发出 SigV2（`AWSAccessKeyId`/`Signature`/`Expires`），服务器按 ADR-0007 拒绝并返回 `403 AccessDenied: Signature Version 2 is disabled by server configuration`；固定 s3v4 后预签名 URL 正常。`aws s3api put-object --body /dev/stdin`（不可寻址 body）被 CLI 拒绝（`Blob values must be a path to a file`），`mc`/`s3cmd`/`rclone` 未安装，因此这些客户端未验证。
