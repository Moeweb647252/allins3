# 兼容矩阵（首版）

状态：**目标矩阵，尚未实现**。更新日期：2026-09-22。对应任务 P0-01。

本矩阵记录每个操作的目标、所需能力、关键参数、payload/checksum 模式、错误响应依据与官方引用。
“状态”一列当前全部为“未实现”，任何一项只有在对应测试通过后才可以改成“已验证”。不得把档位宣传为完整 AWS S3。

图例：`P3/P4` = 目标阶段；`能力` = 所需 driver 能力或档位。

## 1. 操作矩阵

| 操作 | 阶段 | 能力 | 关键参数/语义 | 官方引用 | 状态 |
| --- | --- | --- | --- | --- | --- |
| `ListBuckets` | P3 | 目录/权限 | 仅返回 principal 可见的配置 bucket | [ListBuckets](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html) | 未实现 |
| `HeadBucket` / `GetBucketLocation` | P3 | 目录/权限 | 逻辑 catalog 存在性、region | [HeadBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html) | 未实现 |
| `HeadObject` | P3 | `core-read` | 条件读、无响应体、Range 头部规则 | [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html) | 未实现 |
| `GetObject` | P3 | `core-read` + Range（能力允许） | 条件先于 Range；单区间；`encoding` 语义 | [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) | 未实现 |
| `PutObject` | P3 | `core-write` | 流式校验、`Content-MD5`、checksum/trailer、原子发布 | [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) | 未实现 |
| `DeleteObject` | P3 | `core-write` | 不存在也成功；可选 `If-Match` | [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html) | 未实现 |
| `DeleteObjects` | P3 | `core-write` | ≤1000 key、逐项结果、quiet、请求体校验字段 | [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html) | 未实现 |
| `ListObjectsV2` | P3 | `core-read` | prefix/delimiter/pagination/start-after/`encoding-type=url` | [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html) | 未实现 |
| `CopyObject` | P3 | 读+写（原生或补齐） | 源 revision 固定、`COPY`/`REPLACE`、双向鉴权 | [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html) | 未实现 |
| 条件写/条件删 | P3（可选扩展） | 原子条件能力 | 仅在有原子实现时声明 | [条件写](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) | 未实现 |
| `CreateMultipartUpload` | P4 | `multipart` | 不修改现有目标对象 | [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html) | 未实现 |
| `UploadPart` | P4 | `multipart` | 同一 part number 原子替换 | [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html) | 未实现 |
| `UploadPartCopy` | P4 | `multipart` + 稳定读 | 范围校验，可流式回退 | [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html) | 未实现 |
| `ListParts` / `ListMultipartUploads` | P4 | `multipart` | S3 marker 语义（≠ V2 token） | [ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html) | 未实现 |
| `CompleteMultipartUpload` | P4 | `multipart` | 原子完成、竞争仲裁、未知结果分类 | [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html) | 未实现 |
| `AbortMultipartUpload` | P4 | `multipart` | 与在途 part/Complete 仲裁、回收 | [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html) | 未实现 |
| `ListObjects` v1、浏览器 POST、CORS 配置 | 后续 | — | 不作首期兼容声明 | [ListObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html) | 未实现 |
| `CreateBucket` / `DeleteBucket` | 后续 | — | 需先定义 namespace provisioning | [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html) | 未实现 |

## 2. Payload 与签名模式（P3 必测）

| 模式 | 目标 | 说明 |
| --- | --- | --- |
| 普通 SHA-256 payload | 必须 | `x-amz-content-sha256` 为内容哈希 |
| `UNSIGNED-PAYLOAD` | 必须 | 仍需校验请求签名；仅可信 TLS |
| SigV4 chunked payload | 必须 | 逐 chunk 签名校验通过后才可提交 |
| unsigned/signed chunked + checksum trailer | 必须 | trailer 数值在提交前比对 |

依据：[SigV4 payload 模式](https://docs.aws.amazon.com/AmazonS3/latest/userguide/sigv4-auth-using-authorization-header.html)。

## 3. Checksum 支持范围

| 模式 | 首期算法 |
| --- | --- |
| `Content-MD5` | 单次 PUT、UploadPart、控制请求 |
| 单对象 FULL_OBJECT | CRC32、CRC32C、CRC64NVME、SHA1、SHA256 |
| Multipart FULL_OBJECT | CRC32、CRC32C、CRC64NVME |
| Multipart COMPOSITE | CRC32、CRC32C、SHA1、SHA256 |

非法组合（如 `CRC64NVME + COMPOSITE`）明确拒绝。依据：[完整性保护](https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html)。

## 4. 错误映射（目标）

| 内部情况 | 默认 S3 结果 |
| --- | --- |
| bucket 不存在 | `404 NoSuchBucket` |
| key 不存在 | `404 NoSuchKey` |
| 权限拒绝/只读 | `403 AccessDenied` |
| 条件不成立 | `412 PreconditionFailed` |
| 条件写竞争 | `409 ConditionalRequestConflict` |
| 范围越界 | `416 InvalidRange` |
| 已检查 revision 不可读（重读预算耗尽） | `503 ServiceUnavailable` |
| 校验不符 | `400 BadDigest` |
| 能力未支持 | `501 NotImplemented` |
| 参数/XML 不合法 | `400 InvalidArgument` / `InvalidRequest` / `MalformedXML` |
| 限流/暂时不可用 | `503 SlowDown` / `ServiceUnavailable` |
| 未知提交结果/内部错误 | `500 InternalError` |

## 5. 已知兼容差异（必须记录）

- 列举结果含 XML 1.0 不可表示字符且未请求 `encoding-type=url`：返回 `InvalidRequest`，不输出破损 XML、不改写原 key。
- 首期仅 path-style；virtual-hosted-style 为显式配置项。
- 首期不支持 Versioning、Object Lock、ACL、完整 IAM、STS、SSE、生命周期、通知、复制、网站托管、Select、Directory Buckets、SigV2、SigV4a。
- 不支持的功能必须明确失败，不得忽略有业务含义的参数后返回成功（`INV-12`）。
