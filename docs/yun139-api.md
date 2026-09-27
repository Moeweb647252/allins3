# 139 云盘 API（`yun139` driver 使用的部分）

本文记录只读 driver（`src/drivers/yun139/`，[ADR-0017](adr/0017-read-only-yun139-driver.md)）所依赖的私有接口。这些接口**不是**公开规范，来自对 `https://yun.139.com/` Web 客户端的抓包逆向，并在浏览器之外被逐条复现验证。升级客户端版本时这些契约可能变化，driver 的错误分类会把形状不符报成 `Internal`，而不是静默返回错数据。

## 基址与传输

- Web 前端分片：`https://yun.139.com/w/static/js/app.*.js`、`chunk-vendors.*.js`。
- 业务 API 基址：`https://personal-kd-njs.yun.139.com/hcy`。
- 响应统一为 JSON 信封：`{ "success": bool, "code": "0000", "message": "...", "data": … }`；`code == "0000"` 即成功。

## 认证

- `Authorization: Basic base64("pc:<phone>:<auth_token>")`，其中 `auth_token` 是会话 cookie 的值。仅该头即可通过鉴权；driver 额外接受可选的完整 `Cookie`。
- `mcloud-sign: "<timestamp>,<random>,<sign>"`：
  - `timestamp` = `YYYY-MM-DD HH:mm:ss`，服务本地时区（UTC+8）；
  - `random` = 16 位 `[A-Za-z0-9]`；
  - 令 `s = sort_chars(encodeURIComponent(JSON.stringify(body)))`（无 body 时 `s = ""`；字符级升序排序，因此与 JSON 键序无关），则
    `sign = md5(md5(base64(s)) + md5(timestamp + ":" + random)).toUpperCase()`。
  - `md5` 为 `js-md5`（小写 hex），`base64` 为 `js-base64`（标准字母表、带填充）。
- 必需的静态头（缺 `x-yun-app-channel` / `x-yun-client-info` 等会得到 `04000003 请求头不合法`）：`caller: web`、`x-SvcType`/`x-yun-svc-type: 1`、`x-yun-module-type: 100`、`x-yun-app-channel: 10000034`、`mcloud-route: 001`、`mcloud-version`、`mcloud-channel: 1000101`、`mcloud-client: 10701`、`x-m4c-caller: PC`、`x-m4c-src: 10002`、`x-inner-ntwk: 2`、`x-yun-channel-source`/`x-huawei-channelSrc: 10000034`、`CMS-DEVICE: default`、`INNER-HCY-ROUTER-HTTPS: 1`、`X-Deviceinfo`、`x-yun-client-info`。完整清单见 `client.rs` 的 `STATIC_HEADERS`。

## 端点

### `POST /hcy/file/list` — 列目录

请求：

```json
{ "pageInfo": { "pageSize": 100, "pageCursor": null },
  "orderBy": "updated_at", "orderDirection": "DESC",
  "parentFileId": "/", "imageThumbnailStyleList": ["Small", "Large"] }
```

响应 `data`：`{ "items": [FileItem…], "nextPageCursor": string|null }`。

`FileItem`（相关字段）：`fileId`、`parentFileId`、`name`、`type`（`"folder"`/`"file"`）、`fileExtension`、`category`、`createdAt`、`updatedAt`（ISO-8601，`+08:00`）、`size`（目录为 `null`）、`contentHash`、`contentHashAlgorithm`（文件为 `sha256`）、`revisionId`（形如 `1922…​.1922…` 的字符串）。列表**不按 key 排序**，driver 自行整树枚举后排序。

### `POST /hcy/file/getDownloadUrl` — 取预签名下载地址

请求 `{ "fileId": "<id>" }`。响应 `data.url` 是后端对象存储（中国移动 EOS，**S3 兼容**）上的 SigV4 预签名 URL（约 15 分钟有效，URL 内 `r=<revisionId>` 绑定修订）；`data` 另含 `size`、`revisionId`、`contentHash`。该 URL 支持 `Range`：`Range: bytes=0-15` 返回 `206` 与 `Content-Range: bytes 0-15/<size>`。

### 其他（本 driver 未使用，留给读写里程碑）

`/hcy/file/get`（单文件详情）、`/hcy/file/create` → `/hcy/file/getUploadUrl` → `/hcy/file/complete`（原生多段：`create` 返回 `partInfos[].uploadUrl`，逐段 `PUT`，再 `complete`；`rapidUpload`/`exist` 表示内容哈希去重命中）、`/hcy/file/batchMove|batchDelete|batchCopy`（异步批量任务，经 `/hcy/task/get` 轮询）、`/hcy/file/update`、`/hcy/file/search`、`/hcy/recyclebin/*`。

## 验证方式

- 算法向量（已固化进 `sign.rs` 单测）：
  - 无 body：`timestamp=2026-09-27 19:40:57`，`random=0oW3SlEX1rYl0ln8` → `F263E15C4344FDE450CD254FE98486E6`；
  - 有 body：`{"subDTO":{"controlSwitchType":100,"userDomainId":"1039903047986028223"}}`，`timestamp=2026-09-27 19:40:58`，`random=JB7Ouq5f3bmVfpXh` → `4204E163468D3A3E7F844AFC2837D564`。
- 从 Node（非 Chromium）以该算法重放 `file/list` 成功返回真实目录，且 `getDownloadUrl` + `Range` 实测 `206 …/120538481`，证明 driver 无需浏览器即可工作。
- 真实驱动：`cargo test --features testing,yun139 --test contract_yun139`（契约只读子集 + 真实 range 读取）与 `scripts/e2e_yun139.sh`（4/4）。

## 边界

- 凭据是**个人会话**，会过期/轮换；driver 不做登录流程。
- 无原子条件写、无同步原子发布、非排他写入者，因此只读是当前唯一能满足不变量要求的档位。
- 列表需整树枚举后排序，`max_objects` 提供上限。
- 服务条款风险：把个人云盘作为服务后端可能不符合其使用条款。
