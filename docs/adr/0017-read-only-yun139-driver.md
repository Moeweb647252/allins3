# 0017 只读的 139 云盘 driver

状态：Accepted。日期：2026-09-27。对应 spec §2、§3、§5、§10；INV-01；ADR-0002、ADR-0010、ADR-0012、ADR-0015。

## 背景

P6 的「选定真实后端」在 ADR-0015 里先由本地文件系统（`fs`）落地。第二个真实后端要证明 driver 接缝对**远端、网络、凭据、多写入者**同样成立。目标后端是中国移动云盘（139 个人云盘，`yun.139.com`）：它对每个文件暴露 `size`、sha256 `contentHash`、`revisionId`、`updatedAt`，并且 `file/getDownloadUrl` 返回的是后端 S3 兼容存储（EOS）上的 SigV4 预签名 URL。接口契约与签名算法见 `docs/yun139-api.md`。

关键约束来自该后端自身：**没有隐藏暂存、没有同步原子发布、没有原子的条件写**，且该 namespace **不是排他写入者**（用户本人与其它客户端都在写同一目录）。按 `is_production_write_capable()` 的定义，写模式无法通过生产写准入。

## 决策

1. **本次只实现只读 driver**（`src/drivers/yun139/`，kind `"yun139"`），声明 `read`/`list` 为 `true`，`write`/`multipart`/`native_copy`/`recovery` 全为 `false`（无对应句柄，`is_self_consistent()` 成立）。`range = Native`（预签名 URL 支持 `Range`），`persistence = Durable`（对象在远端存储），`consistency = Eventual` 与 `exclusive_writer = false` 如实声明——因此 `is_production_write_capable()` 为假，写模式被准入拒绝，这不是靠约定而是靠声明。

2. **命名空间即目录，key 即相对路径。** `options.folder_id`（或 `namespaces` 映射）指向一个云盘目录；其下每个文件成为一个对象，key 是相对该目录的路径，嵌套目录折叠为 key 前缀。读取的是**云盘里真实的文件**，不做任何编码封装，因此只读不需要自描述头或第二份元数据：`size`/`contentHash`/`revisionId`/`updatedAt` 直接来自服务端。

3. **revision 用云盘的 `revisionId`。** `stat` 返回它；`read` 先校验 `expected_revision`，再取预签名 URL——该 URL 的 `r=<revisionId>` 参数把下载绑定到具体修订，若不一致返回 `RevisionUnavailable`，绝不退回读当前版本（红线 2）。

4. **私有配置在 `options`，会话凭据由操作者写在部署文件里**（本项目对 `fs` 用的是 `env:` secret_ref，这里按操作者选择用配置值）。凭据以 `Secret` 持有（`Debug` 打码），`validate` 只解析、不联网、无副作用，因此 `check-config` 可在只读环境拒绝坏配置。部署文件必须排除在版本控制之外（`.gitignore` 忽略 `examples/yun139-local.toml`）。

5. **出站 HTTPS 是本仓库的第一个网络客户端**：新增固定版本的 `reqwest`（`rustls` + 平台根证书、`json`、`stream`）与 `serde_json`，都置于 `yun139` feature 之下，默认构建零新增依赖。已相应修改 ADR-0005。

6. **签名与时间戳零新依赖**：签名用已有的 `md-5` + `base64`；`YYYY-MM-DD HH:mm:ss`（UTC+8）格式化与 ISO-8601 解析用整数历法自实现（`stamp.rs`），不引入日期库新 feature。

7. **列表整树枚举后排序。** 云盘列表不按 key 有序，driver 枚举子树、在受限阻塞执行器上排序/折叠，并复用与服务一致的索引游标分页；`max_objects` 提供枚举上限，超出返回 `ResourceExhausted` 而非无限扫描。

## 后果

- `examples/yun139-config.toml`（`development = false`、`profile = "core-read"`、`readonly = true`）可通过 `check-config`；真实进程下由 `scripts/e2e_yun139.sh` 证明：就绪（即鉴权通过）、有序列表、range 读取字节数与 `HeadObject` 一致、以及 PUT 被拒（只读在 API 边缘成立）——4/4。
- 契约 harness 对只读 namespace 全绿并**列出被跳过的写侧检查**（`tests/contract_yun139.rs`，需真实会话，缺则跳过并打印原因）：`capability_consistency`/`profile_validation`/`list_absent_key` 通过，`crud`/条件写/多段/复制/隔离按能力跳过。
- 签名算法由两个抓包向量固化为单测（`sign.rs`），因此算法回归会被 CI 捕获，而无需网络。
- 只读 driver 在无 `state_dir` 的生产部署中可用（无可写 bucket），符合 ADR-0014 的规则。
- 真实读取的代价：一次 `stat`/`read` 需要按路径逐级列目录；`list` 需要枚举整个子树。对小目录（个人云盘常态）可接受，`max_objects` 是安全阀。

## 未决（留作后续里程碑）

- **写模式**：`file/create` → `file/getUploadUrl` → `file/complete` 的原生多段上传、`batchMove`（重命名）、`batchDelete`、`batchCopy` 尚未接入。它们的原子性弱于红线 1/3 的要求，需要单独立项并可能引出新的执行语义，届时再评估能否声明 `write`。
- **列表性能**：整树枚举在超大目录上昂贵；可考虑 `file/search`、按名字排序参数或目录级缓存。
- **凭据生命周期**：不做登录/续期，token 过期由操作者更新部署文件。
- **服务条款**：把个人云盘用作服务后端可能不符合其使用条款，使用前需自行确认。

## 替代方案

- **直接做读写 driver**：会让只读声明变成「暂时如此」，并迫使本次就处理无原子发布/非排他写入的语义缺口；被否决，先以只读证明接缝在远端后端上成立，写模式另立 ADR。
- **包装同步 `curl` 子进程而非新增异步客户端**：能零新依赖，但流式下载、连接复用与错误分类都要借道临时文件与进程，健壮性差；被否决。
- **把元数据编码进对象字节（如 `fs` 的自描述头）**：只读无必要，且会让云盘里的文件不再是用户可见的真实文件；被否决。
