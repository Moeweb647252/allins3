# ADR-0010：能力规划驱动路由，并接入原生 multipart

状态：Accepted。日期：2026-09-25。来源：`/audit` 的四维审查发现「`plan()` / `CapabilityPlan` 在生产路径零调用；`NamespaceHandle.multipart` 零消费者；copy 路由是 ad-hoc 的 `if let Some(native)`」，以及随后对这轮接线的复审发现的边界问题（列出的原生 upload 不可驱动、原生 session 不被回收、marker 语义两路不一致、`/readyz` 无上界、契约未覆盖 `expected`）。承接 [ADR-0008](0008-object-service-decomposition.md) 的缝隙规则，并**收窄** [implementation-plan](../implementation-plan.md) P4-01 记录的有意偏离（`MultipartNamespace` 此前只有契约、没有消费者）。

## 背景

能力模型分两层，此前只有一层被消费：

- `support_for` / `validate_profile` 已被 `NamespaceHandles::get` 调用，是活的；
- 聚合入口 `plan()` / `CapabilityPlan` / `CapabilityError` 只被 `testing::contract` 和测试调用，生产路径零调用。

因此「可选功能必须通过能力规划选择原生、补齐或拒绝」（spec §3.3）只在档位校验一处成立，真正的路由决策是各调用点自己看句柄：copy 用 `if let Some(native) = &handle.native_copy`，multipart 完全走服务层协调器，`handle.multipart` 没有任何消费者。同时 P4-01 记录的偏离是：`MultipartNamespace` 是已声明的扩展契约，但没有 driver 实现它，也没有路由把它接上。

## 决策

1. **规划在打开 namespace 时物化一次**：`NamespaceHandles::get` 用 `features_for_profile(profile)` 调用 `plan()`，把结果与驱动句柄一起缓存在 `Namespace`（对 `NamespaceHandle` 实现 `Deref`，因此只关心接口的调用点无需改动）。`plan()` 失败即启动失败，与档位校验同样 fail closed。
2. **路由只读规划，不再重推**：`Namespace::support()`、`Namespace::copy_route()`、`Namespace::native_multipart()`、`Namespace::writer()` 是唯一的 Native/Emulated 判定点。`copy_object` 由 `copy_route()` 选择原生复制或读流补齐，写入由 `writer()` 给出（`CoreRead` 档位的规划里没有 `Write`，即使驱动暴露了 writer 也不可写）；任何调用点都不再直接检查 `native_copy` / `multipart` / `write`。
3. **multipart 路由在创建时记录在 session 上**：`create_multipart` 按规划决定 upload 归宿 —— 原生 namespace 在驱动里开 upload（客户端看到的就是驱动的 upload id），否则由协调器开。之后的每个操作按 session 上的记录分派，因此一次 upload 不可能被后到的调用换一条路径。两条路径共用本模块的协议语义：清单校验、scheme 定案（`resolve_completion_scheme`）、part 摘要校验（`finish_part_checksum`）、单次完成声明、完成记录与四态对账。清单校验只有一份实现（`validate_manifest`，两条路径各自提供一个 part 视图：协调器的暂存账本、驱动的 part 页），因此不存在「一条路径接受、另一条拒绝」的清单。
4. **原生路由按 spec §3.4 补全驱动契约**：`CreateUploadRequest` 增加 `checksum`（spec 第 249 行要求 create 接收 checksum 方案），`MultipartNamespace::complete` 改为接收 `scheme` 与 `expected` —— 驱动用前者计算并发布对象校验和，用后者在**发布前**校验客户端声明的值，因此被拒的完成不消耗 upload。
5. **两条路径都要可验证**：参考 driver 实现 `MultipartNamespace`（`testing/reference.rs`），契约 harness 增加 `multipart_native_semantics`（`INV-05`：声明与实际行为一致），服务层测试在同一套断言下比较原生路径与协调器路径（`tests/service_multipart.rs`：`the_native_route_and_the_coordinator_agree`）。
6. **有意收窄，均 fail closed**：驱动契约的 `list_uploads` 不带 prefix，由服务层过滤；原生完成的每 part 声明校验和按驱动上报的摘要核对，驱动未上报则拒绝而不是放行；清单里 part 声明的校验和算法必须与该 upload 的 scheme 一致——这条规则现在由共用的清单校验执行，所以协调器也跟着收紧了（它此前会按客户端声明的算法重算并放行，而 part 摘要只按 scheme 计算，那种接受本身不是 upload 的含义）。
7. **清理器按驱动确认回收原生 session**：原生 upload 的字节归驱动，其 session 生命周期也归驱动，因此服务侧记录只在**驱动不再知道这个 upload 时**才退休——租约到期后先向驱动确认（对该 upload `list_parts` 返回 `NotFound` 即已消失），驱动仍持有则保留记录。删掉驱动仍持有的记录会让客户端拿到的 upload id 立刻不可用（比抛弃一个已死的记录更糟），而永不回收则泄漏 session 且 `multipart_is_idle()` 永远不为真。任何非 `NotFound` 的结果（含瞬时错误）都按「仍在」处理，把记录留给下一轮。
8. **列出的原生 upload 必须可驱动**：原生 `ListMultipartUploads` 仍以驱动为准（它才知道哪些 upload 存在），但服务对自己没有记录的原生 upload 会就地**收养**成 session（`checksum` / content / metadata 留空，由驱动持有），而不是返回一个所有入口都会拒绝的临时 session。配合决策 7，收养出的记录在驱动忘记该 upload 后由清理器回收，因此「列出即可驱动」与「不会泄漏」同时成立。
9. **marker 语义归服务层所有**：`(key-marker, upload-id-marker)` 是一个对——upload-id marker 只有与 key marker 同时存在才有意义，空值表示「从头开始」。服务层在一个地方解析这条规则（`upload_marker`），两条路径共用，因此空/半 marker 的选择不会因路径而异（此前原生路径会因空 key marker 报错，而只带一个 marker 时又整个交给驱动）。
10. **打开 namespace 时校验接口与声明一致**：`NamespaceHandles::get` 调用 `handle.is_self_consistent()`（此前只在测试 harness 里断言）。于是决策 2 里「`Native` 规划必然有接口」在生产路径上是事实，而不是只有测试保证的假设；不一致的驱动在打开时即失败，而不是被 `copy_route()` 静默降级成补齐。
11. **就绪探测有界、不回显驱动原因**：`/readyz` 的驱动健康探测加上 `HEALTH_PROBE_TIMEOUT`（5s，与 `SHUTDOWN_DEADLINE` 同理），超时即 503，而不是让一个不答复的驱动挂住负载均衡的探针；驱动上报的 reason 只写日志，不再拼进未鉴权的响应体（它可能带主机名或连接串）。
12. **契约 harness 覆盖 `complete` 的 `expected` MUST**：`multipart_native_semantics` 现在先用一个不匹配的声明校验和完成一次，要求驱动在**发布前**拒绝且不消耗 upload（parts 仍可列出、目标仍不存在），再用正确的声明值完成。因此驱动契约里「发布前校验、被拒不消耗」这条 MUST 有测试盯着，而不是只有文档。

## 后果

- 「能力声明 → 规划 → 路由」现在是同一条链：新增可选能力时先声明，再由 `features_for_profile` 与路由决策点消费，ad-hoc 的 `if let Some(handle)` 不再产生。
- 协调器仍然存在，仍是「没有原生 multipart 的 namespace」的实现；原生 multipart 现在是可选路径而不是未接线的契约，P4-01 的偏离在范围上被收窄（协调器保留，契约被消费）。
- 证据：`cargo fmt --check`、`clippy -D warnings` 干净；132 个单元测试（含 `handles` 的 4 个生产准入用例）+ 76 个集成测试全绿，其中 `multipart_native_semantics`（含新增的 `expected` 拒绝/不消耗/接受三段）、32 个 multipart 服务用例（含 marker 边界、收养、清理器按驱动确认回收、part 算法一致性与 native/协调器等价用例）覆盖新路径；`scripts/e2e_boto3.py` 67/67、`scripts/e2e_drain.sh` 20/20（协调器路径未回归），服务器 `/readyz` 正常、SIGTERM 干净退出、日志 0 条 `level=error`。
- 新增的 `/readyz` 超时用测试驱动的不确定性换确定性：超时即 503，与「驱动不健康」同码，探针的语义仍然是「能否服务」而不是「为什么不能」。

## 替代方案

- **删除未被消费的聚合抽象**：改动最小且不新增分支，但会放弃 spec §3.3 要求的「规划结果」这一层，并把 copy/multipart 的取舍永久留在各调用点。与用户选择的「完整接线」相反，被否决。
- **只接线 copy，保留原生 multipart 未实现**：`handle.multipart` 将继续没有消费者，而档位校验已经允许 `Multipart = Native`，会留下「声明可用但不会被使用」的隐性不一致。被否决。
- **不改驱动契约，改为把 `CompletedPart` 扩展成携带每 part 校验和**：能让驱动在装配时校验每 part，但对象级校验和仍无法在发布前校验（服务层没有字节），原生完成的原子性要求无法满足。被否决。
