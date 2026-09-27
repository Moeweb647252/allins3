# 0013 启动恢复与 readiness 的顺序

状态：Accepted（已实现）。日期：2026-09-26。对应 spec §7.2、§9.2；INV-09、INV-10。

## 背景

spec §7.2 的启动顺序是「解析配置 → 加载凭据与规则 → 构建 driver 实例 → 打开 namespace → 校验 binding/能力 → **恢复状态与清理任务** → readiness → 接收流量」。

当前实现只有前五步与 readiness：`compose::object_service` 构建服务并 `warmup`（打开 namespace 与能力校验），`server::run` 随即置 `ready = true` 并开始接受流量。第六步没有所有者——`CommitRecovery` 只在一次完成的判定里被查询，**没有任何路径在启动时核对遗留的 `InFlight`/`Unknown` write id**。

后果：`INV-09`（旧尝试不得迟到发布）与 `INV-10`（已确认数据符合持久性声明）在生产后端上无法成立——重启后，一个已发布但记录未知的 multipart 完成、或一个仍然在途的 staged write，既不会被对账也不会被清理。

## 决策

1. **恢复是组合阶段的一步，不是可选后续。** 新增 `recover(ctx)`，在 `warmup` 之后、`ready` 之前运行；`object_service` 返回即代表「恢复已完成」。

2. **readiness 在恢复完成前保持 `503`。** `server::run` 只在 `object_service`（含 `warmup` + `recover`）成功返回后置 `ready = true`；恢复期间 `/readyz` 返回 `503`，负载均衡器不会把流量送给一个尚未对账的副本。

3. **对账只读既有状态，绝不重放**（ADR-0003）：对每个未决 `write_id`，按 namespace 是否声明 `recovery` 能力查询 `CommitRecovery`：
   - `Committed` → 恢复完成记录，**不再发布**；
   - `NotCommittedFinal` → 标记可重试（由后续请求在 CAS 获得恢复权后决定）；
   - `InFlight` / `Unknown` → 保留待核对记录、报服务错误，**不重放、不承诺 exactly-once**。

4. **恢复完成后跑一次初始清扫**，租约与状态版本检查与运行期清扫器共用同一实现（ADR-0004 的清理规则不变：不得删除结果未知的已发布对象）。

## 后果

- `INV-09`/`INV-10` 有了启动侧的执行点；`INV-10` 的机器级验证仍属操作者手册（`docs/operations.md`）。
- 进程内测试可以用「丢弃 service、用同一持久 store 重建」模拟恢复；**这不替代真实进程崩溃验证**，必须在文档中标注该限制。
- 恢复失败必须使服务器启动失败（fail closed），而不是以未对账状态开始服务。

## 实现说明

1. **恢复是组合的一步**：`compose::object_service_with_state` 依次 `warmup` → `ObjectService::recover(ctx, lease)` → 返回；`server::run` 只在它成功返回后才置 `ready = true` 并开始监听，所以「/readyz 在恢复完成前为 503」是构造性成立的：恢复期间根本没有 listener。调用方拿到 `object_service_with_state` 的返回值，就是一个已恢复的 service。
2. **对账只读、不重放**：启动时先按持久记录重建 `uploads` / staging / `completions` / `claims`；对每个 claim 按 namespace 是否声明 `recovery` 查询 `CommitRecovery`：
   - `Committed` → 记录完成、回收该 upload，**不再发布**；
   - `NotCommittedFinal` → 记录并释放 claim，可由后续请求重试；
   - `InFlight` / `Unknown` → 保留 claim 与「未决」完成记录，**不重放、不承诺 exactly-once**。
   无 `recovery` 句柄与查询出错都归为 `Unknown`："查不清" 的安全读法是 "可能已发布"。
3. **孤儿 claim 与清扫**：claim 无对应 session 时删除；恢复完成后跑一次初始清扫，与运行期清扫器共用 `sweep_expired_uploads`（ADR-0004 的规则不变）。
4. **报告**：`recover` 返回 `RecoveryReport`（`sessions_restored`/`parts_restored`/`committed`/`retryable`/`pending`/`reclaimed_uploads`），并存进 `ObjectService::recovery_report()`；`server::run` 把它写进启动日志。`pending > 0` 表示有未决完成；它不再需要等到下次重启才可能解决——运行期的重试与清扫器会按后端提交记录在线对账（[ADR-0016](0016-online-pending-completion-resolution.md)，见 `docs/operations.md` 的未决完成处理）。
5. **失败即拒绝启动**：载入不可解析的记录、或恢复时 part 超出当前 staging 配额，都让 `recover` 返回错误，`server::run` 随之以非零退出，不以未对账状态服务。

进程内证据：`tests/service_multipart.rs::an_interrupted_completion_is_reconciled_but_never_replayed`（`LostAck` + `hide_recovery` → 重启后 `pending == 1`，且第二次完成被拒）；`tests/service_multipart.rs::an_acknowledged_part_survives_a_restart_and_can_be_completed`。真实进程证据：`scripts/e2e_fs.sh` 的 `SIGKILL` 步骤。

## 替代方案

- **把恢复交给 driver**：被否决。服务层独占 multipart 协调器状态（P4-01），恢复必须能看到该状态才能对账。
- **在后台异步恢复、先接受流量**：被否决。未对账的副本服务流量会违反 §9.2 的「不得重放」与「不承诺 exactly-once」。
