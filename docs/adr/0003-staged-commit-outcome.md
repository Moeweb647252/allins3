# ADR-0003：分阶段提交与四态结果模型

状态：Accepted。日期：2026-09-22。对应 INV-03、INV-09。

## 背景

`commit` 一旦开始，断连、超时或取消都不能证明“没有发布”。用一个 `retryable: bool` 无法表达“可能已提交”，贸然重试会覆盖别的请求刚提交的对象或产生重复发布。

## 决策

1. 写路径为显式状态机：`Created → Receiving → Validated → Committing → Committed`，失败进入 `Aborting → Aborted`，提交结果不确定进入 `OutcomeUnknown`。`commit` 前失败必须保证：新 key 不存在、旧 key 保持原样。
2. 提交确定性统一用 `CommitState` 四态表达：
   - `Committed`：该 write_id 已发布；后续覆盖/删除不改变这个历史结果，可按记录恢复响应，**不**再次发布。
   - `NotCommittedFinal`：该尝试从未发布，且已终止或已在**实际发布点**被隔离，今后不可能发布；不可逆。
   - `InFlight`：原尝试仍在执行或仍可能发布；等待或查询，不创建替代提交。
   - `Unknown`：无法证明成功或终态未提交（含查无记录、过期、查询失败）；保留待核对记录，不创建替代提交。
3. **“查无记录”不等于“终态未提交”**；`CommitRecovery::query` 只观察既有状态，missing → `Unknown`。
4. 返回 `NotCommittedFinal` 前，driver 必须保证原尝试及其所有在途调用/内部重试都失去发布能力；仅取消本地 future、超时、租约过期或未看到目标对象都不足以证明。driver 做不到就必须返回 `InFlight/Unknown`。
5. 服务层不得复用 write_id 重新发布；恢复 coordinator 用 CAS 保证同一逻辑操作只有一个新尝试，且新提交时重新判断原子写条件。
6. 临时资源必须有显式 `abort`、登记/租约与可恢复清理；drop 不保证异步清理，也不得删除结果未知的已发布对象。

## 后果

- 每个提交尝试使用新的 `WriteId`；作用域含 driver 实例与 namespace。
- 测试必须覆盖两种时序：原提交仍在途但查不到记录时不得第二次提交；终态隔离后即使迟到执行到达发布点也不得覆盖新尝试。
- `DriverError` 携带 `Option<CommitState>` 与 `RetryHint`。

## 替代方案

- **`retryable: bool`**：无法区分“可能已提交”，被否决。
- **超时即视为回滚**：会制造半对象或重复发布，被否决。
