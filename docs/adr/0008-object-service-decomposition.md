# ADR-0008：对象服务的四个协作者拆分与缝隙保持内部

状态：Accepted。日期：2026-09-25。来源：架构深化评审（`improve-codebase-architecture`）候选 2「拆分对象服务」；与 P4-01 / P4-05 的协调器边界直接相关。

## 背景

`ObjectService` 是唯一实现 S3 用例的类型，因此长期是全部状态的所有者：driver 表与 namespace 句柄缓存、列举令牌、并发准入（全局与每 principal、每 driver）、multipart 暂存状态都挂在它的 `ServiceInner` 上。结果是这个“上帝对象”承担了至少五个互不相关的关注点：

- 并发预算出错、令牌过期出错、句柄缓存出错、multipart 配额出错，都必须先读懂同一个类型；
- 每个关注点的测试都要先拼出整个服务（driver 注册、catalog、权限、配置），没有 driver 就无法在单元层验证准入或令牌语义；
- 公共面随内部需要一起膨胀：适配层、测试装置与用例之间没有可见性区分，任何一处接线都可能被跨 crate 调用而冻结成契约。

ADR-0004 §5/§7 已把绑定与句柄的语义定死，但“谁持有句柄缓存、谁签发令牌”在类型层面没有归属，于是同一职责在两个地方（`ObjectService` 与 `ServiceInner` 的方法）各有入口。

## 决策

1. **把 `ServiceInner` 的四个关注点各抽成一个协作者**，`ObjectService` 退化为用例层与组合点：
   - `Admission`：全局槽位与每 driver 实例槽位，饱和即拒绝（`503 SlowDown`），不排队；持有排空逻辑。
   - `NamespaceHandles`：driver 实例表与 namespace 句柄缓存，接口只有 `get(binding, ctx) -> Arc<NamespaceHandle>`，负责打开与能力校验。
   - `TokenStore`：列举令牌记录、TTL 与清理，`issue` 在内部决定过期时刻，调用方不再接触时间类型。
   - `Multipart`：multipart 状态所有者（session、part、完成声明与记录、暂存配额与已用字节）。
2. **接缝保持内部**：四个协作者一律 `pub(crate)`，**不**新增对外公开的 trait 缝隙。目前只有一个适配器（`protocol/s3`），公开缝约会是为不存在的第二个适配器而设的投机抽象。等第二个消费者真实出现再提升可见性。
3. **薄委托保留原调用形状**：`binding_slot` / `operation_slot` / `drain` 继续作为 `ObjectService` 上的薄委托，调用方（服务器、测试装置）不需要改动。
4. **可见性规则**：跨 crate 集成测试用到的入口保持 `pub`；只被同 crate 适配层或测试装置调用的入口改为 `pub(crate)`。公共接口的变更必须同步更新规范与契约测试（spec §10）。
5. **multipart 状态归属唯一**：状态由 `Multipart` 持有，`ObjectService::multipart()` 只交出 `MutexGuard<MultipartState>`，不再由 `ServiceInner` 直接持有 `Mutex<MultipartState>`。
6. **必须保持的不变量**（不因拆分改变）：
   - 句柄缓存键仍为 `bucket#generation`，仍固定 ADR-0004 §5 的绑定版本；
   - 能力校验失败仍映射为 `ServiceError::Internal`；
   - 排空仍由 `ObjectService` 拥有并等待后台任务退出；
   - 依赖方向仍为 `protocol/s3 → service → driver`，`domain` 是共用叶子（ADR-0001）。
7. **准入只在模块内测试**：`Admission` 用 `#[cfg(test)]` 单元测试覆盖（全局预算、每 driver 预算、`0` 关闭、排空等待），不引入测试替身，因为缝隙是内部的，没有第二个实现需要契约。
8. **命名以 `CONTEXT.md` 为准**：`Admission`、`NamespaceHandles`、`TokenStore`、`Multipart` 的名称、职责与“不负责”清单记录在 `CONTEXT.md`。`StagingLedger`（把配额、已暂存/在途字节与 part 表收进一个接口）列为下一个候选，本 ADR **不**实现它。

## 后果

- `ServiceInner` 从九个字段降到七个：`handles`、`catalog`、`limits`、`tokens`、`admission`、`access`、`multipart`。`service/mod.rs` 显著变短，各关注点各有自己的文件与测试。
- 服务核心可在不启动服务器的情况下测试：当前 121 个单元测试 + 64 个集成测试全绿，重构后 `e2e_boto3` / `e2e_streaming_trailer` / `e2e_cli` / `e2e_drain` / `e2e_timeouts` 全部通过。
- 收窄可见性时暴露出三处死代码（`catalog()`、`query_commit()`、`completion_state()` 无调用方）并删除；这是把“只有适配层在用”的入口降为 `pub(crate)` 的直接收益。
- 代价：`ObjectService` 上的薄委托是新增的一层间接，读代码时需要在两个类型之间跳一次；这是有意的，换来的是每个关注点可独立推理与测试。
- 后续：`Composition`（组合根）已作为 `src/compose.rs` 实现，收拢了 `server::run` / `fixture::service` / `ProtocolHarness::new` 各自重推的启动接线（`ADR-0008` 的替代方案里曾把它列为正交候选）。仍未实现的是 `StagingLedger`。

## 替代方案

- **为四个协作者各定义一个公开 trait，做端口/适配器**：目前只有一个适配器与一个组合根，公开的接口缝隙没有第二个实现来证明其形状，只会在错误的边界上冻结契约。被否决；改为内部 `pub(crate)` + 模块内测试。
- **同时把用例层拆成独立 handler 模块**：用例之间共享权限与 catalog，拆开会引入跨模块的借用/生命周期耦合，收益低于噪声。本次不动用例层。
- **抽一个 `Composition` 组合根，收拢 `server::run` / `fixture::service` / `ProtocolHarness::new` 的重复接线**：是真实候选，但与本 ADR 的状态拆分正交，单独评估。
- **让 `ServiceInner` 直接暴露四个字段、不设委托**：会迫使每个调用点自行编排槽位与句柄的获取顺序，把不变量的维护分散到调用方。被否决。
