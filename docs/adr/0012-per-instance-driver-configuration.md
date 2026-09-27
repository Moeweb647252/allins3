# 0012 每实例 driver 配置与受限阻塞执行器的所有权

状态：Accepted。日期：2026-09-26。对应 spec §3.2.5、§7.1、§10；INV-01。

## 背景

driver 接口自 P1 起要求「工厂负责验证 driver 私有配置；核心层只理解公共字段、kind、实例 ID 和能力」（spec §3.2.5），但配置解析只走到 `RawDriver.options` 就没有下文：

- `RawDriver.options` 被 TOML 解析后**没有任何读者**；
- `RawDriver` 没有 spec §7.1 要求的每实例 `secret_refs` 字段；
- `FactoryConfig` 只是一个持有 `instance_id` 的桩，唯一引用它的是一处 `#[allow(dead_code)]` 测试函数；
- `DriverFactory::create(ctx, instance_id, secrets)` 因此无从知道连接串、桶名、根路径，也不知道该解析哪个 secret 引用。

结论：**任何真实后端都无法被配置**。这是「加 driver 之前」最硬的缺口，阻塞了其后所有工作（生产 driver、启动恢复、预检、运维可见性）。

同时 spec §10 要求「阻塞 I/O 放入受限的阻塞执行器」，而全仓 `spawn_blocking` / `block_in_place` 计数为 0：一个包装同步 SDK 的后端要么阻塞 runtime worker 线程，要么自造线程池，且契约没说清是哪种。

## 决策

1. **配置是每实例、由工厂解释的。** `RawDriver` 增加公共字段 `secret_refs: Vec<String>`；`options: toml::Table` 保持不透明。核心只做两件事：证明每个 `secret_refs` 引用可解析（启动前失败，值从不进入日志），以及把 `options` 原样传给工厂。私有 schema 的解析与拒绝是工厂的事——未知 `options` 键由 driver 拒绝，因此部署文件里的拼写错误会在 `check-config` 失败，而不是被静默忽略。

2. **`FactoryConfig` 成为真正的每实例配置**，随 `validate` 与 `create` 一起传入：

   ```rust
   pub struct FactoryConfig {
       pub instance_id: DriverInstanceId,
       pub options: toml::Table,      // 不透明；核心不解释
       pub secret_refs: Vec<String>,  // 已证明可解析
       pub blocking: BlockingExecutor,
   }
   ```

   `DriverRegistry::create_instance` 把**同一个** `FactoryConfig` 先交给 `validate` 再交给 `create`，所以工厂不可能「用一份配置校验、用另一份创建」。

3. **受限阻塞执行器由组合根单独持有**（`BlockingExecutor`，`Semaphore` 限额 + `spawn_blocking`），限额取 `limits.max_blocking_operations`（默认 64；`0` 表示不设服务侧上限，把上限留给 driver，需在 driver-guide 说明）。它随 `FactoryConfig` 交给每个实例，因此两个 driver 不会各自发明一份线程预算。

4. **`check-config` 只调用 `validate`，绝不 `create`。** 新增 `DriverRegistry::validate_instance`；`compose::preflight` 用它。这既让私有 schema 错误在启动前失败，又保证「校验一个文件」没有任何磁盘副作用（曾一度因 `preflight` 调 `create` 而在无写权限的路径上失败并创建目录，已修正）。

## 后果

- 真实后端的配置路径打通：`options` + `secret_refs` 到达工厂，`check-config` 能在不打开监听、不创建目录的前提下拒绝坏配置。
- 默认构建出现一条始终注册的非测试 kind（`fs`，见 ADR-0015），因此 `builtins()` 的表在无 feature 时不再为空。
- `DriverFactory` 的签名改变（`validate`/`create` 接收 `&FactoryConfig`），波及所有实现与测试装置；这是一次性成本，换取核心不必理解任何后端概念。
- 阻塞边界有了单一所有者；同步后端必须使用它，异步后端（如 `fs` 使用 `tokio::fs`，其本身即受限阻塞池）按其声明使用。

## 替代方案

- **核心定义一套通用 typed schema**（连接串/根路径/桶名作为公共字段）：被否决。核心层会因此开始理解后端概念，违背 driver 独立性与 §3.2.5。
- **工厂声明 schema、核心驱动校验**：接口更重，收益（更强的启动前失败）不足以证明其复杂度；私有 schema 由工厂自己校验已满足 spec。留作后续可选。
- **每个 driver 自持阻塞执行器**：被否决。并发预算会成为 N 份互不知情的线程池，无法作为部署策略统一限额。
