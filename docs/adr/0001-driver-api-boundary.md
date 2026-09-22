# ADR-0001：对象 API 边界与模块依赖方向

状态：Accepted。日期：2026-09-22。对应 INV-01。

## 背景

项目要支持多 driver、多实例，并可能在将来接入具体生产后端。若协议层、对象服务层直接依赖某个后端 SDK 或某个协议库的类型，则新增 driver 或替换协议适配层会牵动核心代码。

## 决策

1. 依赖方向固定为：`protocol/s3` → `service` → `driver`，且 `domain` 是被各层共用的叶子模块。`driver` 与 `service` 不得依赖 `protocol`。
2. `driver` 接口（`StorageDriver`、`ReadNamespace`、`WriteNamespace`、`StagedWrite`、扩展接口）只使用 `domain` 类型与 `bytes`/`futures` 等通用类型，不出现 HTTP、XML、SigV4 或任何后端 SDK 类型。
3. 驱动错误统一为 `DriverError`（稳定 kind + 可选 `CommitState` + 安全 message + 内部 cause + 重试提示）；后端 SDK 错误保留为内部 cause，不穿透公共接口。
4. 公共异步接口使用 `#[async_trait]` + `Arc<dyn _>` 动态分发；staged write 使用 `Box<dyn StagedWrite>` 独占消费。
5. `protocol` 对协议库（当前候选 `s3s`）的使用必须封装在适配层内，`driver`/`service` 不感知。

## 后果

- 新增 driver 只需实现 trait 并通过契约测试，不修改 `service`/`protocol`（P1 已用两个独立测试实现验证）。
- 接口需要保持对象安全（object-safe），因此不能使用泛型方法或 `impl Trait` 返回。
- 动态分发带来少量开销，被 Streaming/IO 成本覆盖。

## 替代方案

- **单 trait 泛型静态分发**：性能略好，但无法在运行期按配置注册异构 driver 实例，被否决。
- **把协议库类型暴露给 driver**：会让协议库升级破坏 driver，违反 INV-01，被否决。
