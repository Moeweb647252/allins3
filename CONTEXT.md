# CONTEXT — 术语与模块边界

本文是**领域概念**与**模块接缝**命名的单一出处。架构评审、重构与新增代码都以此为准；已由 ADR 定义的概念只在此指路，不复述。
（本文件由架构评审创建：`improve-codebase-architecture` 的候选 2「拆分对象服务」。）

## 领域术语

| 术语 | 所指 | 出处 |
| --- | --- | --- |
| bucket（逻辑 bucket） | 配置定义的逻辑名，与后端 namespace 名无关 | ADR-0004 |
| binding | `BucketId -> (DriverInstanceId, NamespaceId, BindingGeneration)` 的固定绑定 | ADR-0004 |
| binding generation | 绑定的版本号；请求、分页令牌、upload 都必须固定它 | ADR-0004 §5 |
| namespace handle | 已绑定隔离空间的只读/可写句柄；方法不再接收可替换的后端路径 | ADR-0004 §7 |
| object / revision | 对象与其内部版本；ETag、checksum、revision 是三个不同字段 | ADR-0002 · spec §5.4 |
| staged write | 先写后原子发布的写入原语 | ADR-0003 |
| commit state | `Committed` / `NotCommittedFinal` / `InFlight` / `Unknown` | ADR-0003 · spec §9.2 |
| multipart upload / part | 分片上传及其分片；本项目的协调器在服务层 | spec §8 |
| continuation token | 列举的分页句柄，绑定 bucket、generation、prefix、delimiter、encoding 与鉴权作用域 | spec §4.3 · ADR-0004 |

## 模块与接缝

以下名字是本仓库认可的说法；接口（interface）指调用方必须知道的最小面，实现（implementation）指其后的一切。拆分理由与“缝隙保持内部”的决定见 [ADR-0008](docs/adr/0008-object-service-decomposition.md)。

### Admission（准入）

- **持有**：服务全局槽位、每 driver 实例槽位、饱和策略、排空。
- **接口**：取一个操作的槽位；等待排空。饱和即拒绝，不排队。
- **不负责**：权限判断、catalog 查询、任何后端调用。
- **理由**：并发预算是独立关注点，出错时应当能只看这一个模块。

### NamespaceHandles（句柄）

- **持有**：driver 实例表、namespace 缓存（键为 `bucket#generation`）—— 每项是驱动句柄加一份由它声明物化出的 `CapabilityPlan`。
- **接口**：`get(binding, ctx) -> Arc<Namespace>`（`Namespace` 对 `NamespaceHandle` 实现 `Deref`）；路由只读规划（`support` / `copy_route` / `native_multipart` / `writer`）。
- **不负责**：权限（ADR-0004 §8）、准入、catalog 的查询本身。
- **必须保持**：ADR-0004 §5 的绑定版本固定；能力校验失败映射为 `Internal`；`plan()` 失败即启动失败；句柄的接口与声明不一致时打开即失败（`is_self_consistent`），因此路由读到的 `Native` 规划背后一定有接口（ADR-0010）。

### TokenStore（列举令牌）

- **持有**：令牌记录、TTL、过期与清理。
- **接口**：签发一个令牌（记录 + TTL 在内部决定过期时刻）、解析并校验一个令牌。
- **不负责**：分页游标的产生、重新鉴权（由用例层在每页执行）。
- **理由**：过期语义只应有一个家，调用方不再碰时间类型。

### Multipart（multipart 状态所有者）

- **持有**：`Mutex<MultipartState>` —— session、完成声明（claim）、完成记录，以及一个 `StagingLedger`。
- **接口**：由用例层通过内部接口访问；对外仍是 S3 multipart 操作。session 上记录了这个 upload 的归宿：原生 namespace 的 part 与完成在驱动里，其余由协调器持有（ADR-0010）。
- **不负责**：准入（由 Admission 提供槽位）、句柄缓存；原生 upload 的字节与生命周期（归驱动——服务侧只保留绑定记录，并在驱动确认该 upload 已消失后由清理器退休它）。

### StagingLedger（暂存账本）

- **持有**：暂存配额、已暂存字节、在途（正在接收）字节与 part 表 —— 四者同属一个所有者。
- **接口**：`reserve` / `release`（接收期计费）、`stage`（写入 part，含配额与 part 数上限）、`take`（整体回收并返回释放字节）、`part` / `parts_of`（只读）、`is_idle`。
- **保证**：已暂存 + 在途永不超配额；丢弃 part 与归还字节在同一步完成（此前三处调用方各自手动相减，容易与表的改动脱节）。
- **不负责**：session、claim、完成记录（留在 `MultipartState`）、准入与句柄。

### ObjectService（用例层）

- **持有**：catalog 与权限策略，以及上述四个模块。
- **接口**：S3 用例（put/get/head/list/copy/delete、multipart 全流程）以及给组合根用的薄委托。
- **可见性规则**：跨 crate 的集成测试用到的入口保持 `pub`；只被同 crate 的适配层或测试装置调用的入口是 `pub(crate)`。spec §10 要求公共接口变更同步更新规范与契约测试。

### Composition（组合根）

- **持有**：启动接线 —— 注册 driver kind、按配置创建实例、建服务并 warmup、把 S3 适配层包在服务外。
- **接口**：`src/compose.rs` 内的一组函数；对 crate 内其余部分可见（`pub(crate)`），对外仍是 `server::run` 与测试装置。
- **不负责**：监听、进程生命周期、后台任务（清理器的所有者仍是 `server`）。
- **理由**：`server::run`、`fixture::service`、`ProtocolHarness::new` 曾各自维护一份略有差异的启动顺序，适配层标志可能与服务实际配置漂移。

### 已实现的候选

架构评审提出的三个候选现均已实现：**对象服务拆分**（Admission / NamespaceHandles / TokenStore / Multipart，见 ADR-0008）；ADR-0008 记为“未决”的两项后续 —— **Composition 组合根**（`src/compose.rs`）与 **StagingLedger**（`src/service/staging.rs`）—— 见 [ADR-0009](docs/adr/0009-service-decomposition-followups.md)。能力规划从「只做档位校验」变为**路由的唯一依据**（原生 copy、原生 multipart 或补齐），见 [ADR-0010](docs/adr/0010-capability-plan-drives-routing.md)。

## 进程级全局状态

规则：新增进程级可变状态（`static`、`OnceLock`、`thread_local`、模块级缓存）不进入本仓库。每个状态归它服务的那个对象所有，随对象创建与销毁，这样两个实例、两个测试不会互相影响。

有意保留的进程级边界（每处只是一次读取或无状态生成器）：

| 位置 | 内容 | 为何保留 |
| --- | --- | --- |
| `main::main` | `std::env::args` / `ALLINS3_CONFIG` | 进程入口；参数与环境只在此读一次 |
| `config::EnvSecretResolver` | `std::env::var` 读密钥 | 生产环境的密钥源；测试用 `AppConfig::from_toml(text, &dyn SecretResolver)` 或 `compose::instances_from_config(..., secrets)` 注入自己的解析器 |
| `domain::WriteId::generate` | 进程内唯一的 `WriteId` 序号 | `WriteId` 的契约就是「进程内唯一、不可重用」，它不是可配置依赖；测试断言唯一性而非具体值 |

已注入化（原先各自挂着 `static COUNTER`）：`TokenStore` 的令牌序号与 `S3Handler` 的 `x-amz-request-id` 序号 —— 两者都是对象生命周期内的状态，现由对象自身持有。

## 边界规则

- 依赖方向 `protocol/s3 → service → driver`，`domain` 是共用叶子（ADR-0001）。
- `driver` / `service` 不得依赖 `protocol`；协议库的使用封装在适配层（ADR-0001 §5）。
- 后台任务必须有所有者、取消路径与 shutdown 等待（spec §10）。
