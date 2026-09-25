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

以下名字是本仓库认可的说法；接口（interface）指调用方必须知道的最小面，实现（implementation）指其后的一切。

### Admission（准入）

- **持有**：服务全局槽位、每 driver 实例槽位、饱和策略、排空。
- **接口**：取一个操作的槽位；等待排空。饱和即拒绝，不排队。
- **不负责**：权限判断、catalog 查询、任何后端调用。
- **理由**：并发预算是独立关注点，出错时应当能只看这一个模块。

### NamespaceHandles（句柄）

- **持有**：driver 实例表、namespace 句柄缓存（键为 `bucket#generation`）、打开与能力校验。
- **接口**：`get(binding, ctx) -> Arc<NamespaceHandle>`。
- **不负责**：权限（ADR-0004 §8）、准入、catalog 的查询本身。
- **必须保持**：ADR-0004 §5 的绑定版本固定；能力校验失败映射为 `Internal`。

### TokenStore（列举令牌）

- **持有**：令牌记录、TTL、过期与清理。
- **接口**：签发一个令牌（记录 + TTL 在内部决定过期时刻）、解析并校验一个令牌。
- **不负责**：分页游标的产生、重新鉴权（由用例层在每页执行）。
- **理由**：过期语义只应有一个家，调用方不再碰时间类型。

### Multipart（multipart 状态所有者）

- **持有**：`Mutex<MultipartState>` —— session、part、完成声明（claim）、完成记录、暂存配额与已用字节。
- **接口**：由用例层通过内部接口访问；对外仍是 S3 multipart 操作。
- **不负责**：准入（由 Admission 提供槽位）、句柄缓存。
- **待定**：`StagingLedger`（把配额、已暂存/在途字节与 part 表收进一个接口）是下一个候选，尚未实现。

### ObjectService（用例层）

- **持有**：catalog 与权限策略，以及上述四个模块。
- **接口**：S3 用例（put/get/head/list/copy/delete、multipart 全流程）以及给组合根用的薄委托。
- **可见性规则**：跨 crate 的集成测试用到的入口保持 `pub`；只被同 crate 的适配层或测试装置调用的入口是 `pub(crate)`。spec §10 要求公共接口变更同步更新规范与契约测试。

### 未实现的候选（记录以免重复提议）

- **Composition（组合根）**：把 `server::run`、`fixture::service`、`ProtocolHarness::new` 各自重推的接线收成一个模块。
- **StagingLedger**：见上。

## 边界规则

- 依赖方向 `protocol/s3 → service → driver`，`domain` 是共用叶子（ADR-0001）。
- `driver` / `service` 不得依赖 `protocol`；协议库的使用封装在适配层（ADR-0001 §5）。
- 后台任务必须有所有者、取消路径与 shutdown 等待（spec §10）。
