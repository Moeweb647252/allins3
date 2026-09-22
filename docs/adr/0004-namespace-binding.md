# ADR-0004：Bucket 到 namespace 的绑定

状态：Accepted。日期：2026-09-22。对应 INV-02。

## 背景

一个服务可注册多种 driver、创建多个实例，每个实例内又有隔离空间。若绑定关系模糊，跨 bucket 的 token、upload 或列举可能串数据。

## 决策

1. 三个概念严格区分：`DriverKind`（实现注册名）、`DriverInstanceId`（该实现的一份配置与连接资源）、`NamespaceId`（实例内隔离空间，对核心层不透明）。
2. 绑定固定为 `BucketId -> (DriverInstanceId, NamespaceId, BindingGeneration)`。逻辑 bucket 名不必等于后端 namespace 名。
3. 首期**拒绝**两个可写 bucket 绑定同一 namespace；别名会影响权限、一致性与列举，需另行设计。
4. driver 必须检查自己可识别的 namespace 别名或包含关系；部署文档必须声明无法自动检测的外部别名约束。
5. 每个请求、分页令牌和 upload 固定绑定版本（binding generation）。ID 与 generation 必须显式配置或稳定保存，普通重启不得随机生成新值。
6. 首期不支持热切换 binding；配置变更须停服、排空请求并处理未完成 upload。删除配置项不是删除后端数据。
7. namespace handle 已绑定隔离空间，接口方法不再接收可任意替换的后端 bucket/path。
8. 权限判断属于认证层，`DriverRegistry` 不做 S3 权限判断。

## 后果

- 列举 token 必须绑定 bucket、binding generation、prefix、delimiter、encoding、鉴权作用域与 driver cursor，并认证加密。
- 契约测试用“多实例同名 key”“跨 namespace 同名 key”验证隔离。
- 新增配置需要 generation 的持久化与迁移策略（P2）。

## 替代方案

- **bucket 名直连后端路径**：无法支持别名与隔离，且会泄露后端结构，被否决。
- **运行期热切换 binding**：需要排空与 upload 迁移协议，首期不做。
