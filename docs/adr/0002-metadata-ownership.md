# ADR-0002：元数据所有权与单一提交协议

状态：Accepted。日期：2026-09-22。

## 背景

对象字节、长度、ETag、checksum、用户元数据和内部 revision 若由不同子系统分别写入，就可能出现“字节已写、元数据未写”的半对象，或在 HEAD 与 GET 之间看到不一致的 revision。

## 决策

1. 对象字节、`size`、ETag、checksum、`ContentProperties`、用户元数据和 `ObjectRevision` 由**同一个逻辑提交协议**一起发布；`commit` 是唯一发布入口。
2. `ObjectRevision` 由 driver 定义，是内容与元数据的绑定标识；元数据更新（即使内容与 ETag 不变）必须产生新 revision。
3. `ObjectRevision` **不是** ETag，也**不是** S3 `versionId`。ETag、checksum、revision 是三个独立字段。
4. `stat` 返回的 revision 是后续 `read` 的**必填**条件；driver 无法读取该 revision 时返回 `RevisionUnavailable`，不得回退为按 key 读取当前对象。
5. 核心层不得“先写字节、再独立写一条元数据记录并把两次成功当作原子成功”。driver 若内部组合“数据存储 + 元数据索引”，必须自行实现并证明一致性。

## 后果

- 读路径固定为 stat → 条件判断 → Range 解析 → 携带 `expected_revision` 读取；revision 不可读时按预算重走完整流程。
- 契约测试强制校验“旧 revision 读不到”“元数据更新换 revision”“暂存不可见”三条不变量。
- 对已有外部数据，driver 可返回其可信的原生 ETag，但必须在适配说明中声明，且不能替代 revision。

## 替代方案

- **ETag 兼作 revision**：无法区分元数据更新，被否决。
- **两次独立写入**：会产生半对象，违反 INV-03，被否决。
