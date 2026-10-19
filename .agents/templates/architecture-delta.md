# 架构增量：<标题>

- 日期：YYYY-MM-DD
- 作者角色：Architect
- 关联 PRD：

## 与 docs/design.md 的差异

- 改了哪些节：
- `abi_version`：不变 | 加一

## 数据路径

```text
（写入、发布、查询、恢复）
```

## crate 落点

| 任务 | crate | 说明 |
|---|---|---|

## 一致性

- 谁分配 `seq`：
- 何时 `fsync`：
- 慢读者如何看到 gap：

## 决策

已有决定见设计第 14 节。这里只记正文里还没有的新决定。

## 交给开发的切分

- Engine：
- Query：
