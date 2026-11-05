# Redis 内存模型对照（给 QDB）

一手来源：

- [Key eviction](https://redis.io/docs/latest/develop/reference/eviction/)
- [Memory optimization](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/)
- [redis.conf MEMORY MANAGEMENT](https://github.com/redis/redis/blob/HEAD/redis.conf)
- [OOM error](https://redis.io/docs/latest/develop/reference/errors/common/oom/)
- [INFO memory](https://redis.io/docs/latest/commands/info/)

本文只归纳 Redis **怎么适配不同机器内存**，以及 QDB 应对齐什么、不能照搬什么。不改变 `docs/design.md` 的权威条款；若要落地改设计，另开增量。

## 1. Redis 怎么做

### 1.1 默认：按需增长，不写死「数据集上限」

64 位上 `maxmemory` 缺省为 **0 = 不限制数据集内存**。进程按写入增长；机器能撑多大，数据就可以多大（直到 OS OOM）。  
32 位才有隐式约 3GB 上限。

要点：**引擎不内置「最多 N 条 K 线 / N 个键」这种业务硬顶**；容量是部署参数，不是写死在热路径里的魔法数。

### 1.2 适配机器：`maxmemory` = 字节预算

运维按机器可用 RAM 设：

```text
maxmemory 8gb
```

也可运行时 `CONFIG SET maxmemory ...`。  
到顶之后行为由 **`maxmemory-policy`** 决定，而不是再偷偷截断某类结构。

### 1.3 到顶策略：显式、可配、对写入可见

| 策略族 | 含义 |
|---|---|
| `noeviction`（当前 conf 默认） | **不丢数据**；需要再占内存的写命令返回错误（OOM）；读仍可用 |
| `allkeys-*` / `volatile-*` | 按 LRU / LFU / LRM / random / TTL 等 **显式淘汰键**，给缓存场景用 |

缓存淘汰是产品选择；主库常用 `noeviction` + 够大的 `maxmemory`，宁可写失败也不静默丢权威数据。

### 1.4 可观测

`INFO memory`：`used_memory`、`used_memory_rss`、`maxmemory`、`maxmemory_policy`、碎片率等。  
超限时错误语义清楚（OOM），不是「查出来少了几根却当正常」。

### 1.5 分配器与 RSS

数据用 jemalloc（等）按需分配；删键后 RSS 不一定立刻还给 OS，但逻辑占用下降，后续可复用。  
**不为「也许用到的 65536 槽」在启动时把整块用户数据清零进 RSS**——和「定长文件映射」是另一类问题。

## 2. 和 QDB 的差异（不能照搬的部分）

| | Redis | QDB（现状） |
|---|---|---|
| 用户数据布局 | 堆上动态分配 | 共享内存 **定长** `region.bin`（符号/快照/未闭合 K 线/环） |
| 扩缩 | 键多少用多少 | 建区时就要定 `max_symbols` / `ring_capacity` |
| 内存顶 | `maxmemory` 字节 | 目前主要是区域逻辑长度 + 引擎私有堆（ticks、sealed、用户表、frames） |
| 到顶行为 | policy + 明确错误 | 符号槽满等有错误；私有堆此前曾加过硬条数截断（已撤销） |

定长共享内存是 IPC/ABI 约束，Redis 没有对等物。机器适配时：

1. **区域容量** = 类似「这台机打算给热总线多少固定映射」（环境变量 / 配置），小机小、大机大。  
2. **私有堆** = 更接近 Redis 的 `maxmemory`：字节预算 + 到顶策略，而不是「每个 period 最多 8192 根」。

## 3. 应对齐的 Redis 原则（给 QDB）

1. **配置管容量，代码不写死业务条数上限**  
   已闭合 K 线、用户表行等：不设魔法 N；要控内存用保留策略或 `maxmemory` 式预算。

2. **`maxmemory = 0` 表示不限制（大机默认）**  
   小机部署显式设预算；不要靠引擎里藏一个「席位默认丢数据」。

3. **到顶要显式**  
   - 权威路径（WAL 已提交的行情/表）：优先 `noeviction` → 写入失败 / 明确错误。  
   - 可重建的热缓存（例如仅内存 ticks 窗）：可用保留窗或淘汰，且手册写清，查询超窗不装成「全世界就这些」。

4. **可观测**  
   `/metrics` 或等价：区域逻辑长度、RSS、私有堆估算、`maxmemory`、是否在淘汰、拒绝写次数。

5. **按需落页**  
   区域创建不整文件预触摸（已做）≈ Redis「用多少分配多少」在定长文件上的折中。

## 4. 建议的配置面（设计方向，未实现勿写进手册当已提供）

| 旋钮 | 类似 Redis | 作用 |
|---|---|---|
| `QDB_MAX_SYMBOLS` / `QDB_RING_CAPACITY` | 实例规格 | 定长区域按机器选型（已有） |
| `QDB_MAXMEMORY`（拟） | `maxmemory` | 引擎私有堆字节上限；`0` = 不限 |
| `QDB_MAXMEMORY_POLICY`（拟） | `maxmemory-policy` | `noeviction` \| 按事件时间丢最旧 tick/sealed 等（需单独设计，避免 silently 改权威语义） |
| `QDB_TICK_RETENTION_SECS` | 可选 TTL 类 | 对可重建的 tick 热窗；`0` = 不剪（已有方向） |

第一版若只学一件事：**用字节预算 + 显式策略适配机器，禁止在 crate 里写死「最多留 N 根已闭合 K 线」这类静默截断。**

## 5. 当前仓库落点

- 已有：区域环境变量、创建不 `fill(0)`、Tick 保留可配（`0` 不剪）、已闭合 K 线无条数硬上限、`QDB_MAXMEMORY` + `noeviction`（到顶拒绝增长，不淘汰）。
- 未有：LRU/LFU 等淘汰策略、分配器级 RSS 计量。淘汰策略要另开设计，不能在权威写入路径上静默丢数据。
