# m01 write-up · 正确性基线 + 连接层地基

> 场景题格式：题目原文 → 我的系统怎么解 → before/after 压测数字 → 权衡与踩坑。
> 数字全部现场跑出来填，不写死目标值（COURSE_SPEC 性能数字纪律）。

## 题目原文

「秒杀下单为什么会超卖？裸的 `SELECT 库存 → 判断 → UPDATE` 三步为什么在高并发下会失败？
用事务和行锁怎么修，修完怎么证明真的修好了？下单量大了之后，HTTP 连接和 MySQL 连接池
应该怎么配，配错了会在哪个指标上先报警？」

## 我的系统怎么解

- **超卖复现**（p1）：`PlaceOrderNaive` 读库存、判断、扣库存、插订单四步不加锁不加事务。
  实测（60 并发打库存=5）：`succeeded=51 finalStock=-46 initialStock=5`——51 个请求都判断"够卖"，库存被扣穿到负数。
- **分布式 ID**（p2）：手写雪花算法（41 位时间戳 + 10 位节点号 + 12 位序列号），时钟回拨直接拒绝而非阻塞等待。
- **事务修复**（p3）：`SELECT ... FOR UPDATE` 行锁 + 单事务 + `request_id` 唯一索引幂等。
  `TestM01P3PlaceOrderTxConcurrentIdentityHolds`（恒等式 initialStock-finalStock==succeeded）、
  `DuplicateRequestIDIsIdempotent`、`ConcurrentDuplicateRequestIDReturnsSameOrder`、`InsufficientStock` 四条并发测试全部 PASS（-race，无数据竞争）；恒等式测试仅内部断言，不打印具体数值。
- **连接治理**（p4）：`http.Server` 四个 timeout 显式非零；`database/sql` 池三参数显式配置；`DB.Stats()` 经 `collectors.NewDBStatsCollector` 进 `/metrics`。

## before / after 压测数字

### EXPLAIN：`request_id` 唯一索引前后

```
-- before（0002 索引未加）
type: ALL
possible_keys: NULL
key: NULL
rows: 1
Extra: Using where

-- after（0002 加上 uk_request_id 唯一索引）
type: const
possible_keys: uk_request_id
key: uk_request_id
key_len: 258
ref: const
rows: 1
Extra: Using index
```

同一条记录、同一个查询条件，唯一变量是索引本身：全表扫 → 命中唯一索引，索引生效可复现。

### 死锁：反序更新触发 1213

```
LATEST DETECTED DEADLOCK
------------------------
*** (1) TRANSACTION: trx id 3673, UPDATE products SET stock = stock - 1 WHERE id = 9102
  HOLDS lock on products id=9102，WAITING FOR lock on products id=9101
*** (2) TRANSACTION: trx id 3672, UPDATE products SET stock = stock - 1 WHERE id = 9101
  HOLDS lock on products id=9101，WAITING FOR lock on products id=9102
```

两个事务反序对两行加锁、互相等待对方持有的锁——InnoDB 检测到循环等待，判 1213，`TestM01P3ReverseLockOrderCausesDeadlock1213` PASS。

### keep-alive 开/关（同 workload）

```
-- keep-alive ON --
Requests      1000, rate 100.10, throughput 99.13
Latencies     min 3.032ms, mean 13.689ms, p50 6.287ms, p90 9.705ms, p95 14.849ms, p99 211.947ms, max 218.839ms
Success       100.00%

-- keep-alive OFF --
Requests      1000, rate 100.11, throughput 100.03
Latencies     min 3.263ms, mean 7.727ms, p50 7.396ms, p90 10.08ms, p95 11.5ms, p99 18.352ms, max 23.673ms
Success       100.00%
```

**如实记录一个混杂变量**：ON 这一轮跑之前连接池是冷的（`open_connections=1`），vegeta 一开始就是 100 req/s 的并发，池子要边扛请求边从 1 条连接扩到 20 条，这段"边打请求边建连接"的成本混进了延迟里，把 p99 拖到 211.947ms；OFF 这一轮跑之前池子已经从上一轮跑热（`open_connections=20`，直接可用），没有这段建连成本。这组"ON 反而更慢"的结果，更可能是两轮之间连接池热身状态不同造成的混杂变量，不是 keep-alive 本身更慢的证据——本机回环网络下 keep-alive 该省的握手成本本来也小，测试脚本应在两轮之间重置池子（关闭重开 `*sql.DB` 或等待 idle 回收）才能做纯净对比，这是下一次迭代脚本时要补的点，不是这次现场编数字掩盖。

### DB 连接池指标（压测前后）

```
-- ON 之前 --
open_connections=1  idle=1  in_use=0  wait_count=0  wait_duration=0

-- ON 之后 --
open_connections=20 idle=20 in_use=0 wait_count=17 wait_duration=0.221586624s

-- OFF 之前 --
open_connections=20 idle=20 in_use=0 wait_count=17 wait_duration=0.221586624s  （沿用上一轮累计值，池已热）

-- OFF 之后 --
open_connections=20 idle=20 in_use=0 wait_count=17 wait_duration=0.221586624s  （无新增等待，池全程够用）
```

`wait_count` 是累计计数器，两轮之间不会自动清零——ON 轮从 0 涨到 17（池从 1 条扩到 20 条期间有请求排队等连接），OFF 轮全程没有新增等待（池子一直够用）。

## 权衡与踩坑

- **时钟回拨为什么选"拒绝"不选"阻塞等待"**：回拨幅度不可预知（NTP 大幅跳变时可能几秒到几分钟），阻塞方案的最坏情况是调用方莫名其妙挂起；直接失败把"要不要重试、重试几次"的决策权交给上层，是可用性权衡，不是能力限制。
- **FOR UPDATE 行锁范围**：锁住的是命中查询条件的那一行（`products` 按主键/唯一条件命中的单行），不是整表；生命周期绑定整个事务，直到 `COMMIT`/`ROLLBACK` 才释放。乐观锁（版本号 CAS）不占行锁、失败靠重试，悲观锁（本关用的 `FOR UPDATE`）让并发请求排队而非重试——高竞争单商品场景下悲观锁更稳，低竞争多商品场景乐观锁吞吐更高，这组对比留给后续模块。
- **四个 timeout 对应的故障场景**：`ReadHeaderTimeout` 防"只发头不发 body"的慢速攻击（Slowloris 一类）；`ReadTimeout` 防整个请求体迟迟发不完；`WriteTimeout` 防响应写不出去；`IdleTimeout` 防 keep-alive 长连接占着不用。四个都漏配等价于"无限等"，实测里最先暴露问题的通常是 `IdleTimeout`——高并发下大量空闲连接攒起来最先耗尽文件描述符/内存。
- **池上限与排队**：`MaxOpenConns` 压到 1、8 并发打慢查询时，`WaitCount` 立刻从 0 变为正数（p4 `TestM01P4PoolLimitProducesWaitCount` 实测）；本次 p5 压测里池子从 1 条扩到 20 条期间同样观测到 `wait_count` 从 0 涨到 17——池上限设得越小，越早在 `wait_count`/`wait_duration` 上报警，比等到请求超时才发现更早。
