# m01 write-up · 正确性基线 + 连接层地基

> 场景题格式：题目原文 → 我的系统怎么解 → before/after 压测数字 → 权衡与踩坑。
> 数字全部现场跑出来填，不写死目标值（COURSE_SPEC 性能数字纪律）。

## 题目原文

「秒杀下单为什么会超卖？裸的 `SELECT 库存 → 判断 → UPDATE` 三步为什么在高并发下会失败？
用事务和行锁怎么修，修完怎么证明真的修好了？下单量大了之后，HTTP 连接和 MySQL 连接池
应该怎么配，配错了会在哪个指标上先报警？」

## 我的系统怎么解

- **超卖复现**（p1）：`PlaceOrderNaive` 读库存、判断、扣库存、插订单四步不加锁不加事务。
  <!-- 填：TestM01P1PlaceOrderNaiveConcurrentOversells 的 succeeded / finalStock / initialStock 实测输出 -->
- **分布式 ID**（p2）：手写雪花算法（41 位时间戳 + 10 位节点号 + 12 位序列号），时钟回拨直接拒绝而非阻塞等待。
- **事务修复**（p3）：`SELECT ... FOR UPDATE` 行锁 + 单事务 + `request_id` 唯一索引幂等。
  <!-- 填：TestM01P3PlaceOrderTxConcurrentIdentityHolds 的恒等式实测数字 -->
- **连接治理**（p4）：`http.Server` 四个 timeout 显式非零；`database/sql` 池三参数显式配置；`DB.Stats()` 经 `collectors.NewDBStatsCollector` 进 `/metrics`。

## before / after 压测数字

### EXPLAIN：`request_id` 唯一索引前后

```
<!-- 粘贴 scripts/checks_m01.sh 第 4/6 步的真实 EXPLAIN 输出（type/key/rows/Extra 两组对比） -->
```

### 死锁：反序更新触发 1213

```
<!-- 粘贴 scripts/checks_m01.sh 抓到的 SHOW ENGINE INNODB STATUS · LATEST DETECTED DEADLOCK 段 -->
```

### keep-alive 开/关（同 workload）

```
<!-- 粘贴 scripts/loadtest_m01.sh 两组 vegeta report 的 Latencies/Success/Throughput -->
```

### DB 连接池指标（压测前后）

```
<!-- 粘贴 go_sql_stats_connections_* 系列 before/after 数字，尤其 wait_count/wait_duration -->
```

## 权衡与踩坑

<!--
- 时钟回拨为什么选"拒绝"不选"阻塞等待"？
- FOR UPDATE 行锁范围多大？跟乐观锁（m03 会对比）比性能取舍在哪？
- 四个 timeout 分别对应哪种攻击/故障场景？漏配哪一个最先出事？
- 池上限设多小会先在哪个指标上体现出排队（对照 p4/p5 的 WaitCount 实测）？
-->
