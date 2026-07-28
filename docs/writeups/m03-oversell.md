# m03 write-up · 防超卖三方案 + 四列对比表

> 场景题格式：题目原文 → 我的系统怎么解 → 四列对比表（真实数字）→ 权衡与踩坑。
> 数字全部现场跑出来填，不写死目标值（COURSE_SPEC 性能数字纪律）。

## 题目原文

「秒杀怎么防超卖？乐观锁和悲观锁怎么选？分布式锁有哪些坑？Redis + Lua 原子预扣具体怎么做，
它跟前面几种方案比，代价和收益分别是什么？」

## 我的系统怎么解

- **DB 乐观锁**（p1）两种形态：
  - `PlaceOrderByVersionCAS`：读 `stock`+`version`，`UPDATE ... WHERE id=? AND version=?`，
    `RowsAffected==0` 判定"被抢先"、回到第一步重读重试（`MaxCASRetries=10`），耗尽返回
    `ErrCASRetriesExhausted`（跟"库存不足"是两个不同的哨兵错误，不能混）。
  - `PlaceOrderByConditionalUpdate`：`UPDATE ... WHERE id=? AND stock>=?`，把判断塞进 `WHERE`，
    单条 SQL 靠 InnoDB 行锁天然串行，没有读-改-写窗口，也就不需要重试。
- **Redis 分布式锁**（p2/p3）：`Acquire` 一条 `SET NX PX` 抢锁（token 用 `crypto/rand` 生成）；
  `Release` 跑 `releaseScript`（Lua 里比对 token 再 `DEL`，返回 0 转 `ErrLockLost`），挡住"裸 DEL
  删掉别人锁"这类事故；`StartWatchdog` 起心跳 goroutine 跑 `renewScript`（比对 token 再
  `PEXPIRE`），补上"临界区比 TTL 长导致锁被打穿"这个失效模式。锁内 `SELECT` 故意不加
  `FOR UPDATE`——互斥完全由 Redis 锁提供，两套机制混用说不清谁在保护谁。
- **Redis + Lua 原子预扣**（p4，课眼）：`preDeductScript` 一段 Lua 原子完成"判断 key 是否存在 →
  判断余量够不够 → `DECRBY`"，返回剩余量或 `-1`；key 不存在按不足处理（不能当 0 继续扣，否则一个
  没预热的商品会被无限放行）。`PlaceOrderWithPreDeduct` 预扣成功后同步落库，事务任何一步失败都调
  `RollbackPreDeduct`（`rollbackScript` 一句 `INCRBY` 还回去）——没有这一步，"预扣成功、落库失败"
  这条路径会让库存凭空蒸发（Redis 说卖光了，DB 里却没有对应订单）。

单元测试证据（`go test -race ./internal/deduct/... -run '^TestM03' -v -count=1`，十条全 PASS）：

```
版本号 CAS：60 并发抢 20 库存 → 成功 10 · 库存不足 0 · 重试耗尽 50；
  总 CAS 尝试 555 次，平均每次调用尝试 9.25 次（重试上限 10）
条件更新：60 并发抢 20 库存 → 成功 20 · 库存不足 40；零重试
锁内扣减：60 并发 → 成功 1 · 没抢到锁 59；没抢到锁的直接快速失败（秒杀不排队）
临界区重叠复现：TTL=300ms 但临界区要跑 700ms；A、B 两个不同 token 同时持有同一把锁
续期看门狗：TTL=300ms、心跳=100ms、临界区 900ms（跨 3 个 TTL 周期）；
  期间 B 尝试抢锁 18 次全部被拒，A 最后正常释放
Lua 原子预扣：60 并发抢 20 库存 → 成功 20 · 库存不足 40（这 40 次一条 SQL 都没打）；
  Redis 剩余 0、DB 剩余 0、订单 20 行，三者闭合
```

## 四列对比表（同 workload：库存 100 万/不会卖光、10s @ 200rps、同一商品同一起点）

`./scripts/loadtest_m03.sh` 五轮结果：

| approach | 成功 | conflict/insufficient | p99 | mean | go_sql wait_count |
|---|---|---|---|---|---|
| pessimistic（m01 基线，`FOR UPDATE`） | 2000/2000 | 0 | 15.928ms | 4.143ms | 0 |
| cas（p1 版本号 CAS） | 1938/2000 | 62（重试耗尽） | 109.39ms | 8.327ms | 0 |
| conditional（p1 条件更新） | 2000/2000 | 0 | 9.807ms | 3.480ms | 0 |
| lock（p2/p3 分布式锁） | 1896/2000 | 104（没抢到锁） | 11.728ms | 3.816ms | 0 |
| lua（p4 原子预扣） | 2000/2000 | 0 | 227.839ms | 15.995ms | 111（累计等待 7.097s） |

`seckill_cas_attempts_total` 累计 3153 次 ÷ 2000 请求 ≈ **1.58 次/单**——跟单元测试里 60 并发挤同一
行时实测的 **9.25 次/单** 差出接近 6 倍，同一份 CAS 代码，唯一差别是"请求分散到达"还是"挤在同一
瞬间"，这正是"乐观锁适合低竞争"这句八股背后的真实机理。

恒等式（初始库存 100 万 − 剩余库存 ≡ 本轮订单数）五轮全部 `OK`，没有一轮 `BROKEN`。

## 权衡与踩坑

- **conflict 不能只看数字大小**：`cas` 的 conflict 是"重试到上限仍未成功"（重——每次重试都是一次真
  实 DB 往返，藏在 `seckill_cas_attempts_total` 里不体现在 conflict 计数上）；`lock` 的 conflict
  是"没抢到锁直接快速失败"（轻——一次 `SET NX` 判断不成立立刻返回，不占用任何 DB 资源）。这次压测里
  `lock` 的 conflict 数字（104）比 `cas`（62）更高，不能据此说"分布式锁比乐观锁差"——两个数字背后
  是完全不同重量级的失败，真正该比的是吞吐、p99、DB 池占用这些直接反映系统代价的指标。
- **如实记录一个反直觉的结果，不回避**：`lua` 这一轮的 p99/mean 是五轮里最差的，也是唯一出现
  `go_sql_wait_count_total` 非零的一轮，跟"Lua 预扣热路径不碰 DB 所以更轻"的直觉正相反。原因是这次
  压测把库存给到 100 万（打不完），Redis 那道闸门形同虚设——2000 个请求全部预扣成功，一个都没被挡在
  Redis 层，于是 `lua` 这条路径的每个请求变成"一次 Redis EVAL + 一次完整 DB 事务"，比 `conditional`
  （只有一次 DB `UPDATE`）多了一整趟网络往返；而且几乎不设防地全员放行，导致 2000 个事务近乎同时涌
  向 DB 抢同一行的写锁，连接池才出现等待。`pessimistic`/`conditional` 同样在打同一行，但临界区更短、
  没有额外的 Redis 往返，没有把这么多并发请求同时喂给 DB，所以没触发池等待。这印证了课程本身的纪律：
  库存给得很足时，Lua 预扣"卖光后完全不碰 DB"这个真正的优势观察不到，真正的悬殊要在"库存已尽、流量
  还在打"的场景（单元测试那两条：`TestM03P4PreDeductStopsAtZeroAndFailsFast`、
  `TestM03P4PreDeductConcurrentNoOversell` 里 40 次库存不足"一条 SQL 都没打"）才看得最清楚，压测条件
  下的结论不能无限外推——这次的真实数据甚至是反过来的，这个诚实的发现比按剧本硬凑"lua 更快"更值钱。
- **Lua 原子性的边界**：Redis 执行脚本期间不插入别的客户端命令，这个保证严格地只覆盖 Redis 自己——
  它不知道 MySQL 的存在，管不到"Redis 扣减"和"MySQL 落库"这两个系统之间的一致性。"预扣成功、落库
  失败"是一条真实存在的路径（`TestM03P4OrderFailureRollsBackPreDeduct` 用重复 `request_id` 撞唯一
  索引复现），必须自己写补偿脚本把预扣还回去，这是这个方案吞吐背后要自己兜的复杂度。
- **锁误删与临界区重叠是两类不同的事故，但同源**：`releaseScript`/`renewScript` 都必须先比对 token
  再动锁，本质是同一条纪律——"确认所有权"和"动这把锁"之间不能留窗口。裸 `DEL` 或无条件 `PEXPIRE`
  会让自己的 bug（丢了锁）升级成别人的事故（锁被误删/被顶住变成全局死锁）。
