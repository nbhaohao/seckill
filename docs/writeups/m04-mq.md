# m04 write-up · Kafka 削峰异步化：最终一致 / 幂等消费 / 死信 / lag

> 场景题格式：题目原文 → 我的系统怎么解 → 四组数字对比（真实数字）→ 权衡与踩坑。
> 数字全部现场跑出来填，不写死目标值（COURSE_SPEC 性能数字纪律）。

## 题目原文

「m03 的 Lua 预扣已经把库存闸门搬到 Redis，为什么还要把预扣后的 MySQL 落库塞进 Kafka？
请求提前返回以后，谁来保证订单最终不丢、不重、不被毒丸永远堵住？」

## 我的系统怎么解

- **p1 · 削峰链路打通**：`EnqueueOrder` 复用 m03 的 `deduct.PreDeduct` 原子扣 Redis 名额，把
  `OrderCreated` 编码进 `kgo.Record`，`Record.Key` 固定写 `productID`（同商品哈希进同一分区，只
  保证分区内顺序，不是全 topic 顺序）；`ProduceSync(ctx, record).FirstErr()` 成功才返回 202，失败
  用**独立的短 deadline context**回滚预扣（不能沿用已经因超时被取消的原 ctx，否则回滚命令会被一
  起取消）。消费侧 `PollBatch` 遍历完整 `RecordIter`，交给复用 m01 `PlaceOrderTx` 的 `PlaceRecord`
  落库。实测 202 到 DB 可见的最终一致窗口 **85.787417ms**；模拟不可达 broker 后 Redis 库存从预扣后
  的 2 恢复为 3，回滚生效。
- **p2 · 幂等消费（课眼）**：`NewManualCommitConsumer` 加 `kgo.DisableAutoCommit()`，把"MySQL 已
  提交"和"Kafka offset 已提交"拆成两个独立可观察的动作；`CommitProcessed` 只在业务处理成功后调
  `CommitRecords`，顺序不可反——先提交再处理，进程崩在中间会让这条消息永远没人再碰，先处理再提交
  则崩溃只会造成安全重放。重复消费靠 `orders.request_id` 唯一索引收敛：故意在提交前关闭消费者、用
  同 group 重启触发重放，实测**同一条 Kafka 消息被消费 2 次，订单号始终是 208602397361115136，
  DB 行数保持 1**。
- **p3 · 有界退避 + 死信 DLT**：`ProcessWithRetry` 按 `BaseBackoff` 指数增长等待（`timer` 配合
  `ctx.Done()` 的 `select`，不用 `time.Sleep`，否则收不到取消信号），`MaxAttempts` 是总尝试次数，
  最后一次失败不再空等。耗尽后复制原 `Key`/`Value` 投 `order.created.DLT`，附
  `failure-reason`/`retry-count`/`original-topic`/`original-partition`/`original-offset` 五个诊断
  header；**只有 DLT `ProduceSync` 成功后才提交原 offset**，DLT 失败则原 offset 绝不能提交（否则
  消息在两个 topic 都消失）。实测毒丸退避序列 `backoffs=[5ms 10ms]`，DLT headers 完整可读
  （`retry-count:3 original-partition:2 original-offset:41 original-topic:order.created`）；同一分
  区里毒丸后紧跟的正常请求 `m04-p3-good` 在毒丸进 DLT 后立刻正常落单；另一测试对同一 `request_id`
  重试 3 次，`DB 行数保持 1`（order id=208602399059808256），证明重试全程复用同一幂等键。
- **p4 · lag 观测 + 消费者扩容**：`GroupLagTotal` 用 `kadm.NewClient(client).Lag(ctx, group)` 查
  询，依次检查外层 `err`、`lags[group]` 是否存在、`described.Error()`（`DescribeErr`/`FetchErr`
  合并），三层都没错才信 `described.Lag.Total()`——任何一层出错都直接返回 error，绝不伪装成 0（0
  在这个语义下等于"确认已排空"）。`RegisterLagGauge` 用 `prometheus.NewGaugeFunc` 每次 scrape 自
  建 2 秒 deadline 查询，出错返回 `-1`（lag 天然非负，-1 是不会跟真实值混淆的显式异常信号）。实测
  投 12 条只提交 1 条后 `lag timeline=[11 0]`——先涨到 11、消费完剩余记录后归零；3 分区搭配 4 个
  同组消费者，`members=4 active=3 idle=1`，第 4 个消费者确认没有分区可拿，分区数是并行度的硬天花
  板，加消费者数不改变这个上限。

单元测试证据（`go test -race ./internal/mq/... -run '^TestM04' -v -count=1`，全部 PASS）+
`./scripts/checks_m04.sh` 四步冒烟（`CREATE...partitions=3` → `PRODUCE...offset=0` →
`CONSUME...value=smoke-order` → `LAG...total=0 state=Stable members=1`）与 m01–m03 回归全绿。

## 四组数字对比（`./scripts/loadtest_m04.sh`，同商品同 workload：10s @ 200rps，2000 请求）

| | sync（m03 同步落库） | async（m04 预扣后 202） |
|---|---|---|
| Requests / rate / throughput | 2000 / 200.10 / 200.04 | 2000 / 200.10 / 200.09 |
| Latencies min/mean/p50/p90/p95/**p99**/max | 1.787ms / 5.075ms / 3.219ms / 4.55ms / 5.915ms / **71.94ms** / 81.387ms | 508.625µs / 1.33ms / 1.165ms / 1.838ms / 2.248ms / **5.254ms** / 16.794ms |
| Status | 200:2000 | 202:2000 |
| Kafka lag | 不适用（无队列） | 峰值 **7**，排空到 0 约 **2s**（压测 10s，drain 在 t=12s） |
| 恒等式判断时刻 | 压测结束立即查，`OK` | 必须等 lag=0 后再查，`OK`（提前查会撞见暂时不闭合的正常窗口） |

## 权衡与踩坑

- **API p99 下降不能单独说"系统吞吐提升"**：async 轮 p99 从 71.94ms 降到 5.254ms，看起来快了十几
  倍，但 DB 该写的 2000 单一条没少——只是把 DB 写入从请求时间轴挪到了消费者时间轴。真正该看的是三
  件独立的事：用户等待（API p99/throughput）、DB 实际处理能力（这次两轮总耗时量级接近，说明 DB 写
  入速率没变）、队列扛的债（lag 峰值 7、排空 2s）。只报第一个数字会把"用户体验变好"偷换成"系统总
  工作变快"。
- **提交顺序不可逆，且不能靠 CommitRecords 本身的幂等性掩盖丢单**：`CommitRecords` 对同一 offset
  重复提交不会报错（本质是覆盖写 `__consumer_offsets`），但这只保护"重复提交"，保护不了"提交顺序
  颠倒"——先提交再处理，进程崩在中间的那条消息永远没有机会再被拉取，任何后续重放都无法覆盖已经跳
  过的 offset。p2 的正确性来自"先处理成功、后提交"这一固定顺序，不是来自提交动作本身的幂等。
- **DLT 是新的可靠归宿，不是业务成功**：耗尽重试后提交原 offset 的前提是 DLT `ProduceSync` 已经
  ack，这个提交代表"失败消息已经安全转移到另一个待处理通道"，不代表这一单业务上单出来了。如果颠
  倒顺序（先提交原 offset 再投 DLT），进程崩在两者之间会让消息在原 topic 和 DLT 里同时消失——这是
  本模块里唯一一条"宁可暂时卡住分区，也不能提交"的判断。
- **投递失败回滚仍不是 outbox**：`EnqueueOrder` 的补偿代码能处理"进程活着、看到 `ProduceSync`
  报错"这一种情形，但覆盖不了"进程刚好崩在 Redis 预扣成功和 Kafka 确认之间"这个窗口——那种情况下
  没有任何代码有机会执行回滚，名额直接泄漏，没有对账就发现不了。outbox 的保证来自单个数据库事务
  的原子性（业务写和待发事件绑在同一次 commit），不依赖进程存活去事后补救；但这条链路的库存闸门
  先发生在 Redis，outbox 也没法让 Redis 和 MySQL 同事务，所以这里仍需对账兜底，不是本期实现范围。
- **为什么不上 2PC / TCC / Saga**：2PC 要协调者等最慢参与者 prepare+commit，把可用性和延迟绑死在
  协调器上，不适合秒杀这种对延迟敏感的热路径；TCC 要为预扣、确认、取消三个动作各自设计严格幂等，
  这条链路只有"扣库存 + 异步落库"两步，复杂度远超问题规模；Saga 适合更长的多服务补偿编排，这里链
  路短、DLT 加对账已经能覆盖失败面，引入 Saga 只是多一层没用上的框架。
- **分区数是并行度硬天花板**：3 分区配 4 个同组消费者，实测 `active=3 idle=1`——继续加消费者到
  30 台，理论活动数仍是 3，其余全部站岗。想真正提高并行度必须先增加分区，但改分区数会改变 key 到
  分区的映射，历史消息的分区不会重新分布，本课不做这个动态操作。
