# m02 write-up · 缓存层：穿透 / 击穿 / 雪崩 + 一致性

> 场景题格式：题目原文 → 我的系统怎么解 → before/after 压测数字 → 权衡与踩坑。
> 数字全部现场跑出来填，不写死目标值（COURSE_SPEC 性能数字纪律）。

## 题目原文

「给商品读路径加一层 Redis 缓存之后，为什么反而多出穿透 / 击穿 / 雪崩三类专属事故，
每一类怎么解？DB 里的数据改了，缓存里那份旧数据怎么办——先删缓存再更库，还是先更库再删缓存，
两者留下的脏窗口差多少？」

## 我的系统怎么解

- **cache-aside 读路径 + 预热**（p1）：`ProductCache.Get` 把 Redis 读结果分三支处理——命中（err
  为 nil）直接解 JSON 返回、未命中（`redis.Nil` 哨兵）走 `loadAndFill` 回源、真故障直接返回
  error，不混进回源分支。回填必须带 TTL（传 0 等于永不过期）。`Warm` 复用 `loadAndFill` 做批量
  预热，遇到不存在的商品 `continue` 而不是让整批失败。
  `TestM02P1GetFillsCacheThenSecondReadSkipsDB` 实测：`DB loads=1`（两次 `Get`）、`TTL=36s`；
  `TestM02P1WarmPrefillsSoFirstGetIsHit` 实测：预热期间回源 3 次，之后两次 `Get` 回源 0 次。
- **穿透**（p2）：`loadAndFill` 回源拿到 `ErrProductNotFound` 时写一个不可能被解析成合法商品的
  占位符（`NotFoundPlaceholder`），TTL 用秒级的 `MissTTL`。`TestM02P2MissingIDIsNullCachedAndStopsDBLoads`
  实测：不存在的 id 连打 6 次，DB 只回源 1 次；缓存里的原始值 `"__NULL__"`，`TTL=5s`。
- **雪崩**（p2）：`ttlWithJitter` 给回填 TTL 叠 `[0, TTLJitter)` 的随机量，只往上加不往下减。
  `TestM02P2WarmSpreadsTTLWithJitter` 实测：预热 12 个 key 有 12 个不同过期时刻，
  `min=30.351s max=39.815s 极差=9.464s`（base TTL=30s，jitter 上限=10s）。
- **击穿**（p3，课眼）：`sf singleflight.Group` 是 `ProductCache` 的长期字段，`Get` 把
  未命中分支的"回源+回填"整段包进 `sf.Do` 的闭包，合并结果按值拷贝给每个调用方，不共享指针。
  `TestM02P3ConcurrentMissCollapsesToOneDBLoad` 实测：100 个并发 `Get` 同一个冷 key，DB 回源 1 次；
  `TestM02P3SharedLoadGivesEachCallerItsOwnCopy` 实测：两个调用方拿到的指针不同，改一份不影响另一份。
- **一致性**（p4）：`UpdateStock` 顺序固定为先 `repo.UpdateStock` 落库、成功后再 `rdb.Del` 删 key，
  删除失败包成 error 返回，不吞。`TestM02P4UpdateStockDeletesKeyAndNextReadSeesNewValue` 实测：
  更新后缓存 key 数=0，首读回源 1 次拿到新值 `stock=3`；`TestM02P4StaleFillRaceWindowIsReproducible`
  用可阻塞的假回源精确卡住时序，实测复现出脏数据本身：缓存里回填的旧值 `stock=10`，
  而 DB 已经是 `stock=3`，这条脏记录会一直存活到 TTL 到期。

## before / after 压测数字

同一台机器、同一个 workload（`RATE=200 DURATION=10s`）打同一个商品 `products.id=9700`，
两轮唯一差别是有没有经过 `ProductCache`：`/products/:id`（走缓存）vs
`/debug/products/:id/nocache`（同一个 `SQLProductRepo.LoadProduct`，直连 MySQL）。

```
-- 缓存 ON（/products/9700）--
Requests      2000, rate 200.10, throughput 200.08
Latencies     min 362.25µs, mean 1.251ms, p50 1.203ms, p90 1.732ms, p95 1.902ms, p99 2.521ms, max 17.253ms
Success       100.00%
seckill_product_db_loads          增量 1   (1 -> 2)
seckill_product_reads_total{cached} 增量 2000 (1 -> 2001)
go_sql_open_connections           1 -> 1（全程没变）

-- 缓存 OFF（/debug/products/9700/nocache，同 SQL）--
Requests      2000, rate 200.10, throughput 200.05
Latencies     min 666.583µs, mean 2.008ms, p50 1.947ms, p90 2.832ms, p95 3.034ms, p99 3.927ms, max 17.995ms
Success       100.00%
go_sql_open_connections           1 -> 3（新增两条并发 DB 连接）
```

命中率 = `1 - db_loads增量/cached读请求增量` = `1 - 1/2000 = 99.95%`，跟"第一发 miss、
其余 1999 发全命中"的预期对得上。

相对差异（同机、同 workload、唯一变量是有没有缓存）：p50 缓存快约 38%（1.203ms vs 1.947ms），
p99 缓存快约 36%（2.521ms vs 3.927ms）。归因不停在"缓存快一些"这句空话上——`go_sql_open_connections`
在缓存 ON 那轮全程停在 1，OFF 那轮涨到 3：直连 DB 时每个请求都要真去打一次 MySQL、占一条连接
直到查询返回，缓存挡住的不只是延迟数字，还挡住了 DB 连接池被打开的压力，这条因果链比单看
p99 更有说服力。

## 权衡与踩坑

- **空值缓存 TTL 为什么必须是秒级**：这条记录缓存的是"这个 id 现在查不到"这个极易变化的事实——
  商品随时可能真的上架，一旦上架，这条"不存在"就成了脏数据，且脏着的每一秒对所有用户都是 404，
  比库存数字旧几秒严重得多。正常数据的 TTL 可以是分钟级，`MissTTL` 必须远短于它。
- **singleflight 只在单进程内合并**：这个服务部署 N 个实例，热点 key 过期瞬间 DB 会被同样的
  查询打 N 次而不是 1 次——这不是 bug，是它的作用范围。要做到跨实例只回源一次，需要分布式锁一类
  机制（m03 要压测对比的东西）。N 次和上万次相比，已经把问题从"DB 被打穿"降到"DB 多了几条查询"。
- **先更库再删缓存 vs 先删缓存再更库**：前者正常情况下脏窗口量级是"一次回填"（只有一种精确
  时序交错才踩得上——读已经从 DB 拿到旧值、但还没写回缓存，此刻写请求整段跑完，读才回填进旧值）；
  后者脏窗口量级是"一个 TTL"（删完到写库成功之间整段空窗，任何一个读落进来都会未命中回源读到
  旧值并回填，且这份脏值没人会再纠正）。选删除不选覆盖，是因为并发写请求写 DB 和写缓存的先后
  顺序可能相反，覆盖没有自愈机制，删除是幂等的。
- **自己复现出来的那条脏数据**：`TestM02P4StaleFillRaceWindowIsReproducible` 用可阻塞的假回源
  精确卡住时序后，缓存里 `stock=10` 而 DB 已经是 `stock=3`——面试官问"缓存和数据库不一致怎么办"，
  比背延迟双删更有说服力的答案是"我把这个窗口测出来了，它的上限是我的 TTL"。
- **删除失败为什么必须上报不能吞**：DB 已经改了、缓存还留着旧值，是最恶劣的一种脏——沉默的、
  没有人知道的脏。key 没被真的删掉，后续读请求会直接命中这个旧值，根本走不到回源/回填那条路径，
  永远不会自愈，只能等 TTL 到期；调用方还以为自己更新成功了。
- **两轮压测为什么必须是同一个 SQL**：`/debug/products/:id/nocache` 特意调的是同一个
  `SQLProductRepo.LoadProduct`，跟缓存路径回源走完全相同的查询——如果拿 m01 那个只查
  `stock` 一列的旧调试端点当对照组，两轮差异里就会同时混进"有没有缓存"和"SQL 不一样"两个
  变量，归因直接失效。一次实验只留一个变量，跟 m01 用同一条探针记录做 EXPLAIN 前后对比是
  同一条纪律。
