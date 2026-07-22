# go-seckill

自编生产级秒杀系统（Go + Gin + sqlx + MySQL 8 + Redis 7 + Kafka）。每个模块对应一道经典高并发
场景题，答案是自己压测出来的数字——不是刷题库。写作背景与模块路线图见配套课程仓库的
`courses/seckill-capstone/COURSE_SPEC.md`。

## m01 · 正确性基线 + 连接层地基

- p1 裸下单复现超卖 → p2 手写雪花 ID → p3 事务+行锁+幂等修好超卖 → p4 server/DB 连接治理 → p5 压测归因。

## 快速开始

```bash
cp .env.example .env
docker compose up -d mysql          # m01 只需要 mysql；redis/kafka 是 m02+ 的占位（profiles:future）
./scripts/checks_m01.sh             # 迁移 + EXPLAIN 前后 + go test -race + 死锁复现
go run ./cmd/api                    # 另开终端起 API（:8080）
./scripts/loadtest_m01.sh           # keep-alive on/off 对比 + DB 池指标
```

结果填进 `docs/writeups/m01-baseline.md`。
