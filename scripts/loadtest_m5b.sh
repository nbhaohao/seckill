#!/usr/bin/env bash
# 已就位（AI 生成）：sk-m5b p2 的 before/after 对照台。
#
# 两轮做的是同一件业务（下一单），唯一差别是库存放在几个 key 上：
#   lua     = m03 p4 的单 key 原子预扣（seckill:stock:<id>）—— before 组，m05 冻结版一个字不动
#   bucket  = sk-m5b 的 N 桶分段库存（seckill:stock:<id>:b0..bN-1）—— after 组
# 同 workload、同商品、同起点，对比才只剩「key 拆没拆」这一个变量。
#
# 前置：另开终端跑 `go run ./cmd/api`（默认 :8080，需要 mysql + redis 都在跑）。
set -euo pipefail
cd "$(dirname "$0")/.."

ADDR="${ADDR:-http://127.0.0.1:8080}"
RATE="${RATE:-200}"
DURATION="${DURATION:-10s}"
PRODUCT_ID="${PRODUCT_ID:-9950}"
# 库存给足，让两轮都在"抢得到"的状态下比吞吐；卖光行为与碎片代价由单元测试断言。
STOCK="${STOCK:-1000000}"
BUCKETS="${BUCKETS:-8}"     # N
PROBES="${PROBES:-2}"       # k

VEGETA="go run github.com/tsenart/vegeta/v12@v12.13.0"
MYSQL_EXEC_DB="docker compose exec -T mysql mysql -uroot -proot seckill"
REDIS="docker compose exec -T redis redis-cli"

log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

BODY="$(mktemp)"
trap 'rm -f "$BODY"' EXIT
printf '{"product_id":%s,"user_id":1,"quantity":1}' "$PRODUCT_ID" > "$BODY"

reset_round() {
  $MYSQL_EXEC_DB -e "
    DELETE FROM orders WHERE product_id = ${PRODUCT_ID};
    INSERT INTO products (id, name, stock, version) VALUES (${PRODUCT_ID}, 'loadtest-bucket', ${STOCK}, 0)
      ON DUPLICATE KEY UPDATE stock = ${STOCK}, version = 0;" >/dev/null
  # 单 key 与各桶 key 都清掉，两轮各自重新预热，互不继承残留。
  local keys="seckill:stock:${PRODUCT_ID}"
  for ((b = 0; b < BUCKETS; b++)); do keys="${keys} seckill:stock:${PRODUCT_ID}:b${b}"; done
  # shellcheck disable=SC2086
  $REDIS DEL $keys >/dev/null
  # commandstats 是累计值，不清零的话第二轮会带着第一轮的账，两轮就没法比。
  $REDIS CONFIG RESETSTAT >/dev/null
  $REDIS SLOWLOG RESET >/dev/null
}

identity_check() {
  # 恒等式：初始库存 − DB 剩余库存 ≡ 本轮订单数。破了的那轮数字作废。
  $MYSQL_EXEC_DB -N -e "
    SELECT CONCAT('DB 剩余=', p.stock,
                  ' 已扣=', ${STOCK} - p.stock,
                  ' 订单数=', (SELECT COUNT(*) FROM orders o WHERE o.product_id = p.id),
                  ' 恒等式=', IF(${STOCK} - p.stock = (SELECT COUNT(*) FROM orders o WHERE o.product_id = p.id), 'OK', 'BROKEN'))
    FROM products p WHERE p.id = ${PRODUCT_ID};"
}

redis_evidence() {
  echo "-- Redis commandstats（本轮 EVALSHA/GET/DECRBY 的调用数与耗时）--"
  $REDIS INFO commandstats | grep -E 'cmdstat_(evalsha|eval|get|mget|set|mset|decrby|incrby)' || echo "(无)"
  echo "-- Redis slowlog（本轮最慢的几条；两轮条数与命令形状的差异就是热点的形状变化）--"
  $REDIS SLOWLOG LEN
  $REDIS SLOWLOG GET 5 || true
  echo "-- keyspace（分桶轮应看到 N 个 stock key 而不是 1 个）--"
  $REDIS KEYS "seckill:stock:${PRODUCT_ID}*" | sort
}

outcomes() {
  echo "-- 结果分类（success / insufficient / conflict / error）--"
  curl -s "${ADDR}/metrics" | grep "^seckill_deduct_outcomes_total{approach=\"$1\"" || echo "(无)"
  echo "-- DB 池指标 --"
  curl -s "${ADDR}/metrics" | grep -E '^go_sql_(open_connections|in_use_connections|wait_count_total|wait_duration_seconds_total)' || true
}

run_round() {
  local approach="$1"
  log "vegeta attack · approach=${approach}"
  reset_round
  local url="${ADDR}/debug/orders/${approach}"
  if [ "$approach" = "lua" ]; then
    curl -s -X POST "${ADDR}/debug/deduct/warm/${PRODUCT_ID}" >/dev/null
    echo "已预热单 key：seckill:stock:${PRODUCT_ID} = $($REDIS GET "seckill:stock:${PRODUCT_ID}")"
  else
    echo "已铺桶：$(curl -s -X POST "${ADDR}/debug/bucket/warm/${PRODUCT_ID}?n=${BUCKETS}&k=${PROBES}")"
    url="${url}?n=${BUCKETS}&k=${PROBES}"
  fi
  echo "-- before --"
  outcomes "$approach"
  echo "POST ${url}" \
    | $VEGETA attack -rate="$RATE" -duration="$DURATION" -keepalive=true \
        -header 'Content-Type: application/json' -body "$BODY" \
    | $VEGETA report
  echo "-- after --"
  outcomes "$approach"
  if [ "$approach" = "bucket" ]; then
    echo "-- 各桶剩余（分布均不均匀，直接看这一行）--"
    curl -s "${ADDR}/debug/bucket/remaining/${PRODUCT_ID}?n=${BUCKETS}"
    echo
  fi
  redis_evidence
  echo "-- 恒等式校验 --"
  identity_check
}

for approach in lua bucket; do
  run_round "$approach"
done

cat <<'EOF'

读数方法（这两列横着摆开就是 write-up 里的 before/after 表）：
  · 吞吐 / p50 / p95 / p99  ← 每轮 vegeta report
  · 热点形状               ← commandstats 的 evalsha 调用数与 usec_per_call；
                              分桶轮的调用数会更高（探桶会多打几次 Redis），
                              这是拆桶的直接代价，不许只报好看的那一半
  · key 拓扑               ← KEYS 那一行：1 个 vs N 个。单 key 的硬上限来自
                              「一个 key 只落一个 hash slot / 一个实例」，
                              不是「锁不够细」——这里根本没有锁
  · 分布是否均匀            ← 分桶轮末尾的各桶剩余；差得离谱说明 hash 打散不够
  · 正确性                 ← 每轮末尾恒等式必须 OK，BROKEN 的那轮作废

可选：想直接看热 key，先把 maxmemory-policy 调成 allkeys-lfu，再跑
  docker compose exec -T redis redis-cli --hotkeys
（默认 noeviction 策略下 --hotkeys 会直接报错退出，不是脚本坏了。）

纪律：绝不写死目标 QPS（机器相关），只比同一台机器上这两轮的相对差异，
每个结论都要指向上面某个真实输出（COURSE_SPEC 性能数字纪律）。
EOF
