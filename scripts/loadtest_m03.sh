#!/usr/bin/env bash
# m03 p5：四条防超卖路径「同 workload」对比 —— 本课最值钱的那张表。
#   pessimistic  = m01 的 SELECT ... FOR UPDATE 悲观锁（基线列）
#   cas          = p1 版本号 CAS 乐观锁 + 有界重试
#   conditional  = p1 条件更新单 SQL
#   lock         = p2/p3 Redis 分布式锁
#   lua          = p4 Redis + Lua 原子预扣
# 四轮做的是同一件业务（下一单），唯一差别是"扣库存"那一步的机制，对比才只剩一个变量。
#
# 前置：另开一个终端跑 `go run ./cmd/api`（默认 :8080，需要 mysql + redis 都在跑）。
set -euo pipefail
cd "$(dirname "$0")/.."

ADDR="${ADDR:-http://127.0.0.1:8080}"
RATE="${RATE:-200}"        # 每秒请求数，按机器调整——报告只看同机相对差异，不写死绝对值
DURATION="${DURATION:-10s}"
PRODUCT_ID="${PRODUCT_ID:-9900}"
# 库存给足，让每一轮都在"抢得到"的状态下比吞吐；真正的卖光行为由单元测试断言。
STOCK="${STOCK:-1000000}"

VEGETA="go run github.com/tsenart/vegeta/v12@v12.13.0"
MYSQL_EXEC_DB="docker compose exec -T mysql mysql -uroot -proot seckill"

log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

BODY="$(mktemp)"
trap 'rm -f "$BODY"' EXIT
printf '{"product_id":%s,"user_id":1,"quantity":1}' "$PRODUCT_ID" > "$BODY"

reset_round() {
  # 每轮从同一个起点开跑：库存复位、version 归零、清掉上一轮的订单，
  # 并把 Redis 侧的锁/预扣 key 也清干净（lua 那轮随后单独预热）。
  $MYSQL_EXEC_DB -e "
    DELETE FROM orders WHERE product_id = ${PRODUCT_ID};
    INSERT INTO products (id, name, stock, version) VALUES (${PRODUCT_ID}, 'loadtest-deduct', ${STOCK}, 0)
      ON DUPLICATE KEY UPDATE stock = ${STOCK}, version = 0;" >/dev/null
  docker compose exec -T redis redis-cli DEL "seckill:lock:product:${PRODUCT_ID}" "seckill:stock:${PRODUCT_ID}" >/dev/null
}

identity_check() {
  # 恒等式：初始库存 − 剩余库存 ≡ 本轮订单数。任何一轮破了，那一轮的数字就不许进报告。
  $MYSQL_EXEC_DB -N -e "
    SELECT CONCAT('剩余库存=', p.stock,
                  ' 已扣=', ${STOCK} - p.stock,
                  ' 订单数=', (SELECT COUNT(*) FROM orders o WHERE o.product_id = p.id),
                  ' 恒等式=', IF(${STOCK} - p.stock = (SELECT COUNT(*) FROM orders o WHERE o.product_id = p.id), 'OK', 'BROKEN'))
    FROM products p WHERE p.id = ${PRODUCT_ID};"
}

outcomes() {
  echo "-- 本轮结果分类（success / insufficient / conflict / error）--"
  curl -s "${ADDR}/metrics" | grep "^seckill_deduct_outcomes_total{approach=\"$1\"" || echo "(无)"
  if [ "$1" = "cas" ]; then
    echo "-- CAS 累计尝试次数（除以本轮请求数 = 平均每单重试几次）--"
    curl -s "${ADDR}/metrics" | grep '^seckill_cas_attempts_total' || true
  fi
  echo "-- DB 池指标 --"
  curl -s "${ADDR}/metrics" | grep -E '^go_sql_(open_connections|in_use_connections|wait_count_total|wait_duration_seconds_total)' || true
}

run_round() {
  local approach="$1"
  log "vegeta attack · approach=${approach}"
  reset_round
  if [ "$approach" = "lua" ]; then
    # Lua 预扣的前提：把 DB 库存灌进 Redis（seckill:stock:<id>）。
    curl -s -X POST "${ADDR}/debug/deduct/warm/${PRODUCT_ID}" >/dev/null
    echo "已预热 Redis 库存：$(docker compose exec -T redis redis-cli GET "seckill:stock:${PRODUCT_ID}")"
  fi
  echo "-- before --"
  outcomes "$approach"
  echo "POST ${ADDR}/debug/orders/${approach}" \
    | $VEGETA attack -rate="$RATE" -duration="$DURATION" -keepalive=true \
        -header 'Content-Type: application/json' -body "$BODY" \
    | $VEGETA report
  echo "-- after --"
  outcomes "$approach"
  echo "-- 恒等式校验 --"
  identity_check
}

for approach in pessimistic cas conditional lock lua; do
  run_round "$approach"
done

cat <<'EOF'

读数方法（把这几列横着摆开就是 write-up 里那张表）：
  · 吞吐 / p99             ← 每轮 vegeta report 的 throughput 与 Latencies p99
  · 成功 vs 冲突 vs 售罄   ← seckill_deduct_outcomes_total 按 result 分类；
                             conflict 一列是 cas 的"重试耗尽"和 lock 的"没抢到锁"，
                             它和 insufficient（真卖光）是两件事，别合并
  · 乐观锁的代价           ← seckill_cas_attempts_total ÷ cas 轮的请求数 = 平均每单尝试次数
  · DB 压力               ← go_sql_ 的 in_use / wait_count / wait_duration；
                             lua 那轮应该明显低于前三轮（热路径不打 DB）
  · 正确性                ← 每轮末尾的恒等式必须是 OK，BROKEN 的那轮数字作废

纪律：绝不写死目标 QPS（机器相关），报告只比同一台机器上四轮的相对差异，
每个结论都要指向上面某个真实输出（COURSE_SPEC 性能数字纪律）。
EOF
