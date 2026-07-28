#!/usr/bin/env bash
# m04 p5：m03 Lua 同步落库 vs m04 Kafka 异步落库，同机同 workload 相对比较。
set -euo pipefail
cd "$(dirname "$0")/.."

ADDR="${ADDR:-http://127.0.0.1:8080}"
RATE="${RATE:-200}"
DURATION="${DURATION:-10s}"
PRODUCT_ID="${PRODUCT_ID:-10490}"
STOCK="${STOCK:-1000000}"
VEGETA="go run github.com/tsenart/vegeta/v12@v12.13.0"
MYSQL_EXEC_DB="docker compose exec -T mysql mysql -uroot -proot seckill"
REDIS_EXEC="docker compose exec -T redis redis-cli"
log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

BODY="$(mktemp)"
trap 'rm -f "$BODY"' EXIT
printf '{"product_id":%s,"user_id":1,"quantity":1}' "$PRODUCT_ID" > "$BODY"

reset_round() {
  $MYSQL_EXEC_DB -e "DELETE FROM orders WHERE product_id=${PRODUCT_ID}; INSERT INTO products (id,name,stock,version) VALUES (${PRODUCT_ID},'m04-load',${STOCK},0) ON DUPLICATE KEY UPDATE stock=${STOCK},version=0;" >/dev/null
  $REDIS_EXEC DEL "seckill:stock:${PRODUCT_ID}" >/dev/null
  curl -fsS -X POST "${ADDR}/debug/deduct/warm/${PRODUCT_ID}" >/dev/null
}

identity() {
  $MYSQL_EXEC_DB -N -e "SELECT CONCAT('DB剩余=',stock,' DB已扣=',${STOCK}-stock,' 订单=',(SELECT COUNT(*) FROM orders WHERE product_id=${PRODUCT_ID}),' 恒等式=',IF(${STOCK}-stock=(SELECT COUNT(*) FROM orders WHERE product_id=${PRODUCT_ID}),'OK','BROKEN')) FROM products WHERE id=${PRODUCT_ID};"
}

run_round() {
  local name="$1" endpoint="$2"
  log "$name"
  reset_round
  local result attack_pid started orders lag
  result="$(mktemp)"
  started="$SECONDS"
  echo "POST ${ADDR}${endpoint}" | $VEGETA attack -rate="$RATE" -duration="$DURATION" -keepalive=true -header 'Content-Type: application/json' -body "$BODY" > "$result" &
  attack_pid=$!
  echo "-- 每秒采样 DB 累计订单 / Kafka lag（相邻 DB 数差值就是该秒写入量）--"
  while kill -0 "$attack_pid" 2>/dev/null; do
    orders="$($MYSQL_EXEC_DB -N -e "SELECT COUNT(*) FROM orders WHERE product_id=${PRODUCT_ID};")"
    lag="$(curl -fsS "${ADDR}/metrics" | awk '/^seckill_kafka_consumer_group_lag/{print $2; exit}')"
    printf 't=%ss db_orders=%s lag=%s\n' "$((SECONDS - started))" "$orders" "${lag:-unknown}"
    sleep 1
  done
  wait "$attack_pid"
  $VEGETA report < "$result"
  rm -f "$result"
  if [ "$name" = "async" ]; then
    deadline=$((SECONDS + 30))
    while [ "$SECONDS" -lt "$deadline" ]; do
      lag="$(curl -fsS "${ADDR}/metrics" | awk '/^seckill_kafka_consumer_group_lag/{print $2; exit}')"
      orders="$($MYSQL_EXEC_DB -N -e "SELECT COUNT(*) FROM orders WHERE product_id=${PRODUCT_ID};")"
      printf 'drain t=%ss db_orders=%s lag=%s\n' "$((SECONDS - started))" "$orders" "${lag:-unknown}"
      [ "${lag:-1}" = "0" ] && break
      sleep 1
    done
    echo "异步排空后 lag=${lag:-unknown}，排空等待秒数由上面的轮询实测"
  fi
  identity
}

GOCACHE="${GOCACHE:-/tmp/go-seckill-build}" go run ./cmd/kafka-bootstrap
run_round sync /debug/orders/lua
run_round async /debug/orders/async

cat <<'EOF'

报告只比较同机相对数字：API throughput/p99、DB 订单写入速率、lag 峰值和排空时间。
同步接口返回时恒等式应已闭合；异步接口返回 202 时允许暂时不闭合，必须等 lag=0 后再判最终恒等式。
EOF
