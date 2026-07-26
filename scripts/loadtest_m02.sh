#!/usr/bin/env bash
# m02 p5：商品读路径「走缓存 vs 直连 DB」同 workload 对比 + 命中率。
# 前置：另开一个终端跑 `go run ./cmd/api`（默认 :8080，需要 mysql + redis 都在跑）。
# vegeta 不预装，直接 go run 官方模块（版本同 m01 锁 v12.13.0）。
set -euo pipefail
cd "$(dirname "$0")/.."

ADDR="${ADDR:-http://127.0.0.1:8080}"
RATE="${RATE:-200}"        # 每秒请求数，按机器调整——报告只看同机相对差异，不写死绝对值
DURATION="${DURATION:-10s}"
PRODUCT_ID="${PRODUCT_ID:-9700}"

VEGETA="go run github.com/tsenart/vegeta/v12@v12.13.0"

log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

log "准备商品 ${PRODUCT_ID}（两轮压测读同一行，起点一致）"
docker compose exec -T mysql mysql -uroot -proot seckill -e \
  "INSERT INTO products (id,name,stock) VALUES (${PRODUCT_ID},'loadtest-cache',1000000) ON DUPLICATE KEY UPDATE stock=1000000;"

pool_and_cache_stats() {
  echo "-- DB 池指标 --"
  curl -s "${ADDR}/metrics" | grep '^go_sql_' || true
  echo "-- 缓存回源计数 / 读请求计数 --"
  curl -s "${ADDR}/metrics" | grep -E '^seckill_product_(db_loads|reads_total)' || true
}

run_attack() {
  local path="$1" label="$2"
  log "vegeta attack ${label}：GET ${path}"
  echo "-- before --"
  pool_and_cache_stats
  echo "GET ${ADDR}${path}" | $VEGETA attack -rate="$RATE" -duration="$DURATION" -keepalive=true | $VEGETA report
  echo "-- after --"
  pool_and_cache_stats
}

log "清掉该商品的缓存 key，让第一轮从冷启动开始（第一发是 miss，其余应全部命中）"
docker compose exec -T redis redis-cli DEL "seckill:product:${PRODUCT_ID}" >/dev/null

run_attack "/products/${PRODUCT_ID}" "缓存 ON（走 ProductCache）"
run_attack "/debug/products/${PRODUCT_ID}/nocache" "缓存 OFF（直连 MySQL，SQL 与上面完全相同）"

cat <<'EOF'

读数方法：
  · 命中率 = 1 - (seckill_product_db_loads 增量 / seckill_product_reads_total{path="cached"} 增量)
  · 缓存 ON 那一轮的 db_loads 增量应该≈1（只有第一发 miss；如果明显大于 1，说明 key 被中途挤掉或 TTL 太短）
  · 瓶颈归因必须指名数字：go_sql_ 的 wait_count/in_use、两轮的 p99、db_loads 增量，
    不许写「缓存快一些」这种没有出处的结论（COURSE_SPEC 性能数字纪律）。
把两组 vegeta report + 两组 stats 一起贴进 docs/writeups/m02-cache.md。
EOF
