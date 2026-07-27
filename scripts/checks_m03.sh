#!/usr/bin/env bash
# m03 一键验证：起 MySQL+Redis → 应用 0003 迁移 → 跑 m03 全部红测试(-race，-v 里每条都打印
# 它看到的真实观察值) → 打印 Redis 侧的直接证据（锁 key / 预扣库存 key）→ m01+m02 回归。
# 用法：./scripts/checks_m03.sh
set -euo pipefail
cd "$(dirname "$0")/.."

MYSQL_EXEC_DB="docker compose exec -T mysql mysql -uroot -proot seckill"
REDIS_EXEC="docker compose exec -T redis redis-cli"

log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

log "1/6 docker compose up -d mysql redis"
docker compose up -d mysql redis

log "2/6 等 mysql + redis healthy"
for svc in mysql redis; do
  for i in $(seq 1 40); do
    status="$(docker compose ps "$svc" --format json 2>/dev/null | grep -o '"Health":"[a-z]*"' || true)"
    if echo "$status" | grep -q healthy; then
      echo "$svc healthy"
      break
    fi
    sleep 2
    if [ "$i" -eq 40 ]; then
      echo "$svc 一直没 healthy，退出" >&2
      docker compose logs "$svc" | tail -30
      exit 1
    fi
  done
done

log "3/6 应用 migrations/0003_products_version.sql（乐观锁的 version 列；脚本可重复跑）"
$MYSQL_EXEC_DB < migrations/0003_products_version.sql
$MYSQL_EXEC_DB -e "SHOW COLUMNS FROM products LIKE 'version';"

log "4/6 go test -race（m03 全部 phase）"
export TEST_DB_HOST=127.0.0.1 TEST_DB_PORT=3306 TEST_DB_USER=seckill TEST_DB_PASSWORD=seckill TEST_DB_NAME=seckill
export TEST_REDIS_ADDR=127.0.0.1:6379
go build ./...
go test -race ./internal/deduct/... -run '^TestM03' -v -count=1

log "5/6 Redis 侧证据：测试跑完后锁 key 与预扣库存 key 的状态"
echo '-- p2/p3 的锁 key（测试都正常释放/过期了的话，这里应该全是 empty）--'
for id in 9803 9804 9805 9806 9807; do
  printf 'seckill:lock:product:%s -> ' "$id"
  $REDIS_EXEC GET "seckill:lock:product:$id" || true
done
echo '-- p4 预扣库存 key（卖光那两条应该停在 0，绝不为负）--'
for id in 9808 9809 9810; do
  printf 'seckill:stock:%s -> ' "$id"
  $REDIS_EXEC GET "seckill:stock:$id" || true
done
echo '-- 四方案跑完后 DB 侧的恒等式素材（stock 与订单数）--'
$MYSQL_EXEC_DB -e "SELECT p.id, p.stock, p.version, (SELECT COUNT(*) FROM orders o WHERE o.product_id = p.id) AS orders
                   FROM products p WHERE p.id IN (9801,9802,9805,9808,9809,9810) ORDER BY p.id;"

log "6/6 回归：m01 恒等式 + m02 缓存三连仍然全绿（m03 新增的是并行方案，不能碰坏既有路径）"
go test -race ./... -run '^TestM0[12]' -count=1

cat <<'EOF'

全部通过。接下来（不进本脚本，手动跑）：
  1. 起 API: go run ./cmd/api        （另开一个终端；需要 mysql + redis 都在跑）
  2. 四方案同 workload 压测：./scripts/loadtest_m03.sh
  3. 把 -v 输出里的四类证据（CAS 冲突/重试计数 · 误删 token · 临界区重叠双方 token ·
     Lua 预扣后 Redis 库存不为负）和压测对比表一起填进 docs/writeups/m03-oversell.md
EOF
