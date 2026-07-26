#!/usr/bin/env bash
# m02 一键验证：起 MySQL+Redis → 跑 m02 全部红测试(-race) → 打印 Redis 侧的直接证据
# （缓存里的原始值、TTL 分布、空值缓存占位符）。
# 用法：./scripts/checks_m02.sh
set -euo pipefail
cd "$(dirname "$0")/.."

REDIS_EXEC="docker compose exec -T redis redis-cli"

log() { printf '\n\033[1;34m== %s ==\033[0m\n' "$1"; }

log "1/5 docker compose up -d mysql redis"
docker compose up -d mysql redis

log "2/5 等 mysql + redis healthy"
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

log "3/5 go test -race（m02 全部 phase；-v 输出里每条测试都会打印它看到的真实观察值）"
export TEST_DB_HOST=127.0.0.1 TEST_DB_PORT=3306 TEST_DB_USER=seckill TEST_DB_PASSWORD=seckill TEST_DB_NAME=seckill
export TEST_REDIS_ADDR=127.0.0.1:6379
go build ./...
go test -race ./internal/cache/... -run '^TestM02' -v -count=1

log "4/5 Redis 侧证据：测试刚写进去的 key 长什么样"
echo '-- 商品缓存（p1 回填的 JSON）--'
$REDIS_EXEC GET seckill:product:9601 || true
echo '-- 该 key 的剩余 TTL（秒）--'
$REDIS_EXEC TTL seckill:product:9601 || true
echo '-- 空值缓存（p2 给不存在的 id 留下的占位符 + 它的短 TTL）--'
$REDIS_EXEC GET seckill:product:9610 || true
$REDIS_EXEC TTL seckill:product:9610 || true
echo '-- p2 预热的 12 个 key 的毫秒级 TTL（抖动把过期时刻拉开了多少）--'
for id in $(seq 9620 9631); do
  printf 'seckill:product:%s ' "$id"
  $REDIS_EXEC PTTL "seckill:product:$id" || true
done

log "5/5 恒等式仍然成立（m02 没碰下单路径，m01 的正确性不能退化）"
go test -race ./... -run '^TestM01' -count=1

cat <<'EOF'

全部通过。接下来（不进本脚本，手动跑）：
  1. 起 API: go run ./cmd/api        （另开一个终端；m02 起需要 redis 在跑）
  2. 缓存 on/off 压测对比：./scripts/loadtest_m02.sh
  3. 把 -v 输出里的四类证据（回源计数 / 空值缓存 / singleflight 合并 / 竞态旧值）
     和压测数字一起填进 docs/writeups/m02-cache.md
EOF
