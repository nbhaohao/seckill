package deduct

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/order"
)

// ── m03 p2 · Redis 分布式锁（正确性） / p3 · 它的失效模式 ────────────────────
//
// 第二个方案不改 SQL，改的是"谁能进临界区"：抢到锁的那个进程才允许执行
// 「读库存 → 判断够不够 → 扣减」这三步，别人等着。锁本身不保证这三步原子，
// 它保证的是同一时刻只有一个人在做这三步——这个区别在 p3 会被打穿。

// LockOptions 是持锁的两个参数。
type LockOptions struct {
	// TTL：锁的过期时间。必须有——持锁进程崩了、网络断了，没有 TTL 这把锁就永远锁死。
	// 但 TTL 一旦短于临界区耗时，就会发生 p3 要复现的那件事。
	TTL time.Duration
	// Watchdog（p3）：是否为这把锁开续期看门狗。p2 阶段固定为 false。
	Watchdog bool
	// WatchdogInterval（p3）：续期心跳间隔。
	WatchdogInterval time.Duration
}

// DefaultLockOptions 已就位（AI 生成）：数值不是教学点（教学点是"TTL 和临界区耗时的关系"），
// 给一组便于观察的值。
func DefaultLockOptions() LockOptions {
	return LockOptions{
		TTL:              3 * time.Second,
		Watchdog:         false,
		WatchdogInterval: time.Second,
	}
}

// Lock 是一次持锁的凭据。token 是它的核心：同一个 key 会被无数个进程反复抢到，
// 只有 token 能回答"现在 Redis 里躺着的这把锁，是不是我这一次抢到的那把"。
type Lock struct {
	rdb   *redis.Client
	key   string
	token string
	ttl   time.Duration
}

// Key / Token 已就位（AI 生成）：只读访问器，测试要拿 token 打印证据。
func (l *Lock) Key() string   { return l.key }
func (l *Lock) Token() string { return l.token }

// newToken 已就位（AI 生成）：生成一个不会重复的持锁凭据。
// 用 crypto/rand 而不是时间戳/自增：两个进程在同一毫秒抢锁是秒杀里的常态，
// 凭据撞车会让"判断这把锁是不是我的"直接失效。
func newToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// releaseScript 是 p2 S2 要写的 Lua：比对 token，只有确认这把锁是自己的才删。
//
// 为什么必须是 Lua 而不是"先 GET 比一比、相等再 DEL"：GET 和 DEL 是两次往返，
// 中间隔着一个窗口——这个窗口里锁可能过期、被别人抢走，然后你把别人的锁删了。
// Redis 执行一个脚本期间不会插进别的命令，所以"比对 + 删除"必须整个塞进脚本里。
//
// KEYS[1] = 锁的 key，ARGV[1] = 自己的 token；返回 1 表示删掉了、0 表示这把锁不是自己的。
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`)

// renewScript 是 p3 要写的 Lua：比对 token，确认锁还是自己的才续期。
// 和 releaseScript 是同一个道理——"确认所有权"和"动这把锁"之间不能有窗口。
var renewScript = redis.NewScript(`
-- TODO: phase p3 — AI 将在 p3 学习时实现「比对 token 再续期」
return redis.error_reply("TODO: phase p3 renewScript not implemented")
`)

// Acquire 是 p2 S1：抢锁。
//
// 要实现的形状：生成一个唯一 token（newToken 已就位），用 SET NX PX 一次性写进去——
// NX 保证"只有 key 不存在时才写成功"（这就是互斥），PX 给它一个过期时间（这是崩溃兜底）。
// 抢到返回 *Lock，没抢到返回 ErrLockNotAcquired。
//
// 两个必须一次做完的原因在 review 时会被追问：如果先 SETNX 再单独 EXPIRE，
// 中间崩一下会留下什么状态。
func Acquire(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (*Lock, error) {
	token, err := newToken()
	if err != nil {
		return nil, err
	}
	ok, err := rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLockNotAcquired
	}
	return &Lock{rdb: rdb, key: key, token: token, ttl: ttl}, nil
}

// Release 是 p2 S2：释放自己的那把锁。
//
// 要实现的形状：跑上面那段 releaseScript（用 releaseScript.Run(ctx, l.rdb, []string{l.key}, l.token)），
// 返回 1 表示删成功；返回 0 表示 Redis 里那把锁已经不是自己的了——这不是"释放失败"，
// 是"我的锁早就没了"，应当返回 ErrLockLost 让调用方知道临界区可能已经被别人闯入过。
func (l *Lock) Release(ctx context.Context) error {
	n, err := releaseScript.Run(ctx, l.rdb, []string{l.key}, l.token).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLockLost
	}
	return nil
}

// StartWatchdog 是 p3：给这把锁开一个续期心跳，让长临界区不至于中途丢锁。
//
// 要实现的形状：起一个后台 goroutine，每隔 interval 跑一次 renewScript 把 TTL 顶回去；
// 返回一个 stop 函数，调用方用完锁必须调它把 goroutine 收掉（否则就是 goroutine 泄漏）。
// 续期发现锁已经不是自己的（脚本返回 0）时，看门狗应该停下——继续顶一把别人的锁没有意义。
func (l *Lock) StartWatchdog(interval time.Duration) (stop func()) {
	panic("TODO: phase p3 — AI 将在 p3 学习时实现续期看门狗")
}

// PlaceOrderWithLock 是 p2 S3：用分布式锁保护「读库存 → 判断 → 扣减 → 插订单」。
//
// 要实现的形状：
//  1. Acquire(LockKey(req.ProductID))，没抢到直接返回 ErrLockNotAcquired（秒杀里不排队，快速失败）
//  2. defer Release
//  3. 锁内开事务，做那三步——注意这里的 SELECT 故意不加 FOR UPDATE：
//     本方案的互斥完全由 Redis 那把锁提供，行锁是 m01 悲观锁方案的东西，不要混用
//  4. 提交事务
//
// opts.Watchdog 在 p2 阶段固定是 false，p3 会把看门狗接进来。
func PlaceOrderWithLock(ctx context.Context, rdb *redis.Client, db *sqlx.DB, node *idgen.Node, req order.PlaceOrderRequest, opts LockOptions) (*order.Order, error) {
	panic("TODO: phase p2 — AI 将在 p2 学习时分切片实现锁内扣减")
}
