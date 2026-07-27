package deduct

import (
	"context"
	"errors"
	"testing"
	"time"
)

// m03 · p3 分布式锁的失效模式：TTL 是崩溃兜底，但它同时也是一把悬顶之剑——
// 临界区跑得比 TTL 久，锁就在你手里凭空消失了。

// TestM03P3ExpiredLockLetsTwoHoldersOverlap 复现的是 p2 那把锁的固有缺陷，
// 所以它在 p2 实现完就会绿——这条测试的价值是"看见事故"，不是"卡住 p3"。
// p3 真正的闸门是下面那条看门狗测试。
func TestM03P3ExpiredLockLetsTwoHoldersOverlap(t *testing.T) {
	defer guard(t)
	const productID = int64(9806)
	_, rdb, _ := newTestDeps(t, productID, 10)
	ctx := context.Background()
	key := LockKey(productID)

	const ttl = 300 * time.Millisecond
	const criticalSection = 700 * time.Millisecond

	holderA, err := Acquire(ctx, rdb, key, ttl)
	if err != nil {
		t.Fatalf("A 抢锁应该成功：%v", err)
	}

	// A 的临界区比 TTL 长——它自己毫不知情，还在慢慢干活。
	time.Sleep(criticalSection)

	// 此刻 A 的锁早就过期自动消失了，B 一抢就中。
	holderB, err := Acquire(ctx, rdb, key, 5*time.Second)
	if err != nil {
		t.Fatalf("锁过期后 B 应该能抢到（这正是事故）：%v", err)
	}

	t.Logf("临界区重叠复现：TTL=%v 但临界区要跑 %v；A(token=%s) 还在临界区里，"+
		"B(token=%s) 已经拿着同一把锁进来了——此刻两个持有者同时认为自己独占",
		ttl, criticalSection, holderA.Token(), holderB.Token())

	// A 干完活来释放，才发现锁早就不是自己的了。
	if err := holderA.Release(ctx); !errors.Is(err, ErrLockLost) {
		t.Fatalf("A 的锁已经过期并被 B 抢走，Release 应该返回 ErrLockLost，实际：%v", err)
	}
	t.Logf("A 释放时拿到 ErrLockLost —— 这是它唯一一次得知" +
		"「刚才那段临界区其实没有被保护」，但为时已晚：库存已经被两个人改过了")
}

func TestM03P3WatchdogKeepsLockAliveDuringLongCriticalSection(t *testing.T) {
	defer guard(t)
	const productID = int64(9807)
	_, rdb, _ := newTestDeps(t, productID, 10)
	ctx := context.Background()
	key := LockKey(productID)

	const ttl = 300 * time.Millisecond
	const interval = 100 * time.Millisecond
	const criticalSection = 900 * time.Millisecond

	holderA, err := Acquire(ctx, rdb, key, ttl)
	if err != nil {
		t.Fatalf("A 抢锁应该成功：%v", err)
	}
	stop := holderA.StartWatchdog(interval)

	// 同一段跨过 3 个 TTL 周期的临界区，这次有看门狗在后台把 TTL 顶回去。
	deadline := time.Now().Add(criticalSection)
	attempts, stolen := 0, 0
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		attempts++
		if _, err := Acquire(ctx, rdb, key, time.Second); err == nil {
			stolen++
		} else if !errors.Is(err, ErrLockNotAcquired) {
			t.Fatalf("B 抢锁返回了意外错误：%v", err)
		}
	}
	if stolen != 0 {
		t.Fatalf("看门狗没顶住：临界区期间 B 抢到了 %d 次锁（%d 次尝试）", stolen, attempts)
	}

	stop()
	if err := holderA.Release(ctx); err != nil {
		t.Fatalf("看门狗守到最后，A 应该能正常释放自己的锁，实际：%v", err)
	}

	// 停掉看门狗并释放之后，锁位必须是空的（别人可以正常抢）。
	if _, err := Acquire(ctx, rdb, key, time.Second); err != nil {
		t.Fatalf("释放之后 B 应该能抢到锁：%v", err)
	}

	t.Logf("续期看门狗：TTL=%v、心跳=%v、临界区 %v（跨 3 个 TTL 周期）；"+
		"期间 B 尝试抢锁 %d 次全部被拒，A 最后正常释放 token=%s",
		ttl, interval, criticalSection, attempts, holderA.Token())
}
