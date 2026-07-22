package order

import (
	"errors"
	"sync"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// m01 · p3 行锁竞争/死锁：这条测试不调用 PlaceOrderTx，直接用两个反序更新的事务撞出 InnoDB
// 死锁检测——这是 MySQL 的系统性质（COURSE_SPEC 锚点：MySQL 8 文档 InnoDB 锁/事务隔离 · error 1213），
// 不依赖某个业务函数是否已实现，用来在真实数据库里验证"两个事务交叉等待对方持有的行锁会被
// InnoDB 检测并杀掉其中一个、报 1213"这件事本身。SHOW ENGINE INNODB STATUS 里的
// LATEST DETECTED DEADLOCK 段落由 scripts/checks_m01.sh 单独抓取展示（Go test 只断言错误码）。
func TestM01P3ReverseLockOrderCausesDeadlock1213(t *testing.T) {
	db := testutil.OpenTestDB(t)
	const productA, productB = int64(9101), int64(9102)
	testutil.ResetProduct(t, db, productA, 100)
	testutil.ResetProduct(t, db, productB, 100)

	var firstLockAcquired sync.WaitGroup
	firstLockAcquired.Add(2)
	release := make(chan struct{})
	errs := make([]error, 2)

	run := func(idx int, first, second int64) {
		tx, err := db.Beginx()
		if err != nil {
			errs[idx] = err
			firstLockAcquired.Done()
			return
		}
		defer tx.Rollback()

		if _, err := tx.Exec("UPDATE products SET stock = stock - 1 WHERE id = ?", first); err != nil {
			errs[idx] = err
			firstLockAcquired.Done()
			return
		}
		firstLockAcquired.Done()
		<-release // 等两边都拿到各自第一把锁，再同时去抢对方手里的第二把锁——保证真正交叉等待

		_, err = tx.Exec("UPDATE products SET stock = stock - 1 WHERE id = ?", second)
		if err != nil {
			errs[idx] = err
			return
		}
		errs[idx] = tx.Commit()
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); run(0, productA, productB) }()
	go func() { defer wg.Done(); run(1, productB, productA) }()

	firstLockAcquired.Wait()
	close(release)
	wg.Wait()

	deadlockErrs, nilErrs := 0, 0
	for _, err := range errs {
		if err == nil {
			nilErrs++
			continue
		}
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1213 {
			deadlockErrs++
			continue
		}
		t.Fatalf("unexpected error (want either nil or MySQL 1213 deadlock): %v", err)
	}
	if deadlockErrs != 1 || nilErrs != 1 {
		t.Fatalf("expected exactly one deadlock(1213) and one success among the two reverse-order transactions, got deadlockErrs=%d nilErrs=%d (errs=%v)",
			deadlockErrs, nilErrs, errs)
	}
}
