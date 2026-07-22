package server

import (
	"context"
	"database/sql"
	"net/http"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/nbhaohao/go-seckill/internal/testutil"
)

// m01 · p4 server timeout + ctx + DB 池 + keep-alive

func TestM01P4NewProductionServerSetsNonZeroTimeouts(t *testing.T) {
	// sql.Open 不建立真实连接（lazy），测四个 timeout 字段和池参数不需要真的连上 MySQL。
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/db")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	cfg := DefaultServerConfig()
	srv := NewProductionServer(":0", http.NewServeMux(), db, cfg)

	if srv.ReadHeaderTimeout <= 0 {
		t.Error("ReadHeaderTimeout must be > 0")
	}
	if srv.ReadTimeout <= 0 {
		t.Error("ReadTimeout must be > 0")
	}
	if srv.WriteTimeout <= 0 {
		t.Error("WriteTimeout must be > 0")
	}
	if srv.IdleTimeout <= 0 {
		t.Error("IdleTimeout must be > 0")
	}

	stats := db.Stats()
	if stats.MaxOpenConnections != cfg.MaxOpenConns {
		t.Errorf("MaxOpenConnections = %d, want %d (SetMaxOpenConns not applied?)", stats.MaxOpenConnections, cfg.MaxOpenConns)
	}
}

func TestM01P4PoolLimitProducesWaitCount(t *testing.T) {
	sdb := testutil.OpenTestDB(t)
	cfg := DefaultServerConfig()
	cfg.MaxOpenConns = 1 // 故意压到 1，逼并发查询排队等连接
	cfg.MaxIdleConns = 1
	srv := NewProductionServer(":0", http.NewServeMux(), sdb.DB, cfg)
	_ = srv

	const concurrency = 8
	done := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			var out int
			_ = sdb.QueryRow("SELECT SLEEP(0.2)").Scan(&out)
		}()
	}
	for i := 0; i < concurrency; i++ {
		<-done
	}

	stats := sdb.DB.Stats()
	if stats.WaitCount == 0 {
		t.Errorf("expected WaitCount > 0 with MaxOpenConns=1 under concurrency=%d, got WaitCount=%d (stats=%+v)",
			concurrency, stats.WaitCount, stats)
	}
}

func TestM01P4ContextDeadlineReturnsConnectionToPool(t *testing.T) {
	sdb := testutil.OpenTestDB(t)
	cfg := DefaultServerConfig()
	srv := NewProductionServer(":0", http.NewServeMux(), sdb.DB, cfg)
	_ = srv

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var out int
	err := sdb.QueryRowContext(ctx, "SELECT SLEEP(2)").Scan(&out)
	if err == nil {
		t.Fatal("expected context deadline error, query returned success")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sdb.DB.Stats().InUse == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("connection was not returned to pool within 5s after context deadline: InUse=%d", sdb.DB.Stats().InUse)
}
