package metrics

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

// 已就位（AI 生成）：metrics 是样板代码，这条测试只保证 Register 接线不出错，不是任何 stage 的教学点。
func TestRegisterDoesNotError(t *testing.T) {
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/db")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	reg := prometheus.NewRegistry()
	if err := Register(reg, db); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("Gather: %v", err)
	}
}
