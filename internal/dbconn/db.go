// 已就位（AI 生成）：DSN 拼接与连接建立是样板，不是本课教学点（教学点在 server.NewProductionServer
// 怎么配置这个连接池，不在"怎么连上 MySQL"）。
package dbconn

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// Config 是连接 MySQL 需要的最小字段集合，来自环境变量。
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// DSN 拼出 go-sql-driver/mysql 认识的连接串；parseTime=true 让 DATETIME 列直接解析成 time.Time，
// multiStatements 不开——迁移脚本用单条语句执行，避免一次网络往返里混进多条 SQL 的隐患。
func (c Config) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC",
		c.User, c.Password, c.Host, c.Port, c.DBName)
}

// Open 用 sqlx.Connect 建连接并立刻 Ping 一次，保证返回的 *sqlx.DB 是真的能用的，
// 而不是"字符串格式对但服务器连不上"这种要等到第一条业务 SQL 才会暴露的坑。
func Open(cfg Config) (*sqlx.DB, error) {
	db, err := sqlx.Connect("mysql", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("dbconn: connect: %w", err)
	}
	return db, nil
}
