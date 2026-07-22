package server

import (
	"database/sql"
	"net/http"
	"time"
)

// ServerConfig 把 http.Server 的四个超时字段和 database/sql 连接池的三个参数放进同一份配置里——
// m01 p4 的教学点就是这七个数字：Go 的零值默认是"无限等"，生产环境必须显式配置，否则一条慢查询
// 或一个挂起的客户端连接就能悄悄耗尽资源。
type ServerConfig struct {
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
}

// DefaultServerConfig 已就位（AI 生成）：具体数值不是本关教学重点（教学重点是"必须显式配置"这件事
// 本身），给一组来自 net/http、database/sql 官方文档常见推荐区间的保守默认值。
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxOpenConns:      20,
		MaxIdleConns:      20,
		ConnMaxLifetime:   5 * time.Minute,
	}
}

// NewProductionServer 是 m01 p4 的核心：接手一个已经 Open 好的 *sql.DB，把连接池的三个参数
// 显式配置上去，再拼出一个把四个超时都显式设置了的 *http.Server——两件事一次做完，因为它们
// 服务同一个目的：把"默认值等于无限"这件事从整条请求链路上彻底移除。
func NewProductionServer(addr string, handler http.Handler, db *sql.DB, cfg ServerConfig) *http.Server {
	panic("TODO: phase p4 - 配置 DB 连接池三参数 + 拼出四超时都非零的 http.Server")
}

// AI 将在 p4 学习时分切片实现（下面是思路与顺序，不是终稿代码）：
// 1. db.SetMaxOpenConns(cfg.MaxOpenConns)
//    —— 上限：同一时刻最多多少个连接在跟 MySQL 说话，多出来的请求排队等（体现在 DB.Stats().WaitCount）
// 2. db.SetMaxIdleConns(cfg.MaxIdleConns)
//    —— 空闲态最多保留多少个连接不关掉，避免流量抖动时刚放回池子又要重新握手
// 3. db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
//    —— 连接活满这么久强制重建，防止连接老死在 MySQL 那一侧的某个中间设备/防火墙的空闲超时策略上
// 4. return &http.Server{
//      Addr:              addr,
//      Handler:           handler,
//      ReadHeaderTimeout: cfg.ReadHeaderTimeout,  // 只读请求头的时限——防"只发头不发身体"的慢速攻击
//      ReadTimeout:       cfg.ReadTimeout,        // 读完整个请求（含 body）的时限
//      WriteTimeout:      cfg.WriteTimeout,       // 写响应的时限
//      IdleTimeout:       cfg.IdleTimeout,        // keep-alive 空闲连接最多占着不用多久，超时被回收
//    }
