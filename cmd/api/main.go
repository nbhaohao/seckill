// 已就位（AI 生成）：HTTP 胶水/环境变量解析——把各 internal 包拼起来，本身不是教学点。
package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nbhaohao/go-seckill/internal/dbconn"
	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/metrics"
	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/server"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

type placeOrderBody struct {
	RequestID string `json:"request_id" binding:"required"`
	ProductID int64  `json:"product_id" binding:"required"`
	UserID    int64  `json:"user_id" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required"`
}

func main() {
	dbCfg := dbconn.Config{
		Host:     envOr("DB_HOST", "127.0.0.1"),
		Port:     envIntOr("DB_PORT", 3306),
		User:     envOr("DB_USER", "seckill"),
		Password: envOr("DB_PASSWORD", "seckill"),
		DBName:   envOr("DB_NAME", "seckill"),
	}
	db, err := dbconn.Open(dbCfg)
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}

	node, err := idgen.NewNode(int64(envIntOr("NODE_ID", 1)))
	if err != nil {
		log.Fatalf("idgen.NewNode: %v", err)
	}

	if err := metrics.Register(prometheus.DefaultRegisterer, db.DB); err != nil {
		log.Fatalf("register metrics: %v", err)
	}

	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// /debug/products/:id 是给 scripts/checks_m01.sh 做恒等式校验用的最小读接口，不是业务 API。
	r.GET("/debug/products/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		var stock int
		if err := db.GetContext(c.Request.Context(), &stock, "SELECT stock FROM products WHERE id = ?", id); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"id": id, "stock": stock})
	})

	// /debug/slow 用来演示 p4 的 context deadline：故意跑一条慢查询，客户端超时取消后
	// 连接应该最终归还连接池（DB.Stats().InUse 会掉回去），而不是被这条慢查询永久占住。
	r.GET("/debug/slow", func(c *gin.Context) {
		seconds := 0
		if v := c.Query("seconds"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				seconds = n
			}
		}
		var out int
		err := db.GetContext(c.Request.Context(), &out, "SELECT SLEEP(?)", seconds)
		if err != nil {
			c.JSON(http.StatusGatewayTimeout, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"slept": seconds})
	})

	r.POST("/orders", func(c *gin.Context) {
		var body placeOrderBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		start := time.Now()
		o, err := order.PlaceOrderTx(c.Request.Context(), db, node, order.PlaceOrderRequest{
			RequestID: body.RequestID,
			ProductID: body.ProductID,
			UserID:    body.UserID,
			Quantity:  body.Quantity,
		})
		switch {
		case err == nil:
			metrics.OrdersPlaced.WithLabelValues("success").Inc()
			metrics.OrderLatency.WithLabelValues("success").Observe(time.Since(start).Seconds())
			c.JSON(http.StatusOK, o)
		case errors.Is(err, order.ErrInsufficientStock):
			metrics.OrdersPlaced.WithLabelValues("insufficient_stock").Inc()
			metrics.OrderLatency.WithLabelValues("insufficient_stock").Observe(time.Since(start).Seconds())
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		case errors.Is(err, order.ErrInvalidQuantity), errors.Is(err, order.ErrProductNotFound):
			metrics.OrdersPlaced.WithLabelValues("error").Inc()
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		default:
			metrics.OrdersPlaced.WithLabelValues("error").Inc()
			metrics.OrderLatency.WithLabelValues("error").Observe(time.Since(start).Seconds())
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	cfg := server.DefaultServerConfig()
	srv := server.NewProductionServer(envOr("HTTP_ADDR", ":8080"), r, db.DB, cfg)

	log.Printf("go-seckill listening on %s", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}
}
