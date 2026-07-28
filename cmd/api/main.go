// 已就位（AI 生成）：HTTP 胶水/环境变量解析——把各 internal 包拼起来，本身不是教学点。
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nbhaohao/go-seckill/internal/cache"
	"github.com/nbhaohao/go-seckill/internal/dbconn"
	"github.com/nbhaohao/go-seckill/internal/deduct"
	"github.com/nbhaohao/go-seckill/internal/idgen"
	"github.com/nbhaohao/go-seckill/internal/metrics"
	"github.com/nbhaohao/go-seckill/internal/mq"
	"github.com/nbhaohao/go-seckill/internal/order"
	"github.com/nbhaohao/go-seckill/internal/redisconn"
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

// deductOrderBody 是 m03 压测端点的入参：只有 product_id 必填。
// request_id 留空表示"让服务端生成一个新的"，quantity 留空按 1 算——
// 压测器重放同一份 body 时才不会退化成幂等重放。
type deductOrderBody struct {
	RequestID string `json:"request_id"`
	ProductID int64  `json:"product_id" binding:"required"`
	UserID    int64  `json:"user_id"`
	Quantity  int    `json:"quantity"`
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

	// m02 缓存层接线：Redis 客户端 + 回源计数器 + ProductCache。
	// 连不上 Redis 就直接退出——m02 起，商品读路径依赖它（m01 的下单接口不受影响，但服务启动是同一个进程）。
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	rdb, err := redisconn.Open(rctx, redisconn.Config{
		Addr:     envOr("REDIS_ADDR", "127.0.0.1:6379"),
		Password: envOr("REDIS_PASSWORD", ""),
		DB:       envIntOr("REDIS_DB", 0),
	})
	rcancel()
	if err != nil {
		log.Fatalf("connect redis: %v（先 docker compose up -d redis）", err)
	}
	defer func() { _ = rdb.Close() }()

	// m04 Kafka 胶水（已就位，AI 生成）：核心机制都留在 internal/mq 的 phase TODO。
	// API 只负责把最终形态接起来：202 接单、manual commit、有限重试与 lag gauge。
	brokers := strings.Split(envOr("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	orderTopic := envOr("KAFKA_TOPIC", mq.OrderCreatedTopic)
	dltTopic := envOr("KAFKA_DLT_TOPIC", mq.OrderCreatedDLT)
	consumerGroup := envOr("KAFKA_GROUP", "seckill-order-writer")
	producer, err := mq.NewProducer(brokers...)
	if err != nil {
		log.Fatalf("new Kafka producer: %v", err)
	}
	defer producer.Close()
	consumer, err := mq.NewManualCommitConsumer(consumerGroup, orderTopic, brokers...)
	if err != nil {
		log.Fatalf("new Kafka consumer: %v", err)
	}
	defer consumer.Close()
	if err := mq.RegisterLagGauge(prometheus.DefaultRegisterer, producer, consumerGroup); err != nil {
		log.Fatalf("register Kafka lag gauge: %v", err)
	}
	go func() {
		for {
			pollCtx, pollCancel := context.WithTimeout(context.Background(), 5*time.Second)
			records, err := mq.PollBatch(pollCtx, consumer)
			pollCancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				log.Printf("poll order.created: %v", err)
				continue
			}
			for _, record := range records {
				processCtx, processCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, outcome, err := mq.ProcessWithRetry(processCtx, producer, dltTopic, record, mq.RetryPolicy{MaxAttempts: 3, BaseBackoff: 100 * time.Millisecond}, func(ctx context.Context, record *kgo.Record) (*order.Order, error) {
					return mq.PlaceRecord(ctx, db, node, record)
				})
				processCancel()
				if err != nil {
					log.Printf("process order.created topic=%s partition=%d offset=%d: %v", record.Topic, record.Partition, record.Offset, err)
					continue
				}
				if outcome.DeadLettered {
					log.Printf("order.created moved to DLT partition=%d offset=%d attempts=%d", record.Partition, record.Offset, outcome.Attempts)
				}
				commitCtx, commitCancel := context.WithTimeout(context.Background(), 3*time.Second)
				err = mq.CommitProcessed(commitCtx, consumer, record)
				commitCancel()
				if err != nil {
					log.Printf("commit order.created partition=%d offset=%d: %v", record.Partition, record.Offset, err)
				}
			}
		}
	}()

	productRepo := cache.NewCountingRepo(cache.NewSQLProductRepo(db))
	productCache := cache.New(rdb, productRepo, cache.DefaultOptions())
	if err := metrics.RegisterCacheDBLoads(prometheus.DefaultRegisterer, productRepo.Loads); err != nil {
		log.Fatalf("register cache metrics: %v", err)
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

	// ===== m02 商品读路径：/products/:id 走缓存，/debug/products/:id/nocache 直连 DB =====
	// 两个 handler 的 SQL 完全相同（都走 cache.SQLProductRepo.LoadProduct），
	// 唯一差别就是前面有没有那层缓存——这才能让 p5 的 on/off 对比只剩一个变量。
	r.GET("/products/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		start := time.Now()
		p, err := productCache.Get(c.Request.Context(), id)
		metrics.ProductReads.WithLabelValues("cached").Inc()
		metrics.ProductReadLatency.WithLabelValues("cached").Observe(time.Since(start).Seconds())
		switch {
		case err == nil:
			c.JSON(http.StatusOK, p)
		case errors.Is(err, cache.ErrProductNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	r.GET("/debug/products/:id/nocache", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		start := time.Now()
		p, err := cache.NewSQLProductRepo(db).LoadProduct(c.Request.Context(), id)
		metrics.ProductReads.WithLabelValues("nocache").Inc()
		metrics.ProductReadLatency.WithLabelValues("nocache").Observe(time.Since(start).Seconds())
		switch {
		case err == nil:
			c.JSON(http.StatusOK, p)
		case errors.Is(err, cache.ErrProductNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	// /debug/cache/warm/:id 预热单个商品；/debug/products/:id/stock 改库存（先更库再删缓存）。
	// 两个都是给 m02 的观察脚本用的最小接口，不是业务 API。
	r.POST("/debug/cache/warm/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		if err := productCache.Warm(c.Request.Context(), []int64{id}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"warmed": id})
	})

	r.POST("/debug/products/:id/stock", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		stock, err := strconv.Atoi(c.Query("stock"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad stock"})
			return
		}
		if err := productCache.UpdateStock(c.Request.Context(), id, stock); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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

	// ===== m03 防超卖四方案：/debug/orders/:approach =====
	// 四条路径做的是同一件业务（下一单），唯一差别是"扣库存"那一步用什么机制护住，
	// p5 的对比表才只剩一个变量。approach=pessimistic 就是 m01 那条 FOR UPDATE 基线。
	// /debug/deduct/warm/:id 把 DB 库存灌进 Redis，是 lua 那条路径的前提。
	r.POST("/debug/deduct/warm/:id", func(c *gin.Context) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
			return
		}
		if err := deduct.WarmStock(c.Request.Context(), rdb, db, id); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"warmed": id})
	})

	r.POST("/debug/orders/:approach", func(c *gin.Context) {
		var body deductOrderBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		approach := c.Param("approach")
		// 压测时 vegeta 反复重放同一份 body，request_id 只能由服务端现生成——
		// 否则每一发都撞 orders.uk_request_id，测到的就成了 m01 的幂等快路径而不是扣减机制。
		requestID := body.RequestID
		if requestID == "" {
			id, err := node.NextID()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			requestID = strconv.FormatInt(id, 10)
		}
		quantity := body.Quantity
		if quantity == 0 {
			quantity = 1
		}
		req := order.PlaceOrderRequest{
			RequestID: requestID,
			ProductID: body.ProductID,
			UserID:    body.UserID,
			Quantity:  quantity,
		}

		ctx := c.Request.Context()
		start := time.Now()
		var o *order.Order
		var err error
		switch approach {
		case "pessimistic":
			o, err = order.PlaceOrderTx(ctx, db, node, req)
		case "cas":
			var attempts int
			o, attempts, err = deduct.PlaceOrderByVersionCAS(ctx, db, node, req)
			metrics.CASAttempts.Add(float64(attempts))
		case "conditional":
			o, err = deduct.PlaceOrderByConditionalUpdate(ctx, db, node, req)
		case "lock":
			o, err = deduct.PlaceOrderWithLock(ctx, rdb, db, node, req, deduct.DefaultLockOptions())
		case "lua":
			o, err = deduct.PlaceOrderWithPreDeduct(ctx, rdb, db, node, req)
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "unknown approach: " + approach})
			return
		}
		metrics.DeductLatency.WithLabelValues(approach).Observe(time.Since(start).Seconds())

		switch {
		case err == nil:
			metrics.DeductOutcomes.WithLabelValues(approach, "success").Inc()
			c.JSON(http.StatusOK, o)
		case errors.Is(err, order.ErrInsufficientStock):
			metrics.DeductOutcomes.WithLabelValues(approach, "insufficient").Inc()
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		case errors.Is(err, deduct.ErrCASRetriesExhausted), errors.Is(err, deduct.ErrLockNotAcquired):
			// 这两个不是"卖光了"，是"竞争太激烈，这一发没轮上"——单独一类，
			// 否则对比表里会把方案的退化伪装成正常的售罄。
			metrics.DeductOutcomes.WithLabelValues(approach, "conflict").Inc()
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		default:
			metrics.DeductOutcomes.WithLabelValues(approach, "error").Inc()
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	// m04 异步接单：Redis 预扣 + Kafka broker ack 后立刻 202，DB 落单由上面的 consumer 完成。
	r.POST("/debug/orders/async", func(c *gin.Context) {
		var body deductOrderBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		requestID := body.RequestID
		if requestID == "" {
			id, err := node.NextID()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			requestID = strconv.FormatInt(id, 10)
		}
		quantity := body.Quantity
		if quantity == 0 {
			quantity = 1
		}
		produceCtx, produceCancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer produceCancel()
		accepted, err := mq.EnqueueOrder(produceCtx, rdb, producer, orderTopic, order.PlaceOrderRequest{RequestID: requestID, ProductID: body.ProductID, UserID: body.UserID, Quantity: quantity})
		switch {
		case err == nil:
			c.JSON(http.StatusAccepted, gin.H{"request_id": accepted.RequestID, "status": "accepted"})
		case errors.Is(err, order.ErrInsufficientStock):
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		}
	})

	// 202 之后的薄查询端点：没有完整订单状态机，DB 尚不可见时只返回 pending。
	r.GET("/orders/requests/:requestID", func(c *gin.Context) {
		var found order.Order
		err := db.GetContext(c.Request.Context(), &found, "SELECT id, product_id, user_id, request_id, quantity, status, created_at FROM orders WHERE request_id = ?", c.Param("requestID"))
		switch {
		case err == nil:
			c.JSON(http.StatusOK, found)
		case errors.Is(err, sql.ErrNoRows):
			c.JSON(http.StatusAccepted, gin.H{"request_id": c.Param("requestID"), "status": "pending"})
		default:
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
