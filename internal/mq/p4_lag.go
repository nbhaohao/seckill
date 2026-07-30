package mq

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// lagScrapeTimeout bounds each /metrics read of GroupLagTotal so an
// unreachable broker cannot leave the handler blocked forever.
const lagScrapeTimeout = 2 * time.Second

// GroupLagTotal is m04 p4 S1. AI will implement it during p4.
//  1. kadm computes lag from committed group offsets and log-end offsets;
//     GroupLag.Total is the sum across this group's topic partitions.
func GroupLagTotal(ctx context.Context, client *kgo.Client, group string) (int64, error) {
	lags, err := kadm.NewClient(client).Lag(ctx, group)
	if err != nil {
		return 0, err
	}
	described, ok := lags[group]
	if !ok {
		return 0, fmt.Errorf("group %s not found in lag response", group)
	}
	if err := described.Error(); err != nil {
		return 0, err
	}
	return described.Lag.Total(), nil
}

// RegisterLagGauge is m04 p4 S2. AI will implement it during p4.
//  1. The gauge reads current lag at scrape time; it does not invent a second
//     counter whose value can drift away from Kafka's committed offsets.
//  2. Each scrape gets its own short deadline; an unavailable broker must not
//     leave the /metrics handler blocked forever.
func RegisterLagGauge(reg prometheus.Registerer, client *kgo.Client, group string) error {
	return reg.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "seckill_kafka_consumer_group_lag",
		Help: "Kafka consumer group 已提交 offset 与 log end 的差距总和，-1 表示查询失败。",
	}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), lagScrapeTimeout)
		defer cancel()
		lag, err := GroupLagTotal(ctx, client, group)
		if err != nil {
			return -1
		}
		return float64(lag)
	}))
}
