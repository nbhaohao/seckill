package mq

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
	panic("TODO: phase p4") // AI 将在 p4 S2 按上面的 why 边界实现。
}
