// Command kafka-smoke proves the single-broker m04 environment before study:
// create a three-partition topic, produce one record, consume and commit it,
// then read the real group lag through kadm.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	topic := fmt.Sprintf("m04-smoke-%d", time.Now().UnixNano())
	group := topic + "-group"
	adminClient, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:9092"))
	must(err)
	defer adminClient.Close()
	must(adminClient.Ping(ctx))
	admin := kadm.NewClient(adminClient)
	defer func() {
		_, _ = admin.DeleteGroup(context.Background(), group)
		_, _ = admin.DeleteTopic(context.Background(), topic)
	}()
	created, err := admin.CreateTopic(ctx, 3, 1, nil, topic)
	must(err)
	fmt.Printf("CREATE topic=%s partitions=3 err=%v\n", created.Topic, created.Err)

	record := &kgo.Record{Topic: topic, Key: []byte("product-42"), Value: []byte("smoke-order")}
	must(adminClient.ProduceSync(ctx, record).FirstErr())
	fmt.Printf("PRODUCE topic=%s partition=%d offset=%d key=%s\n", record.Topic, record.Partition, record.Offset, record.Key)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:9092"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	must(err)
	defer consumer.Close()
	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		must(errs[0].Err)
	}
	iter := fetches.RecordIter()
	if iter.Done() {
		must(fmt.Errorf("consume returned no record"))
	}
	got := iter.Next()
	fmt.Printf("CONSUME topic=%s partition=%d offset=%d value=%s\n", got.Topic, got.Partition, got.Offset, got.Value)
	must(consumer.CommitRecords(ctx, got))

	lags, err := admin.Lag(ctx, group)
	must(err)
	lag := lags[group]
	must(lag.Error())
	fmt.Printf("LAG group=%s total=%d state=%s members=%d\n", group, lag.Lag.Total(), lag.State, len(lag.Members))
}
