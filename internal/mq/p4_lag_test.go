package mq

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestM04P4LagRisesThenReturnsToZero(t *testing.T) {
	admin := openKafka(t)
	topic, group := uniqueName(t, "p4-orders"), uniqueName(t, "p4-group")
	createTopic(t, admin, topic)
	const total = 12
	for i := 0; i < total; i++ {
		produceRaw(t, admin, topic, reqFor(10407, uniqueName(t, "p4-request")))
	}
	consumer, err := NewManualCommitConsumer(group, topic, testBroker)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 15*time.Second)
	records, err := PollBatch(pollCtx, consumer)
	pollCancel()
	if err != nil || len(records) != total {
		t.Fatalf("poll batch: records=%d want=%d err=%v", len(records), total, err)
	}
	first := records[0]
	if err := CommitProcessed(context.Background(), consumer, first); err != nil {
		t.Fatalf("commit first: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	var lagSeries []int64
	var high int64
	for time.Now().Before(deadline) {
		lag, err := GroupLagTotal(context.Background(), admin, group)
		if err == nil {
			lagSeries = append(lagSeries, lag)
			if lag > high {
				high = lag
			}
			if lag > 0 {
				break
			}
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-time.After(time.Until(deadline)):
		}
	}
	if high <= 0 {
		t.Fatalf("expected lag > 0 after partial consumption, series=%v", lagSeries)
	}

	for consumed := 1; consumed < total; consumed++ {
		record := records[consumed]
		if err := CommitProcessed(context.Background(), consumer, record); err != nil {
			t.Fatalf("commit record %d: %v", consumed, err)
		}
	}
	for time.Now().Before(deadline) {
		lag, err := GroupLagTotal(context.Background(), admin, group)
		if err == nil {
			lagSeries = append(lagSeries, lag)
			if lag == 0 {
				t.Logf("lag timeline=%v; rose to %d then drained to zero", lagSeries, high)
				return
			}
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-time.After(time.Until(deadline)):
		}
	}
	t.Fatalf("lag did not drain to zero before timeout, series=%v", lagSeries)
}

func TestM04P4ThreePartitionsLeaveFourthConsumerIdle(t *testing.T) {
	admin := openKafka(t)
	topic, group := uniqueName(t, "p4-scale"), uniqueName(t, "p4-scale-group")
	createTopic(t, admin, topic)
	joinCtx, cancelJoin := context.WithCancel(context.Background())
	defer cancelJoin()
	consumers := make([]*kgo.Client, 0, 4)
	for i := 0; i < 4; i++ {
		client, err := kgo.NewClient(kgo.SeedBrokers(testBroker), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.DisableAutoCommit())
		if err != nil {
			t.Fatalf("new consumer %d: %v", i, err)
		}
		consumers = append(consumers, client)
		go client.PollFetches(joinCtx)
	}
	t.Cleanup(func() {
		cancelJoin()
		for _, client := range consumers {
			client.Close()
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	var members, active, idle int
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		groups, err := kadm.NewClient(admin).DescribeGroups(ctx, group)
		cancel()
		if err == nil {
			described := groups[group]
			if described.State == "Stable" && len(described.Members) == 4 {
				members = len(described.Members)
				for _, member := range described.Members {
					assignment, ok := member.Assigned.AsConsumer()
					assigned := 0
					if ok {
						for _, topicAssignment := range assignment.Topics {
							assigned += len(topicAssignment.Partitions)
						}
					}
					if assigned == 0 {
						idle++
					} else {
						active++
					}
				}
				break
			}
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-time.After(time.Until(deadline)):
		}
	}
	if _, err := GroupLagTotal(context.Background(), admin, group); err != nil {
		t.Fatalf("read group lag: %v", err)
	}
	if members != 4 || active != 3 || idle != 1 {
		t.Fatalf("3 partitions with 4 consumers: members=%d active=%d idle=%d", members, active, idle)
	}
	t.Logf("topic partitions=3 group members=%d active consumers=%d idle consumers=%d", members, active, idle)
}
