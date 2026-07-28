// Command kafka-bootstrap creates the two m04 topics through kadm. It is
// idempotent: TopicAlreadyExists is accepted after metadata confirms 3 parts.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := strings.Split(envOr("KAFKA_BROKERS", "127.0.0.1:9092"), ",")
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := kadm.NewClient(client)
	for _, topic := range []string{envOr("KAFKA_TOPIC", "order.created"), envOr("KAFKA_DLT_TOPIC", "order.created.DLT")} {
		_, _ = admin.CreateTopic(ctx, 3, 1, nil, topic)
	}
	// CreateTopic returns before every broker has the new metadata, so poll
	// instead of failing on the first empty partition list.
	for _, topic := range []string{envOr("KAFKA_TOPIC", "order.created"), envOr("KAFKA_DLT_TOPIC", "order.created.DLT")} {
		partitions, err := waitForPartitions(ctx, admin, topic, 3)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("TOPIC topic=%s partitions=%d\n", topic, partitions)
	}
}

func waitForPartitions(ctx context.Context, admin *kadm.Client, topic string, want int) (int, error) {
	var last string
	for {
		details, err := admin.ListTopics(ctx)
		if err != nil {
			return 0, err
		}
		detail, ok := details[topic]
		switch {
		case !ok:
			last = "not in metadata yet"
		case detail.Err != nil:
			last = detail.Err.Error()
		case len(detail.Partitions) == want:
			return len(detail.Partitions), nil
		default:
			last = fmt.Sprintf("partitions=%d want=%d", len(detail.Partitions), want)
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("topic %s not ready: %s", topic, last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
