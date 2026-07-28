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
	details, err := admin.ListTopics(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, topic := range []string{envOr("KAFKA_TOPIC", "order.created"), envOr("KAFKA_DLT_TOPIC", "order.created.DLT")} {
		detail, ok := details[topic]
		if !ok || detail.Err != nil {
			log.Fatalf("topic %s unavailable: %+v", topic, detail)
		}
		if len(detail.Partitions) != 3 {
			log.Fatalf("topic %s partitions=%d want=3", topic, len(detail.Partitions))
		}
		fmt.Printf("TOPIC topic=%s partitions=%d\n", topic, len(detail.Partitions))
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
