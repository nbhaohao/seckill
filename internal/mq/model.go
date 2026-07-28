// Package mq contains the m04 Kafka order pipeline.
//
// This file is already in place (AI generated): wire types, JSON shape, and
// sentinel errors are shared scaffolding rather than a learning target.
package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nbhaohao/go-seckill/internal/order"
)

const (
	OrderCreatedTopic = "order.created"
	OrderCreatedDLT   = "order.created.DLT"
)

var ErrNoRecord = errors.New("mq: poll returned no record before context ended")

// OrderCreated is deliberately JSON: schema registries and Avro/Protobuf are
// outside m04. RequestID is the idempotency key reused by orders.uk_request_id.
type OrderCreated struct {
	RequestID  string    `json:"request_id"`
	ProductID  int64     `json:"product_id"`
	UserID     int64     `json:"user_id"`
	Quantity   int       `json:"quantity"`
	AcceptedAt time.Time `json:"accepted_at"`
}

type AcceptedOrder struct {
	RequestID string
	Status    int
	Accepted  time.Time
}

type RetryPolicy struct {
	MaxAttempts int
	BaseBackoff time.Duration
}

type RetryResult struct {
	Attempts     int
	Backoffs     []time.Duration
	DeadLettered bool
}

type RecordHandler func(context.Context, *kgo.Record) (*order.Order, error)

func encodeOrderCreated(req order.PlaceOrderRequest, acceptedAt time.Time) ([]byte, error) {
	return json.Marshal(OrderCreated{
		RequestID: req.RequestID, ProductID: req.ProductID, UserID: req.UserID,
		Quantity: req.Quantity, AcceptedAt: acceptedAt,
	})
}

func DecodeOrderCreated(record *kgo.Record) (OrderCreated, error) {
	var event OrderCreated
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return OrderCreated{}, fmt.Errorf("decode order.created: %w", err)
	}
	return event, nil
}

func RequestFromRecord(record *kgo.Record) (order.PlaceOrderRequest, error) {
	event, err := DecodeOrderCreated(record)
	if err != nil {
		return order.PlaceOrderRequest{}, err
	}
	return order.PlaceOrderRequest{
		RequestID: event.RequestID, ProductID: event.ProductID,
		UserID: event.UserID, Quantity: event.Quantity,
	}, nil
}

func dltHeaders(record *kgo.Record, cause error, attempts int) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "failure-reason", Value: []byte(cause.Error())},
		{Key: "retry-count", Value: []byte(strconv.Itoa(attempts))},
		{Key: "original-topic", Value: []byte(record.Topic)},
		{Key: "original-partition", Value: []byte(strconv.FormatInt(int64(record.Partition), 10))},
		{Key: "original-offset", Value: []byte(strconv.FormatInt(record.Offset, 10))},
	}
}

func HeaderMap(record *kgo.Record) map[string]string {
	out := make(map[string]string, len(record.Headers))
	for _, header := range record.Headers {
		out[header.Key] = string(header.Value)
	}
	return out
}
