package mq

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// NewManualCommitConsumer is m04 p2 S1. AI will implement it during p2.
//  1. DisableAutoCommit makes the offset boundary visible: processing success
//     comes first, then CommitRecords. A crash between them intentionally replays.
func NewManualCommitConsumer(group, topic string, brokers ...string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
}

// CommitProcessed is m04 p2 S2. AI will implement it during p2.
//  1. Only a successfully handled record may cross this line. Committing first
//     converts a crash into silent order loss rather than safe duplicate work.
func CommitProcessed(ctx context.Context, consumer *kgo.Client, record *kgo.Record) error {
	return consumer.CommitRecords(ctx, record)
}
