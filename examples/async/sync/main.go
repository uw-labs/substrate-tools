package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/async"
	"github.com/uw-labs/substrate-tools/instrumented"
	"github.com/uw-labs/substrate/kafka"
)

func main() {
	asyncSource, err := kafka.NewAsyncMessageSource(kafka.AsyncMessageSourceConfig{
		ConsumerGroup: "example-async-sync",
		Brokers:       []string{"localhost:9092"},
		Topic:         "example-topic",
		Version:       "2.0.1",
		Offset:        -2, // oldest
	})
	if err != nil {
		panic(err)
	}

	asyncSource = instrumented.NewAsyncMessageSource(asyncSource, prometheus.CounterOpts{
		Name: "messages_consumed_total",
		Help: "Total count of messages consumed",
	}, "example-topic", "example-async-sync")

	source := async.NewMessageSource(asyncSource)
	defer func() {
		err := source.Close()
		if err != nil {
			panic(err)
		}
	}()

	handler := func(ctx context.Context, msg substrate.Message, ack async.AckFunc) error {
		println("consumed:", string(msg.Data()))
		return ack()
	}

	// source.ConsumeMessages blocks. CTRL+C to exit.
	err = source.ConsumeMessages(context.Background(), handler)
	if err != nil {
		panic(err)
	}
}
