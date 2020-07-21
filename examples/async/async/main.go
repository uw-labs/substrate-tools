package main

import (
	"context"
	"time"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/ackordering"
	"github.com/uw-labs/substrate-tools/async"
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

	asyncSource = ackordering.NewAsyncMessageSource(asyncSource)

	opts := []async.MessageSourceOption{
		async.WithSourceConsumers(10),     // 10 concurrent message handlers
		async.WithSourceMsgBufferSize(10), // buffer 10 messages from the source
		async.WithSourceAckBufferSize(10), // buffer 10 acks from the ConsumerMessageHandler
	}

	source := async.NewMessageSource(asyncSource, opts...)
	defer func() {
		err := source.Close()
		if err != nil {
			panic(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler := func(ctx context.Context, msg substrate.Message, ack async.AckFunc) error {
		defer ack()

		println("consumed:", string(msg.Data()))
		return nil
	}

	err = source.ConsumeMessages(ctx, handler)
	if err != nil {
		panic(err)
	}
}
