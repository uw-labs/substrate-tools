package main

import (
	"context"
	"time"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/ackordering"
	"github.com/uw-labs/substrate-tools/simple"
	"github.com/uw-labs/substrate/kafka"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	asyncSource, err := kafka.NewAsyncMessageSource(kafka.AsyncMessageSourceConfig{
		ConsumerGroup: "simple-consumer",
		Brokers:       []string{"localhost:9092"},
		Topic:         "example-topic",
		Version:       "2.0.1",
		Offset:        -2, // oldest
	})
	if err != nil {
		panic(err)
	}

	msgFn := func(_ context.Context, msg substrate.Message) error {
		println("consumed:", string(msg.Data()))
		return nil
	}

	asyncSource = ackordering.NewAsyncMessageSource(asyncSource)
	source := simple.NewAsyncMessageSource(ctx, asyncSource, simple.WithMsgFunc(msgFn))

	defer func() {
		err := source.Close()
		if err != nil {
			panic(err)
		}
	}()

	err = source.Run()
	if err != nil {
		panic(err)
	}
}
