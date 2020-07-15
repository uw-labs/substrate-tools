package main

import (
	"context"
	"sync"
	"time"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/flush"
	"github.com/uw-labs/substrate/kafka"
)

func main() {
	asyncSink, err := kafka.NewAsyncMessageSink(kafka.AsyncMessageSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "example-stratesub-topic",
		Version: "2.0.1",
	})
	if err != nil {
		panic(err)
	}

	// `cancel` is used to terminate `sink.Run`. Timeout is used to prevent
	// `sink.Flush` being able to infinitely block.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sink := flush.NewAsyncMessageSink(asyncSink, flush.WithAckFunc(func(msg substrate.Message) error {
		println(string(msg.Data()))
		return nil
	}))
	defer func() {
		err := sink.Flush(ctx)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		err = sink.Run(ctx)
		if err != nil {
			panic(err)
		}
	}()

	messages := []string{
		"message one",
		"message two",
		"message three",
		"message four",
		"message five",
		"message six",
		"message seven",
		"message eight",
		"message nine",
		"message ten",
	}

	var wg sync.WaitGroup
	wg.Add(len(messages))

	for _, msg := range messages {
		go func(msg string) {
			defer wg.Done()

			sink.PublishMessage([]byte(msg))
		}(msg)
	}

	wg.Wait()
}
