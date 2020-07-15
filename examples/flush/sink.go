package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/flush"
	"github.com/uw-labs/substrate/kafka"
)

const messages = 200

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
		fmt.Println(string(msg.Data()))
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

	var wg sync.WaitGroup
	wg.Add(messages)

	for i := 0; i < messages; i++ {
		go func(i int) {
			defer wg.Done()

			sink.PublishMessage([]byte(string('A' + i)))
		}(i)
	}

	wg.Wait()
}
