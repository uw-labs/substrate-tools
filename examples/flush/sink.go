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
		Topic:   "example-topic",
		Version: "2.0.1",
	})
	if err != nil {
		panic(err)
	}

	// This context is used to control the goroutines initialised by the flushing wrapper.
	// Canelling this context closes the underlying sink and allows `Flush` to return.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ackFn := flush.WithAckFunc(func(msg substrate.Message) error {
		println(string(msg.Data()))
		return nil
	})

	sink := flush.NewAsyncMessageSink(ctx, asyncSink, ackFn)
	defer func() {
		// Flush will block until all messages produced using `PublishMessage` have been acked
		// by the AckFunc, if provided.
		err := sink.Flush()
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		// Run blocks until the sink is closed or an error occurs. If the AckFn retruns an error
		// it will be returned by `Run`.
		err = sink.Run()
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

			// This context is used to control the publishing of the message. This ctx
			// could, and probably should, be different to the lifecyle context passed
			// into the constructor.
			sink.PublishMessage(context.Background(), []byte(msg))
		}(msg)
	}

	wg.Wait()
}
