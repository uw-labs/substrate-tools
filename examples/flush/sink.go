package main

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/flush"
	"github.com/uw-labs/substrate/instrumented"
	"github.com/uw-labs/substrate/kafka"
)

func main() {
	sink, err := kafka.NewAsyncMessageSink(kafka.AsyncMessageSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "example-topic",
		Version: "2.0.1",
	})
	if err != nil {
		panic(err)
	}

	sink = instrumented.NewAsyncMessageSink(sink, prometheus.CounterOpts{
		Name: "messages_produced_total",
		Help: "The total count of messages produced",
	}, "topic")

	ackFn := flush.WithAckFunc(func(msg substrate.Message) error {
		println(string(msg.Data()))
		return nil
	})

	flushSink := flush.NewAsyncMessageSink(context.Background(), sink,
		ackFn, flush.WithMsgBufferSize(50), flush.WithAckBufferSize(100))

	defer func() {
		// Flush will block until all messages produced using `PublishMessage` have been acked
		// by the AckFunc, if provided.
		err := flushSink.Flush()
		if err != nil {
			panic(err)
		}

		err = flushSink.Close()
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		// Run blocks until the sink is closed or an error occurs. If the AckFn retruns an error
		// it will be returned by `Run`.
		err = flushSink.Run()
		if err != nil {
			panic(err)
		}
	}()

	messages := []string{
		"message one",
		"message two",
	}

	var wg sync.WaitGroup
	wg.Add(len(messages))

	for _, msg := range messages {
		go func(msg string) {
			defer wg.Done()

			// This context is used to control the publishing of the message. This ctx
			// could, and probably should, be different to the lifecyle context passed
			// into the constructor.
			err := flushSink.PublishMessage(context.Background(), []byte(msg))
			if err != nil {
				panic(err)
			}
		}(msg)
	}

	wg.Wait()
}
