# substrate-tools [![CircleCI](https://circleci.com/gh/uw-labs/substrate-tools.svg?style=svg)](https://circleci.com/gh/uw-labs/substrate-tools)
Collection of middleware, wrappers and other tools for substrate.

## Wrappers

### Ack Ordering
Is a message source wrapper that allows the user to acknowledge messages in any order and it will ensure
messages are sent to the actual message source in the same order they are consumed.

### Async
Is an async message source wrapper that allows the user to utilise a handler pattern for interacting
with an async message source. It removes the need to manually handle the message and acknowledgement
channels.

See https://github.com/uw-labs/substrate-tools/tree/master/examples/async for example usage.

### Instrumented
Provides wrappers for both message source and message sink that add prometheus metrics labeled with topic and status (either success or error).

### Multi
Is a message source wrapper that wraps any number of sources. It consumes messages from all of them and passes them on to the user.
It ensures that the acknowledgements are passed to the correct source.

### Flush
Is a message flushing wrapper which blocks until all produced messages have been acked by the user. In the scenario that the user performs an action only after a message has been produced, the flushing wrapper provides a guarantee that such an action is only performed on a successful sink.

```go
import (
	"github.com/uw-labs/substrate-tools/flush"
)
```

#### Simple Example

```go
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

flushSink := flush.NewAsyncMessageSink(context.Background(), sink)
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
```

#### Complex example with Ack function and buffer sizes


```go
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
```

## Other

### Message
Provides a simple implementation of the `substrate.Message` interface.

### Mock
Provides a mock message source that can be used in testing as is done in this repo.
