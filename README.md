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

### Instrumented
Provides wrappers for both message source and message sink that add prometheus metrics labeled with topic and status (either success or error).

### Multi
Is a message source wrapper that wraps any number of sources. It consumes messages from all of them and passes them on to the user.
It ensures that the acknowledgements are passed to the correct source.

### Flush
Is a message flushing wrapper which blocks until all produced messages have been acked by the user. In the scenario that the user performs an action only after a message has been produced, the flushing wrapper provides a guarantee that such an action is only performed on a successful sink.

#### Example usage

```go
    asyncSink, err := kafka.NewAsyncMessageSink(kafka.AsyncMessageSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "example-stratesub-topic",
		Version: "2.0.1",
	})
	if err != nil {
		panic(err)
	}

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
```

## Other

### Message
Provides a simple implementation of the `substrate.Message` interface.

### Mock
Provides a mock message source that can be used in testing as is done in this repo.
