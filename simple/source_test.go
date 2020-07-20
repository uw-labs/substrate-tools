package simple_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/message"
	"github.com/uw-labs/substrate-tools/simple"
)

type messageSourceMock struct {
	substrate.AsyncMessageSource
	consumerMessagesMock func(context.Context, chan<- substrate.Message, <-chan substrate.Message) error
}

func (m messageSourceMock) ConsumeMessages(ctx context.Context, msgs chan<- substrate.Message, acks <-chan substrate.Message) error {
	return m.consumerMessagesMock(ctx, msgs, acks)
}

func (m messageSourceMock) Close() error {
	return nil
}

func TestAsyncMessageSourceSuccess(t *testing.T) {
	ctx := context.TODO()
	messages := []string{"one", "two", "three"}

	mock := &messageSourceMock{
		consumerMessagesMock: func(ctx context.Context, msgs chan<- substrate.Message, acks <-chan substrate.Message) error {
			for _, m := range messages {
				msgs <- message.FromString(m)
			}

			for {
				select {
				case <-ctx.Done():
					return nil
				case <-acks:
					// do nothing
				}
			}
		},
	}

	var wg sync.WaitGroup
	wg.Add(len(messages))

	source := simple.NewAsyncMessageSource(ctx, mock, simple.WithMsgFunc(func(_ context.Context, msg substrate.Message) error {
		wg.Done()
		return nil
	}))

	defer func() {
		err := source.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	go source.Run()
	wg.Wait()
}

func TestAsyncMessageSourceMissingMessageFn(t *testing.T) {
	ctx := context.TODO()
	messages := []string{"message"}

	mock := &messageSourceMock{
		consumerMessagesMock: func(ctx context.Context, msgs chan<- substrate.Message, acks <-chan substrate.Message) error {
			for _, m := range messages {
				msgs <- message.FromString(m)
			}

			for {
				select {
				case <-ctx.Done():
					return nil
				case <-acks:
					// do nothing
				}
			}
		},
	}

	source := simple.NewAsyncMessageSource(ctx, mock)

	defer func() {
		err := source.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	err := source.Run()
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestAsyncMessageSourceMsgFnError(t *testing.T) {
	ctx := context.TODO()
	messages := []string{"message"}

	mock := &messageSourceMock{
		consumerMessagesMock: func(ctx context.Context, msgs chan<- substrate.Message, acks <-chan substrate.Message) error {
			for _, m := range messages {
				msgs <- message.FromString(m)
			}

			for {
				select {
				case <-ctx.Done():
					return nil
				case <-acks:
					// do nothing
				}
			}
		},
	}

	msgFnErr := fmt.Errorf("dummy error")

	source := simple.NewAsyncMessageSource(ctx, mock, simple.WithMsgFunc(func(_ context.Context, msg substrate.Message) error {
		return msgFnErr
	}))

	defer func() {
		err := source.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	err := source.Run()
	if err == nil {
		t.Fatal("expected an error")
	}

	if err != msgFnErr {
		t.Fatalf("expected %v, have %v", msgFnErr, err)
	}
}

func BenchmarkAsyncMessageSink_1_50(b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchmarkBufferSizes(b, 1, 50)
	}
}

func BenchmarkAsyncMessageSink_50_100(b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchmarkBufferSizes(b, 50, 100)
	}
}

func BenchmarkAsyncMessageSink_100_100(b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchmarkBufferSizes(b, 100, 100)
	}
}

func BenchmarkAsyncMessageSink_100_50(b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchmarkBufferSizes(b, 100, 50)
	}
}

func BenchmarkAsyncMessageSink_1000_1000(b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchmarkBufferSizes(b, 1000, 1000)
	}
}

func benchmarkBufferSizes(b *testing.B, msgBufferSize, ackBufferSize int) {
	ctx := context.TODO()
	messages := []string{"one", "two", "three"}

	mock := &messageSourceMock{
		consumerMessagesMock: func(ctx context.Context, msgs chan<- substrate.Message, acks <-chan substrate.Message) error {
			for _, m := range messages {
				msgs <- message.FromString(m)
			}

			for {
				select {
				case <-ctx.Done():
					return nil
				case <-acks:
					// do nothing
				}
			}
		},
	}

	var wg sync.WaitGroup
	wg.Add(len(messages))

	source := simple.NewAsyncMessageSource(ctx, mock, simple.WithMsgFunc(func(_ context.Context, msg substrate.Message) error {
		wg.Done()
		return nil
	}), simple.WithSourceMsgBufferSize(msgBufferSize), simple.WithSourceAckBufferSize(ackBufferSize))

	defer func() {
		err := source.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	go source.Run()
	wg.Wait()
}
