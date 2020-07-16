package flush_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/flush"
)

type asyncMessageSinkMock struct {
	substrate.AsyncMessageSink
	publishMessageMock func(context.Context, chan<- substrate.Message, <-chan substrate.Message) error
}

func (m asyncMessageSinkMock) PublishMessages(ctx context.Context, acks chan<- substrate.Message, msgs <-chan substrate.Message) error {
	return m.publishMessageMock(ctx, acks, msgs)
}

func (m asyncMessageSinkMock) Close() error {
	return nil
}

const messages = 100

func TestAsyncMessageSinkSuccess(t *testing.T) {
	ctx := context.TODO()

	mock := asyncMessageSinkMock{
		publishMessageMock: func(ctx context.Context, acks chan<- substrate.Message, msgs <-chan substrate.Message) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgs:
					acks <- msg
				}
			}
		},
	}

	var ackedMessages string
	sink := flush.NewAsyncMessageSink(ctx, mock, flush.WithAckFunc(func(msg substrate.Message) error {
		ackedMessages += string(msg.Data())
		return nil
	}))

	var wg sync.WaitGroup
	wg.Add(messages)

	go sink.Run()
	for i := 0; i < messages; i++ {
		go func(i int) {
			defer wg.Done()

			sink.PublishMessage(ctx, []byte(string('A'+i)))
		}(i)
	}

	wg.Wait() // wait for the messages to publish

	// Usually we could defer `sink.Flush()`. However, as `Flush` blocks until ack
	// processing is complete, and we want to test the `ackedMessages` value *only*
	// once processing is complete, we must call `sink.Flush` first excplicitly -
	// only then we can safely test `ackedMessages`.
	err := sink.Flush()
	if err != nil {
		t.Fatal(err)
	}

	if ackedMessages == "" {
		t.Fatal("recieved no acks")
	}

	if ackedMessages == "ABCDEFGHIJ" {
		t.Fatal("recieved messages in synchronous order")
	}
}

func TestAsyncMessageSinkInterruption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())

	mock := asyncMessageSinkMock{
		publishMessageMock: func(ctx context.Context, acks chan<- substrate.Message, msgs <-chan substrate.Message) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgs:
					acks <- msg
				}
			}
		},
	}

	var ackedMessages int
	sink := flush.NewAsyncMessageSink(ctx, mock, flush.WithAckFunc(func(msg substrate.Message) error {
		time.Sleep(1 * time.Second) // simulating doing some work
		ackedMessages++
		return nil
	}))

	var wg sync.WaitGroup
	wg.Add(messages)

	go sink.Run()
	for i := 0; i < messages; i++ {
		go func(i int) {
			defer wg.Done()

			sink.PublishMessage(ctx, []byte(string('A'+i)))
		}(i)
	}

	wg.Wait() // wait for the messages to publish

	// Cancel production before we process the acks are processed.
	cancel()

	// Usually we could defer `sink.Flush()`. However, as `Flush` blocks until ack
	// processing is complete, and we want to test the `ackedMessages` value *only*
	// once processing is complete, we must call `sink.Flush` first excplicitly -
	// only then we can safely test `ackedMessages`.
	err := sink.Flush()
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestAsyncMessageSinkAckError(t *testing.T) {
	ctx := context.TODO()

	mock := asyncMessageSinkMock{
		publishMessageMock: func(ctx context.Context, acks chan<- substrate.Message, msgs <-chan substrate.Message) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgs:
					acks <- msg
				}
			}
		},
	}

	sink := flush.NewAsyncMessageSink(ctx, mock, flush.WithAckFunc(func(msg substrate.Message) error {
		return fmt.Errorf("error: %s", msg.Data())
	}))

	sink.PublishMessage(ctx, []byte("dummy-message"))

	err := sink.Run()
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestAsyncMessageSinkPublishError(t *testing.T) {
	mock := asyncMessageSinkMock{
		publishMessageMock: func(ctx context.Context, acks chan<- substrate.Message, msgs <-chan substrate.Message) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgs:
					acks <- msg
				}
			}
		},
	}

	var acks uint32
	sink := flush.NewAsyncMessageSink(context.TODO(), mock, flush.WithAckFunc(func(msg substrate.Message) error {
		atomic.AddUint32(&acks, 1)
		return nil
	}))

	go sink.Run()

	reqCtx, reqCancel := context.WithCancel(context.TODO())
	reqCancel()

	errCanceled := sink.PublishMessage(reqCtx, []byte("dummy-message"))

	err := sink.PublishMessage(context.TODO(), []byte("dummy-message"))
	if err != nil {
		t.Fatal(err)
	}

	sink.Flush()

	a := atomic.LoadUint32(&acks)
	if (a == 1 && errCanceled == nil) && a != 2 {
		t.Fatalf("expected either 2 acks, or 1 ack and 1 error: got %d and %v", a, errCanceled)
	}
}

func TestAsyncMessageSinkEarlyCloseProducesPanic(t *testing.T) {
	ctx := context.TODO()

	mock := asyncMessageSinkMock{
		publishMessageMock: func(ctx context.Context, acks chan<- substrate.Message, msgs <-chan substrate.Message) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgs:
					acks <- msg
				}
			}
		},
	}

	sink := flush.NewAsyncMessageSink(ctx, mock)
	sink.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected a panic")
		}
	}()

	go sink.Run()
	sink.PublishMessage(ctx, []byte("dummy-message")) // send to a closed channel
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

	mock := asyncMessageSinkMock{
		publishMessageMock: func(ctx context.Context, acks chan<- substrate.Message, msgs <-chan substrate.Message) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgs:
					acks <- msg
				}
			}
		},
	}

	var ackedMessages string
	sink := flush.NewAsyncMessageSink(ctx, mock, flush.WithAckFunc(func(msg substrate.Message) error {
		ackedMessages += string(msg.Data())
		return nil
	}), flush.WithMsgBufferSize(msgBufferSize), flush.WithAckBufferSize(ackBufferSize))

	var wg sync.WaitGroup
	wg.Add(messages)

	go sink.Run()
	for i := 0; i < messages; i++ {
		go func(i int) {
			defer wg.Done()

			sink.PublishMessage(ctx, []byte(string('A'+i)))
		}(i)
	}

	wg.Wait() // wait for the messages to publish

	// Usually we could defer `sink.Flush()`. However, as `Flush` blocks until ack
	// processing is complete, and we want to test the `ackedMessages` value *only*
	// once processing is complete, we must call `sink.Flush` first excplicitly -
	// only then we can safely test `ackedMessages`.
	err := sink.Flush()
	if err != nil {
		b.Fatal(err)
	}

	if ackedMessages == "" {
		b.Fatal("recieved no acks")
	}

	if ackedMessages == "ABCDEFGHIJ" {
		b.Fatal("recieved messages in synchronous order")
	}
}
