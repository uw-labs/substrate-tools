// Package flush provides a message flushing wrapper around `substrate.AsyncMessageSink`.
package flush

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/message"
	"github.com/uw-labs/sync/rungroup"
)

const (
	defaultMsgBufferSize = 50
	defaultAckBufferSize = 100
)

// An AsyncMessageSinkOption is a function which sets an AsyncMessageSink configuration option.
type AsyncMessageSinkOption func(ams *AsyncMessageSink)

// WithAckFunc set the AckFunc callback function, which is called for each ack recieved. If the
// AckFunc returns an error, it will be returned by `Run`.
func WithAckFunc(ackFn AckFunc) AsyncMessageSinkOption {
	return func(ams *AsyncMessageSink) {
		ams.ackFn = ackFn
	}
}

// WithMsgBufferSize sets the msg channel buffer size.
func WithMsgBufferSize(size int) AsyncMessageSinkOption {
	return func(ams *AsyncMessageSink) {
		ams.msgBufferSize = size
	}
}

// WithAckBufferSize sets the ack channel buffer size.
func WithAckBufferSize(size int) AsyncMessageSinkOption {
	return func(ams *AsyncMessageSink) {
		ams.ackBufferSize = size
	}
}

// AckFunc is a callback function executed by `Run` for each message succesfully produced. The ack
// counter is only incremented once this function returns. If no AckFunc is provided the ack counter
// is always incremented.
type AckFunc func(msg substrate.Message) error

// AsyncMessageSink wraps substrate.AsyncMessageSink and provides an interface for interaction with
// the underlying sink, as well as the capability to flush messages.
type AsyncMessageSink struct {
	sink          substrate.AsyncMessageSink
	msgs          uint64
	msgBufferSize int
	msgCh         chan substrate.Message
	acks          uint64
	ackBufferSize int
	ackCh         chan substrate.Message
	ackFn         AckFunc
}

// NewAsyncMessageSink returns a pointer a new AsyncMessageSink.
// See examples/flush/sink.go for example usage.
func NewAsyncMessageSink(sink substrate.AsyncMessageSink, opts ...AsyncMessageSinkOption) *AsyncMessageSink {
	ams := &AsyncMessageSink{
		sink:          sink,
		msgBufferSize: defaultMsgBufferSize,
		ackBufferSize: defaultAckBufferSize,
	}

	for _, opt := range opts {
		opt(ams)
	}

	ams.msgCh = make(chan substrate.Message, ams.msgBufferSize)
	ams.ackCh = make(chan substrate.Message, ams.ackBufferSize)

	return ams
}

// Run initialises message publishing using the underlying sink and blocks until either an error
// occurs or the context is done. If an AckFunc is configured, Run will execute it for each ack
// recieved. If an error is returned, the user should cancel the context to prevent `Flush` from
// blocking.
func (ams *AsyncMessageSink) Run(ctx context.Context) error {
	group, groupctx := rungroup.New(ctx)

	group.Go(func() error {
		return ams.sink.PublishMessages(groupctx, ams.ackCh, ams.msgCh)
	})

	group.Go(func() error {
		for {
			select {
			case <-groupctx.Done():
				return nil
			case msg := <-ams.ackCh:
				if ams.ackFn != nil {
					err := ams.ackFn(msg)
					if err != nil {
						return err
					}
				}

				atomic.AddUint64(&ams.acks, 1)
			}
		}
	})

	return group.Wait()
}

// PublishMessage publishes a message to the underlying sink. PublishMessage is desgined to be
// called concurrently by the user.
func (ams *AsyncMessageSink) PublishMessage(msg []byte) {
	atomic.AddUint64(&ams.msgs, 1)
	ams.msgCh <- message.NewMessage(msg)
}

// Close closes the underlying sink and releases it's resources.
func (ams *AsyncMessageSink) Close() error {
	return ams.sink.Close()
}

// Status calls the Status method on the underlying sink.
func (ams *AsyncMessageSink) Status() (*substrate.Status, error) {
	return ams.sink.Status()
}

// Flush blocks until the AsyncMessageSink has consumed as many acks as messages produced or ctx
// is done. Flush returns an error if the context is cancelled before all messages produced have
// been acked.
func (ams *AsyncMessageSink) Flush(ctx context.Context) error {
	defer ams.Close()

	for {
		select {
		case <-ctx.Done():
			if atomic.LoadUint64(&ams.msgs) > atomic.LoadUint64(&ams.acks) {
				return fmt.Errorf("incomplete flush: %d left to ack", atomic.LoadUint64(&ams.msgs)-atomic.LoadUint64(&ams.acks))
			}

			return nil
		default:
			if atomic.LoadUint64(&ams.acks) != atomic.LoadUint64(&ams.msgs) {
				continue
			}

			return nil
		}
	}
}
