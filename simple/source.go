// Package simple provides a smaller and simpler API for substrate.
package simple

import (
	"context"
	"fmt"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"
)

// An AsyncMessageSourceOption is a function which sets an AsyncMessageSource configuration option.
type AsyncMessageSourceOption func(ams *AsyncMessageSource)

// WithSourceMsgBufferSize sets the msg channel buffer size.
func WithSourceMsgBufferSize(size int) AsyncMessageSourceOption {
	return func(ams *AsyncMessageSource) {
		ams.msgBufferSize = size
	}
}

// WithSourceAckBufferSize sets the ack channel buffer size.
func WithSourceAckBufferSize(size int) AsyncMessageSourceOption {
	return func(ams *AsyncMessageSource) {
		ams.ackBufferSize = size
	}
}

// WithSourceConsumers sets the number of concurrent source consumers.
func WithSourceConsumers(consumers int) AsyncMessageSourceOption {
	return func(ams *AsyncMessageSource) {
		ams.consumers = consumers
	}
}

// WithMsgFunc sets the function to be called for each message recieved. Not providing
// a MsgFunc causes `Run` to produce an error.
func WithMsgFunc(msgFn MsgFunc) AsyncMessageSourceOption {
	return func(ams *AsyncMessageSource) {
		ams.msgFn = msgFn
	}
}

// MsgFunc is a callback function executed by `Run` for each message succesfully consumed.
type MsgFunc func(ctx context.Context, msg substrate.Message) error

// AsyncMessageSource wraps substrate.AsyncMessageSource and provides a smaller and simpler api
// for interaction with the underlying source.
type AsyncMessageSource struct {
	ctx           context.Context
	cancel        context.CancelFunc
	source        substrate.AsyncMessageSource
	msgBufferSize int
	ackBufferSize int
	consumers     int
	msgFn         MsgFunc
}

// NewAsyncMessageSource returns a pointer a new AsyncMessageSource.
// See examples/simple/source/main.go for example usage.
func NewAsyncMessageSource(ctx context.Context, source substrate.AsyncMessageSource, opts ...AsyncMessageSourceOption) *AsyncMessageSource {
	ctx, cancel := context.WithCancel(ctx)

	ams := &AsyncMessageSource{
		ctx:           ctx,
		cancel:        cancel,
		source:        source,
		msgBufferSize: 50,
		ackBufferSize: 1000,
		consumers:     1000,
	}

	for _, opt := range opts {
		opt(ams)
	}

	return ams
}

// Run initialises message consumption using the underlying source and blocks until either an error
// occurs or the constructor context is done. If no MsgFn is configured, Run will return an error.
func (ams *AsyncMessageSource) Run() error {
	group, groupctx := rungroup.New(ams.ctx)

	msgCh := make(chan substrate.Message, ams.msgBufferSize)
	ackCh := make(chan substrate.Message, ams.ackBufferSize)

	group.Go(func() error {
		defer close(msgCh)
		return ams.source.ConsumeMessages(groupctx, msgCh, ackCh)
	})

	for i := 0; i < ams.consumers; i++ {
		group.Go(func() error {
			for msg := range msgCh {
				if ams.msgFn == nil {
					return fmt.Errorf("missing message func")
				}

				err := ams.msgFn(groupctx, msg)
				if err != nil {
					return err
				}

				ackCh <- msg
			}

			return nil
		})
	}

	return group.Wait()
}

// Close permanently closes the underlying source and releases its resources.
func (ams *AsyncMessageSource) Close() error {
	ams.cancel()
	return ams.source.Close()
}

// Status calls the Status method on the underlying source.
func (ams *AsyncMessageSource) Status() (*substrate.Status, error) {
	return ams.source.Status()
}
