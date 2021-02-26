package async

import (
	"context"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"
)

// MessageSourceOption is a function which sets a MessageSource configuration option.
type MessageSourceOption func(msa *messageSourceAdapter)

// WithSourceMsgBufferSize sets the msg channel buffer size. The default value is 0 (unbuffered).
func WithSourceMsgBufferSize(size uint) MessageSourceOption {
	return func(msa *messageSourceAdapter) {
		msa.msgBuffer = size
	}
}

// WithSourceAckBufferSize sets the ack channel buffer size. The default value is 0 (unbuffered).
func WithSourceAckBufferSize(size uint) MessageSourceOption {
	return func(msa *messageSourceAdapter) {
		msa.ackBuffer = size
	}
}

// WithSourceConsumers sets the number of concurrent source consumers. The default value is 1 (synchronous).
// Warning: When using Kafka (and possibly other backends), setting consumers > 1 breaks ordering guarantees
// made possible by producing ordered to a topic using a KeyFunc. If this is something you care about, it's
// reccomended that you set this value to 1 and scale horizontally.
func WithSourceConsumers(consumers uint) MessageSourceOption {
	return func(msa *messageSourceAdapter) {
		msa.consumers = consumers
	}
}

// AckFunc is a function to call to send acknowledgement that a message was received. It should be
// called at least once for each message read.
type AckFunc func() error

// ConsumerMessageHandler is the callback function type that async message consumers must implement.
// The AckFunc should be called at least once for each message read. Multiple calls of the AckFunc
// will do nothing.
type ConsumerMessageHandler func(ctx context.Context, msg substrate.Message, ack AckFunc) error

// MessageSource represents a message source that allows async consumption and relieves the consumer
// from having to deal with acknowledgements using channels.
type MessageSource interface {
	// Close closed the MessageSource, freeing underlying resources.
	Close() error
	// ConsumeMessages calls the handler function for each message available to consume. An
	// acknowledgement will only be sent to the broker when the handler calls the AckFunc provided.
	// If an error is returned by the handler, it will be propogated and returned from this
	// function. This function will block until the context is done or until an error occurs.
	ConsumeMessages(ctx context.Context, handler ConsumerMessageHandler) error
	substrate.Statuser
}

// NewMessageSource returns a new insurance asynchronous message source, given
// an AsyncMessageSource. When Close is called on the AsyncMessageSource,
// this is also propogated to the underlying AsyncMessageSource.
func NewMessageSource(source substrate.AsyncMessageSource, opts ...MessageSourceOption) MessageSource {
	msa := &messageSourceAdapter{
		source:    source,
		consumers: 1,
	}

	for _, opt := range opts {
		opt(msa)
	}

	return msa
}

type messageSourceAdapter struct {
	source    substrate.AsyncMessageSource
	msgBuffer uint
	ackBuffer uint
	consumers uint
}

func (a *messageSourceAdapter) ConsumeMessages(ctx context.Context, handler ConsumerMessageHandler) error {
	rg, ctx := rungroup.New(ctx)

	messages := make(chan substrate.Message, a.msgBuffer)
	acks := make(chan substrate.Message, a.ackBuffer)

	rg.Go(func() error {
		return a.source.ConsumeMessages(ctx, messages, acks)
	})

	for i := uint(0); i < a.consumers; i++ {
		rg.Go(func() error {
			for {
				select {
				case msg := <-messages:
					acked := false
					ackFunc := func() error {
						if acked {
							return nil
						}
						select {
						case acks <- msg:
							acked = true
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					if err := handler(ctx, msg, ackFunc); err != nil {
						return err
					}
				case <-ctx.Done():
					return nil
				}
			}
		})
	}

	return rg.Wait()
}

func (a *messageSourceAdapter) Close() error {
	return a.source.Close()
}

func (a *messageSourceAdapter) Status() (*substrate.Status, error) {
	return a.source.Status()
}
