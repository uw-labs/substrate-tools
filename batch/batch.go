package batch

import (
	"context"
	"io"
	"time"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/rungroup"

	"github.com/uw-labs/substrate-tools/batch/internal/ticker"
)

// Default configuration for the batch message source.
const (
	DefaultBatchSize    = 100
	DefaultBatchMaxWait = 10 * time.Second
)

// ConsumerMessageHandler represents a consumer that processes messages un batches.
type ConsumerMessageHandler func(ctx context.Context, batch []substrate.Message) error

// MessageSource is a wrapper for substrate.AsyncMessageSource that hides the channel
// usage and instead allows the user to consume messages in batches.
type MessageSource interface {
	ConsumeMessages(ctx context.Context, handler ConsumerMessageHandler) error
	io.Closer
	substrate.Statuser
}

// The MessageSourceOption type defines a function that modifies a field of the
// batchMessageSource type
type MessageSourceOption func(r *batchMessageSource)

// WithSize specifies batch size. The default is 100.
func WithSize(batchSize int) MessageSourceOption {
	return func(r *batchMessageSource) {
		r.batchSize = batchSize
	}
}

// WithMaxWait specifies the longest time the source should wait until it gets a full batch.
// After this time passes an incomplete batch will be passed to the callback. The default is 10s.
func WithMaxWait(batchMaxWait time.Duration) MessageSourceOption {
	return func(r *batchMessageSource) {
		r.batchMaxWait = batchMaxWait
	}
}

type batchMessageSource struct {
	source substrate.AsyncMessageSource

	batchMaxWait time.Duration
	batchSize    int
}

// NewMessageSource returns a batch message source,
func NewMessageSource(source substrate.AsyncMessageSource, opts ...MessageSourceOption) MessageSource {
	s := &batchMessageSource{
		source: source,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.batchMaxWait == time.Duration(0) {
		s.batchMaxWait = DefaultBatchMaxWait
	}
	if s.batchSize == 0 {
		s.batchSize = DefaultBatchSize
	}
	return s
}

// ConsumeMessages consumes messages in a batch.
func (r *batchMessageSource) ConsumeMessages(ctx context.Context, handler ConsumerMessageHandler) error {
	rg, ctx := rungroup.New(ctx)

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	rg.Go(func() error {
		return r.handleMessages(ctx, handler, messages, acks)
	})
	rg.Go(func() error {
		return r.source.ConsumeMessages(ctx, messages, acks)
	})
	return rg.Wait()
}

func (r *batchMessageSource) handleMessages(ctx context.Context, handler ConsumerMessageHandler, messages <-chan substrate.Message, acks chan<- substrate.Message) error {
	tick := ticker.New(r.batchMaxWait)
	defer tick.Stop()

	batch := make([]substrate.Message, 0, r.batchSize)

	process := func() error {
		if len(batch) > 0 {
			if err := handler(ctx, batch); err != nil {
				return err
			}
		}

		for _, ack := range batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case acks <- ack:
			}
		}

		batch = make([]substrate.Message, 0, r.batchSize)
		tick.Reset() // Reset the ticker to get correct deadline to receive next batch

		return nil
	}
	putMessage := func(msg substrate.Message) error {
		batch = append(batch, msg)

		if len(batch) == r.batchSize {
			return process()
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messages:
			if err := putMessage(msg); err != nil {
				return err
			}
		case <-tick.C:
			if err := process(); err != nil {
				return err
			}
		}
	}
}

// Status calls the status method on the underlying substrate.AsyncMessageSource
func (r *batchMessageSource) Status() (*substrate.Status, error) {
	return r.source.Status()
}

// Close closes the underlying substrate.AsyncMessageSource.
func (r *batchMessageSource) Close() error {
	return r.source.Close()
}
