package async

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/message"
)

type messageSourceMock struct {
	substrate.AsyncMessageSource
	consumerMessagesMock func(context.Context, chan<- substrate.Message, <-chan substrate.Message) error
}

func (m messageSourceMock) ConsumeMessages(ctx context.Context, in chan<- substrate.Message, acks <-chan substrate.Message) error {
	return m.consumerMessagesMock(ctx, in, acks)
}

func TestConsumeMessagesSuccessfully(t *testing.T) {
	receivedAcks := make(chan substrate.Message)

	source := messageSourceAdapter{
		source: &messageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				messages <- message.FromString("payload")

				for {
					select {
					case <-ctx.Done():
						return nil
					case ack := <-acks:
						receivedAcks <- ack
					}
				}
			},
		},
		consumers: 1,
	}

	sourceContext, sourceCancel := context.WithTimeout(context.Background(), time.Second)
	defer sourceCancel()

	errs := make(chan error)

	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, func(_ context.Context, _ substrate.Message, ack AckFunc) error {
			return ack()
		})
	}()

	for {
		select {
		case err := <-errs:
			assert.NoError(t, err)
			return
		case ack := <-receivedAcks:
			assert.NotEmpty(t, ack.Data())

			sourceCancel()
		}
	}
}

func TestConsumeMessagesSuccessfullyConcurrently(t *testing.T) {
	receivedAcks := make(chan substrate.Message)

	var wg sync.WaitGroup

	source := messageSourceAdapter{
		source: &messageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				for i := 0; i < 100; i++ {
					wg.Add(1)
					messages <- message.FromString("payload")
				}

				for {
					select {
					case <-ctx.Done():
						return nil
					case ack := <-acks:
						receivedAcks <- ack
					}
				}
			},
		},
		consumers: 5,
		msgBuffer: 100,
		ackBuffer: 100,
	}

	sourceContext, sourceCancel := context.WithTimeout(context.Background(), time.Second)
	defer sourceCancel()

	errs := make(chan error)

	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, func(_ context.Context, _ substrate.Message, ack AckFunc) error {
			wg.Done()
			return ack()
		})
	}()

	wg.Wait()

	for {
		select {
		case err := <-errs:
			assert.NoError(t, err)
			return
		case ack := <-receivedAcks:
			assert.NotEmpty(t, ack.Data())
		}
	}
}

func TestConsumeMessagesDoubleAckFuncCall(t *testing.T) {
	receivedAcks := make(chan substrate.Message)

	source := messageSourceAdapter{
		source: &messageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				messages <- message.FromString("payload")

				for {
					select {
					case <-ctx.Done():
						return nil
					case ack := <-acks:
						receivedAcks <- ack
					}
				}
			},
		},
		consumers: 1,
	}

	sourceContext, sourceCancel := context.WithTimeout(context.Background(), time.Second)
	defer sourceCancel()

	errs := make(chan error)

	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, func(_ context.Context, _ substrate.Message, ack AckFunc) error {
			defer sourceCancel()
			err := ack()
			if err != nil {
				return err
			}
			return ack()
		})
	}()
	ackCount := 0
	for {
		select {
		case err := <-errs:
			assert.NoError(t, err)
			return
		case ack := <-receivedAcks:
			ackCount++
			if ackCount == 1 {
				assert.NotEmpty(t, ack.Data())
			} else {
				assert.FailNow(t, "received unexpected ack")
			}
		}
	}
}

func TestConsumeMessagesAckFuncTimeout(t *testing.T) {
	receivedAcks := make(chan substrate.Message)

	source := messageSourceAdapter{
		source: &messageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				messages <- message.FromString("payload")

				select {
				case <-ctx.Done():
					return nil
				default:
					return nil
				}
			},
		},
		consumers: 1,
	}

	sourceContext, sourceCancel := context.WithTimeout(context.Background(), time.Second)
	defer sourceCancel()

	errs := make(chan error)

	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, func(_ context.Context, _ substrate.Message, ack AckFunc) error {
			return ack()
		})
	}()

	for {
		select {
		case err := <-errs:
			assert.Error(t, err)
			return
		case <-receivedAcks:
			require.FailNow(t, "should not ack")
		}
	}
}

func TestConsumeMessagesWithError(t *testing.T) {
	consumingErr := errors.New("consuming error")

	source := messageSourceAdapter{
		source: &messageSourceMock{
			consumerMessagesMock: func(_ context.Context, _ chan<- substrate.Message, _ <-chan substrate.Message) error {
				return consumingErr
			},
		},
		consumers: 1,
	}

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, func(_ context.Context, _ substrate.Message, ack AckFunc) error {
			return ack()
		})
	}()

	err := <-errs
	assert.Error(t, err)
	assert.Equal(t, consumingErr, err)

	sourceCancel()
}

func TestConsumeOnBackendShutdown(t *testing.T) {
	expectedErr := errors.New("shutdown")
	backendCtx, backendCancel := context.WithCancel(context.Background())

	source := messageSourceAdapter{
		source: &messageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				select {
				case <-ctx.Done():
					return nil
				case <-backendCtx.Done():
					return expectedErr
				}
			},
		},
		consumers: 1,
	}

	sourceContext, sourceCancel := context.WithTimeout(context.Background(), time.Second*5)
	defer sourceCancel()

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, func(_ context.Context, _ substrate.Message, ack AckFunc) error {
			return ack()
		})
	}()

	// Shutdown backend
	backendCancel()

	// Check wrapper shuts down properly
	select {
	case <-sourceContext.Done():
		t.Fatalf("Wrapper failed to shutdown.")
	case err := <-errs:
		assert.Equal(t, expectedErr, err)
	}
}
