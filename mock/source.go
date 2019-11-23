package mock

import (
	"context"
	"errors"
	"sync"

	"github.com/uw-labs/substrate"
)

var _ substrate.AsyncMessageSource = (*AsyncMessageSource)(nil)

// AsyncMessageSource is a message source that sends the provided messages to the user.
// Waits for acknowledgements and then terminates. It expects acknowledgements to be in order
// in which the messages were written.
type AsyncMessageSource struct {
	// Messages that will be sent to the client.
	Messages []substrate.Message

	mutex  sync.RWMutex
	ctx    context.Context
	cancel func()
	closed bool
}

// ConsumeMessages consumes messages sends all messages in the underlying slice to the client, waits for all of them
// to be acknowledged in the correct order and then waits for the provided context to be cancelled. It will error
// in case the source was already closed.
func (s *AsyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	ctx, err := s.init(ctx)
	if err != nil {
		return err
	}

	toWrite, toAck := 0, -1

	checkAck := func(msg substrate.Message) error {
		if toAck == -1 {
			return substrate.InvalidAckError{
				Expected: nil,
				Acked:    msg,
			}
		}
		if msg != s.Messages[toAck] {
			return substrate.InvalidAckError{
				Acked:    msg,
				Expected: s.Messages[toAck],
			}
		}
		toAck++
		return nil
	}

	for toWrite < len(s.Messages) {
		select {
		case <-ctx.Done():
			return nil
		case messages <- s.Messages[toWrite]:
			toWrite++
			if toAck == -1 {
				toAck = 0
			}
		case msg := <-acks:
			if err := checkAck(msg); err != nil {
				return err
			}
		}
	}
	for toAck < len(s.Messages) {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-acks:
			if err := checkAck(msg); err != nil {
				return err
			}
		}
	}

	<-ctx.Done()
	return nil
}

func (s *AsyncMessageSource) init(ctx context.Context) (context.Context, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.ctx != nil {
		return nil, errors.New("source already in use")
	}
	if s.closed {
		return nil, errors.New("source already closed")
	}
	s.ctx, s.cancel = context.WithCancel(ctx)

	return s.ctx, nil
}

// Close closes the message source it will cause the call to ConsumeMessages to return.
func (s *AsyncMessageSource) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// Status returns the status of this message source. It will report not working after the source was closed.
func (s *AsyncMessageSource) Status() (*substrate.Status, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if !s.closed {
		return &substrate.Status{Working: true}, nil
	}

	return &substrate.Status{
		Working:  false,
		Problems: []string{"sink already closed"},
	}, nil
}

// WasClosed indicates whether the close method was called on the message source.
func (s *AsyncMessageSource) WasClosed() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.closed
}
