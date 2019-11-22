package mock

import (
	"context"

	"github.com/uw-labs/substrate"
)

// NewAsyncMessageSource returns a message source that sends the provided messages to the user.
// Waits for acknowledgements and then terminates. It expects acknowledgements to be in order
// in which the messages were written.
func NewAsyncMessageSource(msgs []substrate.Message) substrate.AsyncMessageSource {
	return mockSource{
		messages: msgs,
	}
}

type mockSource struct {
	messages []substrate.Message
}

func (s mockSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	toWrite, toAck := 0, -1

	checkAck := func(msg substrate.Message) error {
		if toAck == -1 {
			return substrate.InvalidAckError{
				Expected: nil,
				Acked:    msg,
			}
		}
		if msg != s.messages[toAck] {
			return substrate.InvalidAckError{
				Acked:    msg,
				Expected: s.messages[toAck],
			}
		}
		toAck++
		return nil
	}

	for toWrite < len(s.messages) {
		select {
		case <-ctx.Done():
			return nil
		case messages <- s.messages[toWrite]:
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
	for toAck < len(s.messages) {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-acks:
			if err := checkAck(msg); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s mockSource) Close() error {
	return nil
}

func (s mockSource) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}
