package multi_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate-tools/message"
	"github.com/uw-labs/substrate-tools/mock"
	"github.com/uw-labs/substrate-tools/multi"
)

func TestNewMessageSource_Error(t *testing.T) {
	_, err := multi.NewMessageSource(nil)
	require.Equal(t, multi.ErrNoMessageSources, err)
}

func TestMultiMessageSource_ConsumeMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	sources := []substrate.AsyncMessageSource{
		&mock.AsyncMessageSource{
			Messages: []substrate.Message{
				message.FromString("1.1"),
				message.FromString("1.2"),
			},
		},
		&mock.AsyncMessageSource{
			Messages: []substrate.Message{
				message.FromString("2.1"),
				message.FromString("2.2"),
				message.FromString("2.3"),
			},
		},
		&mock.AsyncMessageSource{
			Messages: []substrate.Message{
				message.FromString("3.1"),
				message.FromString("3.2"),
				message.FromString("3.3"),
				message.FromString("3.4"),
			},
		},
	}
	source, err := multi.NewMessageSource(sources)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, source.Close())
	}()

	messages, acks := make(chan substrate.Message), make(chan substrate.Message)
	go func() {
		require.NoError(t, source.ConsumeMessages(ctx, messages, acks))
	}()

	consumed := make([]substrate.Message, 9)
	for i := 0; i < len(consumed); i++ {
		select {
		case <-ctx.Done():
			require.FailNow(t, "failed to consume all messages")
		case consumed[i] = <-messages:
		}
	}
	for i := 0; i < len(consumed); i++ {
		select {
		case <-ctx.Done():
			require.FailNow(t, "failed to acknowledge all messages")
		case acks <- consumed[i]:
		}
	}
}

func TestMultiMessageSource_Close(t *testing.T) {
	sources := []substrate.AsyncMessageSource{
		&mock.AsyncMessageSource{},
		&mock.AsyncMessageSource{},
		&mock.AsyncMessageSource{},
	}
	source, err := multi.NewMessageSource(sources)
	require.NoError(t, err)

	require.NoError(t, source.Close())
	for i := 0; i < len(sources); i++ {
		require.True(t, sources[i].(*mock.AsyncMessageSource).WasClosed())
	}
}

func TestMultiMessageSource_Status(t *testing.T) {
	sources := []substrate.AsyncMessageSource{
		&mock.AsyncMessageSource{},
		&mock.AsyncMessageSource{},
		&mock.AsyncMessageSource{},
		&mock.AsyncMessageSource{},
	}
	source, err := multi.NewMessageSource(sources)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, source.Close())
	}()

	status, err := source.Status()
	require.NoError(t, err)
	require.True(t, status.Working)
	require.Len(t, status.Problems, 0)

	require.NoError(t, sources[0].Close())
	require.NoError(t, sources[1].Close())

	status, err = source.Status()
	require.NoError(t, err)
	require.False(t, status.Working)
	require.Len(t, status.Problems, 2)
}
