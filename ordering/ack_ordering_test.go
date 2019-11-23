package ordering_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uw-labs/substrate"

	"github.com/uw-labs/substrate-tools/message"
	"github.com/uw-labs/substrate-tools/mock"
	"github.com/uw-labs/substrate-tools/ordering"
)

func TestAckOrderingMessageSource(t *testing.T) {
	mockSource := &mock.AsyncMessageSource{
		Messages: []substrate.Message{
			message.FromString("1"),
			message.FromString("2"),
			message.FromString("3"),
			message.FromString("4"),
			message.FromString("5"),
			message.FromString("6"),
			message.FromString("7"),
			message.FromString("8"),
		},
	}

	source := ordering.NewAckOrderingMessageSource(mockSource)
	messages, acks := make(chan substrate.Message), make(chan substrate.Message)

	go func() {
		require.NoError(t, source.ConsumeMessages(context.Background(), messages, acks))
	}()

	consumed := make([]substrate.Message, len(mockSource.Messages))
	for i := 0; i < len(mockSource.Messages); i++ {
		consumed[i] = <-messages
	}
	rand.Shuffle(len(consumed), func(i, j int) {
		consumed[i], consumed[j] = consumed[j], consumed[i]
	})

	for i := 0; i < len(consumed); i++ {
		acks <- consumed[i]
	}

	require.NoError(t, source.Close())
	require.True(t, mockSource.WasClosed())
}
