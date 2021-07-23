package batch_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"

	"github.com/uw-labs/substrate-tools/batch"
	"github.com/uw-labs/substrate-tools/message"
	"github.com/uw-labs/substrate-tools/mock"
)

func TestBatchMessageSource(t *testing.T) {
	t.Parallel()
	tt := []struct {
		Name         string
		Messages     []substrate.Message
		BatchSize    int
		HandlerError error

		ExpectedBatches [][]substrate.Message
		ExpectsError    bool
	}{
		{
			Name: "It should process messages in batches",
			Messages: []substrate.Message{
				message.FromString("message-1"),
				message.FromString("message-2"),
				message.FromString("message-3"),
				message.FromString("message-4"),
			},
			BatchSize: 2,
			ExpectedBatches: [][]substrate.Message{{
				message.FromString("message-1"),
				message.FromString("message-2"),
			}, {
				message.FromString("message-3"),
				message.FromString("message-4"),
			}},
		},
		{
			Name: "It should return errors from handler",
			Messages: []substrate.Message{
				message.FromString("handler-error"),
			},
			BatchSize: 1,
			ExpectedBatches: [][]substrate.Message{{
				message.FromString("handler-error"),
			}},
			ExpectsError: true,
			HandlerError: fmt.Errorf("handler-error"),
		},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			source := batch.NewMessageSource(
				&mock.AsyncMessageSource{Messages: tc.Messages},
				batch.WithBatchSize(tc.BatchSize),
				batch.WithBatchMaxWait(time.Second),
			)

			// Current issue with this test is it cancels the context after 3 seconds
			// since rd.ConsumeMessages will go on forever. We get around this being a problem
			// by running subtests in parallel, so overall test time is capped to 3 seconds.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()

			batchCount := 0
			expectedErr := tc.HandlerError
			if expectedErr == nil {
				expectedErr = context.DeadlineExceeded
			}
			assert.Equal(t, expectedErr, source.ConsumeMessages(ctx, func(ctx context.Context, messages []substrate.Message) error {
				assert.Equal(t, tc.ExpectedBatches[batchCount], messages)
				batchCount++
				return tc.HandlerError
			}))
		})
	}
}
