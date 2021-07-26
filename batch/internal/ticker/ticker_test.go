package ticker_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uw-labs/substrate-tools/batch/internal/ticker"
)

const duration = time.Millisecond * 50

func TestTicker(t *testing.T) {
	assert := require.New(t)

	prev := time.Now()
	tick := ticker.New(duration)
	defer tick.Stop()

	for i := 0; i < 10; i++ {
		cur := <-tick.C
		actual := cur.Sub(prev)
		prev = cur

		assert.Truef(actual >= duration, "time gap too short: %v expected:%v", actual, duration)
	}
}

func TestTicker_Reset(t *testing.T) {
	assert := require.New(t)

	tick := ticker.New(duration)
	defer tick.Stop()

	start := <-tick.C
	time.Sleep(duration * 2)
	tick.Reset()
	end := <-tick.C

	actual, expected := end.Sub(start), 3*duration
	assert.Truef(actual >= 3*duration, "reset failed: time gap too short: %v expected:%v", actual, expected)
}
