package ticker

import (
	"sync"
	"time"
)

// ResetableTicker is a ticker implementation that can be reset.
type ResetableTicker struct {
	duration time.Duration
	timer    *time.Timer
	C        <-chan time.Time
	reset    chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
}

// New returns a new instance of a resetable ticker
func New(d time.Duration) *ResetableTicker {
	c := make(chan time.Time)

	t := &ResetableTicker{
		duration: d,
		timer:    time.NewTimer(d),
		C:        c,
		reset:    make(chan struct{}),
		stop:     make(chan struct{}),
	}
	go t.loop(c)

	return t
}

// stopTimer stops the underlying timer and drains its channel if necessary.
func (t *ResetableTicker) stopTimer() {
	if !t.timer.Stop() {
		<-t.timer.C
	}
}

func (t *ResetableTicker) loop(out chan<- time.Time) {
	for {
		select {
		case res := <-t.timer.C:
			// Channel has been drained in this branch,
			// so we don't have to worry about stopping the timer
			select {
			case out <- res:
			case <-t.reset:
			case <-t.stop:
				return
			}
		case <-t.reset:
			t.stopTimer()
		case <-t.stop:
			t.stopTimer()
			return
		}
		t.timer.Reset(t.duration)
	}
}

// Reset will restart the timer. It panics when called after reset.
func (t *ResetableTicker) Reset() {
	select {
	case t.reset <- struct{}{}:
	case <-t.stop:
		panic("reset called after stop")
	}
}

// Stop will stop the ticker.
func (t *ResetableTicker) Stop() {
	t.stopOnce.Do(func() { close(t.stop) })
}
