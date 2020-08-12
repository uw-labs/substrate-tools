package instrumented

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/uw-labs/substrate"
)

var sinkLabels = []string{"status", "topic"}

// NewAsyncMessageSink returns an instance of substrate.AsyncMessageSink that  exposes prometheus metrics
// for the message sink labelled with topic and status. It panics in case it can't register the metric.
func NewAsyncMessageSink(sink substrate.AsyncMessageSink, counterOpts prometheus.CounterOpts, topic string) substrate.AsyncMessageSink {
	counter := prometheus.NewCounterVec(counterOpts, sinkLabels)

	if err := prometheus.Register(counter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}
	counter.WithLabelValues("error", topic).Add(0)
	counter.WithLabelValues("success", topic).Add(0)

	return &instrumentedSink{
		impl:    sink,
		counter: counter,
		topic:   topic,
	}
}

// instrumentedSink is an instrumented message sink
// The counter vector will have the labels "status" and "topic"
type instrumentedSink struct {
	impl    substrate.AsyncMessageSink
	counter *prometheus.CounterVec
	topic   string
}

// PublishMessages implements message publishing wrapped in instrumentation.
func (ams *instrumentedSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) (rerr error) {
	successes := make(chan substrate.Message, cap(acks))

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- ams.impl.PublishMessages(ctx, successes, messages)
	}()

	for {
		select {
		case success := <-successes:
			ams.counter.WithLabelValues("success", ams.topic).Inc()
			select {
			case acks <- success:
			case <-ctx.Done():
				return <-errs
			case err := <-errs:
				if isUnexpectedError(err) {
					ams.counter.WithLabelValues("error", ams.topic).Inc()
				}
				return err
			}
		case <-ctx.Done():
			return <-errs
		case err := <-errs:
			if isUnexpectedError(err) {
				ams.counter.WithLabelValues("error", ams.topic).Inc()
			}
			return err
		}
	}
}

func isUnexpectedError(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, context.Canceled):
		// we're expecting producers to mark the stopping of producing by cancelling the context
		return false
	default:
		return true
	}
}

// Close closes the message sink.
func (ams *instrumentedSink) Close() error {
	return ams.impl.Close()
}

// Status returns the status of this sink, or an error if the status could not be determined.
func (ams *instrumentedSink) Status() (*substrate.Status, error) {
	return ams.impl.Status()
}
