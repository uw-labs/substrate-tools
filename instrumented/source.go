package instrumented

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/uw-labs/substrate"
)

var sourceLabels = []string{"status", "topic", "consumer"}

// NewAsyncMessageSink returns an instance of substrate.AsyncMessageSource that exposes prometheus metrics
// for the message source labelled with topic and status. It panics in case it can't register the metric.
func NewAsyncMessageSource(source substrate.AsyncMessageSource, counterOpts prometheus.CounterOpts, topic, consumer string) substrate.AsyncMessageSource {
	counter := prometheus.NewCounterVec(counterOpts, sourceLabels)

	if err := prometheus.Register(counter); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}
	counter.WithLabelValues("error", topic, consumer).Add(0)
	counter.WithLabelValues("success", topic, consumer).Add(0)

	return &instrumentedSource{
		impl:     source,
		counter:  counter,
		topic:    topic,
		consumer: consumer,
	}
}

// instrumentedSource is an instrumented message source
// The counter vector will have the labels "status", "topic" and "consumer"
type instrumentedSource struct {
	impl     substrate.AsyncMessageSource
	counter  *prometheus.CounterVec
	topic    string
	consumer string
}

// ConsumeMessages implements message consuming wrapped in instrumentation
func (ams *instrumentedSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	toBeAcked := make(chan substrate.Message, cap(acks))

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- ams.impl.ConsumeMessages(ctx, messages, toBeAcked)
	}()

	for {
		select {
		case ack := <-acks:
			select {
			case toBeAcked <- ack:
			case <-ctx.Done():
				return <-errs
			case err := <-errs:
				if err != nil {
					ams.counter.WithLabelValues("error", ams.topic, ams.consumer).Inc()
				}
				return err
			}
			ams.counter.WithLabelValues("success", ams.topic, ams.consumer).Inc()
		case <-ctx.Done():
			return <-errs
		case err := <-errs:
			if err != nil {
				ams.counter.WithLabelValues("error", ams.topic, ams.consumer).Inc()
			}
			return err
		}
	}
}

// Close closes the message source
func (ams *instrumentedSource) Close() error {
	return ams.impl.Close()
}

// Status returns the status of this source, or an error if the status could not be determined.
func (ams *instrumentedSource) Status() (*substrate.Status, error) {
	return ams.impl.Status()
}
