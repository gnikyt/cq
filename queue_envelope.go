package cq

import (
	"fmt"
	"time"
)

// envelopePreparedJob is a prepared job with its envelope.
type envelopePreparedJob struct {
	job      Job
	envelope *Envelope
}

// envelopePreparedJobs prepares a batch of jobs with their metadata.
// It validates the handler and payloads, encodes the payloads,
// and returns a slice of prepared jobs with their metadata.
// This is used for batch enqueueing of envelopes.
func envelopePreparedJobs[T any](handler EnvelopeHandler[T], payloads []T) ([]envelopePreparedJob, error) {
	prepared := make([]envelopePreparedJob, 0, len(payloads))
	for i, payload := range payloads {
		job, envelope, err := envelopeJobAndMetadata(handler, payload)
		if err != nil {
			return nil, fmt.Errorf("cq: prepare envelope batch (item=%d): %w", i, err)
		}
		prepared = append(prepared, envelopePreparedJob{
			job:      job,
			envelope: envelope,
		})
	}
	return prepared, nil
}

// EnqueueEnvelope submits a typed envelope handler and payload to the queue.
// Envelope type/payload are persisted at enqueue time when an envelope store is configured.
func EnqueueEnvelope[T any](q *Queue, handler EnvelopeHandler[T], payload T) error {
	job, envelope, err := envelopeJobAndMetadata(handler, payload)
	if err != nil {
		return err
	}
	q.doEnqueue(job, enqueueOptions{blocking: true, envelope: envelope})
	return nil
}

// TryEnqueueEnvelope attempts to submit a typed envelope handler and payload without blocking.
// Returns true when accepted, false when queue is full and no new worker can be started.
func TryEnqueueEnvelope[T any](q *Queue, handler EnvelopeHandler[T], payload T) (bool, error) {
	job, envelope, err := envelopeJobAndMetadata(handler, payload)
	if err != nil {
		return false, err
	}
	return q.doEnqueue(job, enqueueOptions{blocking: false, envelope: envelope}), nil
}

// DelayEnqueueEnvelope submits a typed envelope handler and payload after the given delay.
func DelayEnqueueEnvelope[T any](q *Queue, handler EnvelopeHandler[T], payload T, delay time.Duration) error {
	job, envelope, err := envelopeJobAndMetadata(handler, payload)
	if err != nil {
		return err
	}
	q.doDelayEnqueue(job, delay, enqueueOptions{blocking: true, envelope: envelope})
	return nil
}

// EnqueueEnvelopeBatch submits a batch of typed envelope payloads for one handler.
// The batch is pre-validated before enqueueing so encoding errors fail fast.
func EnqueueEnvelopeBatch[T any](q *Queue, handler EnvelopeHandler[T], payloads []T) error {
	prepared, err := envelopePreparedJobs(handler, payloads)
	if err != nil {
		return err
	}
	for _, item := range prepared {
		q.doEnqueue(item.job, enqueueOptions{blocking: true, envelope: item.envelope})
	}
	return nil
}

// DelayEnqueueEnvelopeBatch submits a batch of typed envelope payloads after delay.
// The batch is pre-validated before scheduling so encoding errors fail fast.
func DelayEnqueueEnvelopeBatch[T any](q *Queue, handler EnvelopeHandler[T], payloads []T, delay time.Duration) error {
	prepared, err := envelopePreparedJobs(handler, payloads)
	if err != nil {
		return err
	}

	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()

		select {
		case <-q.ctx.Done():
			return // Context is done, stop the timer.
		case <-timer.C:
			for _, item := range prepared {
				q.doEnqueue(item.job, enqueueOptions{blocking: true, envelope: item.envelope})
			}
		}
	}()
	return nil
}

// TryEnqueueEnvelopeBatch attempts to submit a batch without blocking.
// It returns how many jobs were accepted before the queue became full.
func TryEnqueueEnvelopeBatch[T any](q *Queue, handler EnvelopeHandler[T], payloads []T) (int, error) {
	prepared, err := envelopePreparedJobs(handler, payloads)
	if err != nil {
		return 0, err
	}

	accepted := 0
	for _, item := range prepared {
		if ok := q.doEnqueue(item.job, enqueueOptions{blocking: false, envelope: item.envelope}); !ok {
			break
		}
		accepted++
	}
	return accepted, nil
}
