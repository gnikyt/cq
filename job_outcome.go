package cq

import (
	"context"
	"errors"
	"fmt"
)

// Sentinel errors for job outcome.
var (
	// ErrRetryable is returned (or wrapped) when a job should be treated as
	// retryable: eligible for retry according to retry policy.
	ErrRetryable = errors.New("cq: retryable")

	// ErrDiscard is returned (or wrapped) when a job should be treated as
	// discarded: no retry, not counted as failed.
	ErrDiscard = errors.New("cq: discard")

	// ErrPermanent is returned (or wrapped) when a job should be treated as
	// permanently failed: no retry, count as failed.
	ErrPermanent = errors.New("cq: permanent")
)

// Kept for future/internal predicate helpers.
var _ = isRetryable

// outcomeError wraps an error with an outcome marker.
type outcomeError struct {
	outcome error // ErrRetryable, ErrDiscard, or ErrPermanent.
	err     error
}

// Error implements the error interface.
func (e *outcomeError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("%s: %v", e.outcome, e.err)
	}
	return e.outcome.Error()
}

// Unwrap implements the error interface.
func (e *outcomeError) Unwrap() error {
	return e.err
}

// Is implements the error interface.
func (e *outcomeError) Is(target error) bool {
	return e.outcome == target
}

// AsRetryable wraps err so that errors.Is(..., ErrRetryable) is true.
// The wrapped error is returned by errors.Unwrap.
func AsRetryable(err error) error {
	return &outcomeError{outcome: ErrRetryable, err: err}
}

// AsDiscard wraps err so that errors.Is(..., ErrDiscard) is true.
// The wrapped error is returned by errors.Unwrap. If err is nil,
// the outcome is still discard (e.g. "no work to do").
func AsDiscard(err error) error {
	return &outcomeError{outcome: ErrDiscard, err: err}
}

// AsPermanent wraps err so that errors.Is(..., ErrPermanent) is true.
// The wrapped error is returned by errors.Unwrap. Stops retries and
// counts as failure.
func AsPermanent(err error) error {
	return &outcomeError{outcome: ErrPermanent, err: err}
}

// isRetryable returns true if err is a retryable error.
func isRetryable(err error) bool {
	return errors.Is(err, ErrRetryable)
}

// isDiscarded returns true if err is a discarded error.
func isDiscarded(err error) bool {
	return errors.Is(err, ErrDiscard)
}

// isPermanent returns true if err is a permanent error.
func isPermanent(err error) bool {
	return errors.Is(err, ErrPermanent)
}

// WithOutcome invokes optional callbacks based on job outcome.
// If the job returns ErrDiscard (errors.Is(err, ErrDiscard)), onDiscarded is
// called and WithOutcome returns nil so the job is not counted as failed.
// Otherwise a non-nil error triggers onFailed and is returned.
// Then, if onCompleted is not nil, it is called and nil is returned.
func WithOutcome(
	job Job,
	onCompleted func(),
	onFailed func(error),
	onDiscarded func(error),
) Job {
	return func(ctx context.Context) error {
		err := job(ctx)
		if err == nil {
			if onCompleted != nil {
				onCompleted()
			}
			return nil
		}
		if errors.Is(err, ErrDiscard) {
			if onDiscarded != nil {
				onDiscarded(errors.Unwrap(err))
			}
			return nil
		}
		if onFailed != nil {
			onFailed(err)
		}
		return err
	}
}

// WithResultHandler invokes callbacks on job success or failure.
// onCompleted runs after a nil error.
// onFailed runs with the returned error.
//
// Deprecated: use WithOutcome instead. WithResultHandler
// is implemented by calling WithOutcome and will be removed in a future version.
func WithResultHandler(job Job, onCompleted func(), onFailed func(error)) Job {
	return WithOutcome(job, onCompleted, onFailed, nil)
}
