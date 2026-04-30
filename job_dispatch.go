package cq

import (
	"context"
	"errors"
	"fmt"
)

// ErrDispatcherNilFunc is returned when a nil JobDispatcherFunc is invoked.
var ErrDispatcherNilFunc = errors.New("cq: job dispatcher nil func")

// ErrDispatchRequired is returned when WithDispatchOnContention or WithDispatchOnError
// is used with a nil JobDispatcher.
var ErrDispatchRequired = errors.New("cq: job dispatcher required")

// DispatchErrorKind classifies dispatcher failures.
type DispatchErrorKind string

const (
	// DispatchErrorKindRejected indicates a dispatcher rejected the job.
	DispatchErrorKindRejected DispatchErrorKind = "rejected"
	// DispatchErrorKindUnavailable indicates a dispatcher was unavailable.
	DispatchErrorKindUnavailable DispatchErrorKind = "unavailable"
)

// DispatchReason identifies why a job was dispatched.
type DispatchReason uint8

const (
	// DispatchReasonContention indicates WithDispatchOnContention dispatched after a contention error.
	DispatchReasonContention DispatchReason = iota
	// DispatchReasonError indicates WithDispatchOnError dispatched after a non-nil inner error.
	DispatchReasonError
)

// DispatchError wraps dispatcher errors with a typed kind.
type DispatchError struct {
	Kind DispatchErrorKind
	Err  error
}

// Error returns a human-readable dispatch error.
func (e *DispatchError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err == nil {
		return fmt.Sprintf("dispatch (kind=%s)", e.Kind)
	}
	return fmt.Sprintf("dispatch (kind=%s): %v", e.Kind, e.Err)
}

// Unwrap returns the underlying error.
func (e *DispatchError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// DispatchRequest contains dispatch metadata and the job handoff.
type DispatchRequest struct {
	Key    string
	Job    Job
	Reason DispatchReason
	Err    error
}

// dispatchJob dispatches the job to the dispatcher.
func dispatchJob(ctx context.Context, d JobDispatcher, key string, job Job, reason DispatchReason, cause error) error {
	if d == nil {
		return ErrDispatchRequired
	}
	return d.Dispatch(ctx, DispatchRequest{
		Key:    key,
		Job:    job,
		Reason: reason,
		Err:    cause,
	})
}

// JobDispatcher routes a job elsewhere for handling.
type JobDispatcher interface {
	Dispatch(ctx context.Context, req DispatchRequest) error
}

// JobDispatcherFunc adapts a function to the JobDispatcher interface.
type JobDispatcherFunc func(ctx context.Context, req DispatchRequest) error

// Dispatch calls f(ctx, req).
func (f JobDispatcherFunc) Dispatch(ctx context.Context, req DispatchRequest) error {
	if f == nil {
		return &DispatchError{
			Kind: DispatchErrorKindUnavailable,
			Err:  ErrDispatcherNilFunc,
		}
	}
	return f(ctx, req)
}
