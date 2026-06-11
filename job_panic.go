package cq

import "fmt"

// PanicOrigin identifies the recovery boundary that caught a panic.
type PanicOrigin string

const (
	// PanicOriginJob identifies a panic recovered around submitted job execution.
	PanicOriginJob PanicOrigin = "job"
	// PanicOriginWorker identifies a panic recovered by the outer worker safety boundary.
	PanicOriginWorker PanicOrigin = "worker"
)

// PanicError records a panic recovered while executing work.
type PanicError struct {
	Value  any
	Origin PanicOrigin
}

// Error implements error.
func (e *PanicError) Error() string {
	return fmt.Sprintf("cq: %s panic: %v", e.Origin, e.Value)
}

// Unwrap returns the recovered value when it implements error.
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}
