package cq

import (
	"context"
	"errors"
	"fmt"
)

// ErrorClass represents a normalized error category.
type ErrorClass string

const (
	ErrorClassRetryable ErrorClass = "retryable"
	ErrorClassPermanent ErrorClass = "permanent"
	ErrorClassIgnored   ErrorClass = "ignored"
)

// ErrorClassifier maps a raw error into an ErrorClass.
type ErrorClassifier func(error) ErrorClass

// ClassifiedError wraps an error with classification metadata.
type ClassifiedError struct {
	Class ErrorClass
	Err   error
}

// Error implements the error interface.
func (e *ClassifiedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return string(e.Class)
	}
	return fmt.Sprintf("%s: %v", e.Class, e.Err)
}

// Unwrap returns the underlying error.
func (e *ClassifiedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IsClass reports whether err is a ClassifiedError of the specified class.
func IsClass(err error, class ErrorClass) bool {
	var ce *ClassifiedError
	if errors.As(err, &ce) {
		return ce.Class == class
	}
	return false
}

// WithErrorClassifier classifies job errors and wraps them as ClassifiedError.
// Behavior:
//   - nil job error -> nil
//   - ErrorClassIgnored -> nil
//   - ErrorClassRetryable / ErrorClassPermanent -> ClassifiedError
//   - unknown/empty class -> ClassifiedError with ErrorClassPermanent
//
// classifier is required and will panic if nil.
func WithErrorClassifier(job Job, classifier ErrorClassifier) Job {
	if classifier == nil {
		panic("WithErrorClassifier: classifier is required")
	}

	return func(ctx context.Context) error {
		err := job(ctx)
		if err == nil {
			return nil
		}

		class := classifier(err)
		switch class {
		case ErrorClassIgnored:
			return nil
		case ErrorClassRetryable, ErrorClassPermanent:
			return &ClassifiedError{
				Class: class,
				Err:   err,
			}
		default:
			return &ClassifiedError{
				Class: ErrorClassPermanent,
				Err:   err,
			}
		}
	}
}
