package cq

import (
	"context"
	"errors"
	"fmt"
)

// DependencyFailMode controls how a failing dependency affects the downstream job.
type DependencyFailMode int

const (
	// DependencyFailCancel stops execution and returns an error wrapping ErrDependencyCancelled.
	DependencyFailCancel DependencyFailMode = iota
	// DependencyFailSkip stops execution and returns AsDiscard(ErrDependencySkipped).
	DependencyFailSkip
	// DependencyFailContinue ignores the failure and proceeds to the next dependency or job.
	DependencyFailContinue
)

// Dependency pairs a Job with a DependencyFailMode.
type Dependency struct {
	job      Job
	failMode DependencyFailMode
}

// Sentinel errors for dependency outcomes.
var (
	// ErrDependencyCancelled is returned when a dependency with DependencyFailCancel mode fails.
	ErrDependencyCancelled = errors.New("cq: dependency cancelled")
	// ErrDependencySkipped is returned (wrapped in AsDiscard) when a dependency with DependencyFailSkip mode fails.
	ErrDependencySkipped = errors.New("cq: dependency skipped")
)

// Dep constructs a Dependency with the given job and fail mode.
func Dep(job Job, failMode DependencyFailMode) Dependency {
	return Dependency{job: job, failMode: failMode}
}

// WithDependsOn wraps job so that each dependency runs sequentially before it,
// with configurable failure behavior per dependency.
func WithDependsOn(job Job, deps ...Dependency) Job {
	return func(ctx context.Context) error {
		for _, dep := range deps {
			err := dep.job(ctx)
			if err == nil {
				continue
			}
			switch dep.failMode {
			case DependencyFailCancel:
				return fmt.Errorf("%w: %w", ErrDependencyCancelled, err)
			case DependencyFailSkip:
				return AsDiscard(ErrDependencySkipped)
			case DependencyFailContinue:
				// Ignore failure... proceed to next dependency.
			}
		}
		return job(ctx)
	}
}
