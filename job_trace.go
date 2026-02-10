package cq

import (
	"context"
	"time"
)

// TraceHook defines tracing callbacks for job execution.
type TraceHook interface {
	// Start is called before job execution and may return an enriched context.
	Start(ctx context.Context, name string) context.Context
	// Success is called when a job completes without error.
	Success(ctx context.Context, duration time.Duration)
	// Failure is called when a job completes with error.
	Failure(ctx context.Context, err error, duration time.Duration)
}

// WithTracing wraps a job with start/success/failure tracing callbacks.
// If hook is nil, the original job runs unchanged.
func WithTracing(job Job, name string, hook TraceHook) Job {
	return func(ctx context.Context) error {
		if hook == nil {
			return job(ctx)
		}

		traceCtx := hook.Start(ctx, name)
		start := time.Now()

		err := job(traceCtx)
		dur := time.Since(start)

		if err != nil {
			hook.Failure(traceCtx, err, dur)
			return err
		}

		hook.Success(traceCtx, dur)
		return nil
	}
}
