package cq

import (
	"context"
	"errors"
)

// contentionTryKey marks contexts where wrappers should use try/report semantics instead of block/discard:
// WithoutOverlap uses TryLock.
// WithUnique / WithUniqueWindow return ErrUniqueContended on duplicate.
type contentionTryKey struct{}

// ContextWithContentionTry marks ctx for contention-try behavior.
//
// Propagation: this flag travels with ctx, so every nested WithoutOverlap,
// WithUnique, and WithUniqueWindow under this ctx switches to try semantics
// (TryLock / return ErrUniqueContended) instead of their default
// block / discard behavior. WithDispatchOnContention and WithErrorOnContention
// set this automatically.
func ContextWithContentionTry(ctx context.Context) context.Context {
	return context.WithValue(ctx, contentionTryKey{}, struct{}{})
}

// contentionTryFromContext reports whether ctx was produced with ContextWithContentionTry.
func contentionTryFromContext(ctx context.Context) bool {
	_, ok := ctx.Value(contentionTryKey{}).(struct{})
	return ok
}

// IsContentionError reports whether err is ErrUniqueContended,
// ErrWithoutOverlapContended, or ErrConcurrencyByKeyLimited.
func IsContentionError(err error) bool {
	return errors.Is(err, ErrUniqueContended) ||
		errors.Is(err, ErrWithoutOverlapContended) ||
		errors.Is(err, ErrConcurrencyByKeyLimited)
}

// WithErrorOnContention runs the inner job with ContextWithContentionTry and
// returns any contention error (ErrUniqueContended, ErrWithoutOverlapContended,
// ErrConcurrencyByKeyLimited) unchanged so it can be classified by callers.
// Compose with WithRelease and IsContentionError to re-enqueue duplicates
// instead of discarding or dispatching them.
func WithErrorOnContention(job Job) Job {
	return func(ctx context.Context) error {
		return job(ContextWithContentionTry(ctx))
	}
}

// WithDispatchOnContention runs the inner job with ContextWithContentionTry,
// then dispatches on IsContentionError.
func WithDispatchOnContention(job Job, key string, d JobDispatcher) Job {
	return func(ctx context.Context) error {
		err := job(ContextWithContentionTry(ctx))
		if err == nil {
			return nil
		}
		if !IsContentionError(err) {
			return err
		}
		return dispatchJob(ctx, d, key, job, DispatchReasonContention, nil)
	}
}

// WithDispatchOnError dispatches on any inner error. Unlike
// WithDispatchOnContention, this does NOT inject contention-try, so unique /
// overlap wrappers retain their default block / discard behavior and only
// "real" errors from the inner job trigger dispatch.
func WithDispatchOnError(job Job, key string, d JobDispatcher) Job {
	return func(ctx context.Context) error {
		err := job(ctx)
		if err == nil {
			return nil
		}
		return dispatchJob(ctx, d, key, job, DispatchReasonError, err)
	}
}
