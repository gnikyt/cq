package cq

import (
	"context"
	"errors"
)

// contentionTryKey marks contexts where wrappers should use try/report semantics instead of block/discard:
// WithoutOverlap makes one acquisition attempt.
// WithUnique / WithUniqueWindow return ErrUniqueContended on duplicate.
type contentionTryKey struct{}

// ContextWithContentionTry marks ctx for contention-try behavior.
//
// Propagation: this flag travels with ctx, so every nested WithoutOverlap,
// WithUnique, and WithUniqueWindow under this ctx switches to try semantics
// (one acquisition attempt / return ErrUniqueContended) instead of their default
// block / discard behavior. WithErrorOnContention sets this automatically.
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
// Compose with WithRelease and IsContentionError to resubmit duplicates
// instead of discarding them.
func WithErrorOnContention(job Job) Job {
	return func(ctx context.Context) error {
		return job(ContextWithContentionTry(ctx))
	}
}
