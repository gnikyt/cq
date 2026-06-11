package cq

import (
	"context"
	"errors"
	"sync"
)

// Keyed concurrency wrapper errors.
var (
	ErrConcurrencyByKeyLimited      = errors.New("cq: concurrency by key limit reached")
	ErrConcurrencyByKeyInvalidLimit = errors.New("cq: concurrency by key invalid limit")
)

// KeyConcurrencyLimiter controls concurrent execution caps per key.
// Implementations may be in-memory or distributed.
type KeyConcurrencyLimiter interface {
	Acquire(ctx context.Context, key string) error
	Release(ctx context.Context, key string) error
}

// MemoryKeyConcurrencyLimiter limits in-flight executions per key in-process.
type MemoryKeyConcurrencyLimiter struct {
	mu     sync.Mutex
	limit  int
	counts map[string]int
}

// NewMemoryKeyConcurrencyLimiter creates an in-memory keyed concurrency limiter.
func NewMemoryKeyConcurrencyLimiter(limit int) *MemoryKeyConcurrencyLimiter {
	return &MemoryKeyConcurrencyLimiter{
		limit:  limit,
		counts: make(map[string]int),
	}
}

// Acquire attempts to reserve one slot for key up to limit.
func (l *MemoryKeyConcurrencyLimiter) Acquire(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.limit <= 0 {
		return ErrConcurrencyByKeyInvalidLimit
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	current := l.counts[key]
	if current >= l.limit {
		return ErrConcurrencyByKeyLimited // At limit.
	}

	l.counts[key] = current + 1
	return nil
}

// Release frees one reserved slot for key.
func (l *MemoryKeyConcurrencyLimiter) Release(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	current, ok := l.counts[key]
	if !ok || current <= 0 {
		return nil // Nothing to release.
	}

	next := current - 1
	if next == 0 {
		delete(l.counts, key) // Cleanup entries.
		return nil
	}
	l.counts[key] = next
	return nil
}

// WithConcurrencyByKey caps concurrent executions for a key using limiter.
// When the limit is reached, Acquire returns ErrConcurrencyByKeyLimited and this wrapper returns it.
// Callers can classify the returned error with IsContentionError.
func WithConcurrencyByKey(job Job, key string, limiter KeyConcurrencyLimiter) Job {
	return func(ctx context.Context) (err error) {
		if limiter == nil {
			return job(ctx)
		}

		if err := limiter.Acquire(ctx, key); err != nil {
			return err
		}
		defer func() {
			err = errors.Join(err, limiter.Release(context.WithoutCancel(ctx), key))
		}()
		return job(ctx)
	}
}
