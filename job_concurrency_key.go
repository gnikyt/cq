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
	Acquire(key string) error
	Release(key string)
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
func (l *MemoryKeyConcurrencyLimiter) Acquire(key string) error {
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
func (l *MemoryKeyConcurrencyLimiter) Release(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	current, ok := l.counts[key]
	if !ok || current <= 0 {
		return // Nothing to release.
	}

	next := current - 1
	if next == 0 {
		delete(l.counts, key) // Cleanup entries.
		return
	}
	l.counts[key] = next
}

// WithConcurrencyByKey caps concurrent executions for a key using limiter.
// When the limit is reached, Acquire returns ErrConcurrencyByKeyLimited and this wrapper returns it.
// Compose with WithDispatchOnContention to hand off contended runs to a JobDispatcher.
func WithConcurrencyByKey(job Job, key string, limiter KeyConcurrencyLimiter) Job {
	return func(ctx context.Context) error {
		if limiter == nil {
			return job(ctx)
		}

		if err := limiter.Acquire(key); err != nil {
			return err
		}
		defer limiter.Release(key)
		return job(ctx)
	}
}
