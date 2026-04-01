package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryKeyConcurrencyLimiter(t *testing.T) {
	t.Run("invalid_limit_rejected", func(t *testing.T) {
		limiterZero := NewMemoryKeyConcurrencyLimiter(0)
		if err := limiterZero.Acquire("customer:1"); !errors.Is(err, ErrConcurrencyByKeyInvalidLimit) {
			t.Fatalf("Acquire(): limit=0 got err=%v, want %v", err, ErrConcurrencyByKeyInvalidLimit)
		}

		limiterNegative := NewMemoryKeyConcurrencyLimiter(-1)
		if err := limiterNegative.Acquire("customer:1"); !errors.Is(err, ErrConcurrencyByKeyInvalidLimit) {
			t.Fatalf("Acquire(): limit<0 got err=%v, want %v", err, ErrConcurrencyByKeyInvalidLimit)
		}
	})

}

func TestWithConcurrencyByKey(t *testing.T) {
	t.Run("same_key_is_capped", func(t *testing.T) {
		limiter := NewMemoryKeyConcurrencyLimiter(1)
		key := "customer:123"

		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		var calls atomic.Int32

		job := WithConcurrencyByKey(func(ctx context.Context) error {
			calls.Add(1)
			entered <- struct{}{}
			<-release // Hold first run in-flight so second run can not acquire.
			return nil
		}, key, limiter)

		var wg sync.WaitGroup
		wg.Add(2)

		var firstErr error
		var secondErr error

		go func() {
			defer wg.Done()
			firstErr = job(context.Background())
		}()
		<-entered // Ensure first call acquired slot and is running.

		go func() {
			defer wg.Done()
			secondErr = job(context.Background())
		}()

		time.Sleep(20 * time.Millisecond)
		close(release)
		wg.Wait()

		if firstErr != nil {
			t.Fatalf("first run: got err=%v, want nil", firstErr)
		}
		if !errors.Is(secondErr, ErrConcurrencyByKeyLimited) {
			t.Fatalf("second run: got err=%v, want %v", secondErr, ErrConcurrencyByKeyLimited)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("calls: got %d, want 1", got)
		}
	})

	t.Run("release_happens_on_panic", func(t *testing.T) {
		limiter := NewMemoryKeyConcurrencyLimiter(1)
		key := "customer:panic"

		wrapped := WithConcurrencyByKey(func(ctx context.Context) error {
			panic("boom")
		}, key, limiter)

		func() {
			defer func() { _ = recover() }()
			_ = wrapped(context.Background())
		}()

		// Slot should be available again because defer Release ran.
		if err := limiter.Acquire(key); err != nil {
			t.Fatalf("Acquire(): expected slot to be released after panic, got err=%v", err)
		}
	})
}
