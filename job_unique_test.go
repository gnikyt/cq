package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type acquireFailLocker struct {
	acquireCalls atomic.Int32
}

type overlapRetryLocker struct {
	acquireTimes chan time.Time
	attempts     atomic.Int32
}

type nonRenewableLocker struct {
	mu    sync.Mutex
	locks map[string]LockValue[struct{}]
}

func newNonRenewableLocker() *nonRenewableLocker {
	return &nonRenewableLocker{
		locks: make(map[string]LockValue[struct{}]),
	}
}

func (l *nonRenewableLocker) Get(_ context.Context, key string) (LockValue[struct{}], bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lock, ok := l.locks[key]
	return lock, ok, nil
}

func (l *nonRenewableLocker) Acquire(_ context.Context, key string, lock LockValue[struct{}]) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.locks[key]; ok && !existing.IsExpired() {
		return false, nil
	}
	l.locks[key] = lock
	return true, nil
}

func (l *nonRenewableLocker) Release(_ context.Context, key string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.locks[key]; !ok {
		return false, nil
	}
	delete(l.locks, key)
	return true, nil
}

func (l *acquireFailLocker) Get(_ context.Context, _ string) (LockValue[struct{}], bool, error) {
	return LockValue[struct{}]{}, false, nil
}

func (l *acquireFailLocker) Acquire(_ context.Context, _ string, _ LockValue[struct{}]) (bool, error) {
	l.acquireCalls.Add(1)
	return false, nil
}

func (l *acquireFailLocker) Release(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (l *overlapRetryLocker) Get(_ context.Context, _ string) (LockValue[struct{}], bool, error) {
	return LockValue[struct{}]{}, false, nil
}

func (l *overlapRetryLocker) Acquire(_ context.Context, _ string, _ LockValue[struct{}]) (bool, error) {
	l.acquireTimes <- time.Now()
	return l.attempts.Add(1) >= 2, nil
}

func (l *overlapRetryLocker) Release(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func TestWithoutOverlap(t *testing.T) {
	var wg sync.WaitGroup
	locker := NewOverlapMemoryLocker()
	runs := 25

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	job := WithoutOverlap(func(ctx context.Context) error {
		defer wg.Done()

		cur := concurrent.Add(1)
		for {
			prev := maxConcurrent.Load()
			if cur <= prev || maxConcurrent.CompareAndSwap(prev, cur) {
				break
			}
		}

		time.Sleep(5 * time.Millisecond)
		concurrent.Add(-1)
		return nil
	}, "jobo", locker)

	wg.Add(runs)
	for range runs {
		go job(context.Background())
	}
	wg.Wait()

	if got := maxConcurrent.Load(); got != 1 {
		t.Errorf("WithoutOverlap: max concurrent got %d, want 1", got)
	}

	t.Run("waiting_respects_context_cancellation", func(t *testing.T) {
		locker := NewOverlapMemoryLocker()
		started := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		first := WithoutOverlap(func(context.Context) error {
			close(started)
			<-release
			return nil
		}, "jobo", locker)
		go func() {
			done <- first(context.Background())
		}()
		<-started

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := WithoutOverlap(func(context.Context) error {
			t.Fatal("cancelled waiting job executed")
			return nil
		}, "jobo", locker)(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WithoutOverlap(): got %v, want %v", err, context.Canceled)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("first WithoutOverlap(): %v", err)
		}
	})

	t.Run("retry_interval_option_controls_wait", func(t *testing.T) {
		locker := &overlapRetryLocker{acquireTimes: make(chan time.Time, 2)}
		interval := 40 * time.Millisecond
		job := WithoutOverlap(
			func(context.Context) error { return nil },
			"jobo",
			locker,
			WithOverlapRetryInterval(interval),
		)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithoutOverlap(): %v", err)
		}
		first := <-locker.acquireTimes
		second := <-locker.acquireTimes
		if elapsed := second.Sub(first); elapsed < interval {
			t.Fatalf("retry interval: got %v, want at least %v", elapsed, interval)
		}
	})

	t.Run("non_positive_retry_interval_uses_default", func(t *testing.T) {
		cfg := resolveOverlapOptions([]OverlapOption{WithOverlapRetryInterval(0)})
		if cfg.retryInterval != defaultOverlapRetryTick {
			t.Fatalf("retry interval: got %v, want %v", cfg.retryInterval, defaultOverlapRetryTick)
		}
	})

}

func TestWithUnique(t *testing.T) {
	t.Run("bare_duplicate_returns_nil", func(t *testing.T) {
		locker := NewUniqueMemoryLocker()
		started := make(chan struct{})
		go WithUnique(func(ctx context.Context) error {
			close(started)
			time.Sleep(50 * time.Millisecond)
			return nil
		}, "test", 0, locker)(context.Background())
		<-started
		time.Sleep(5 * time.Millisecond)
		err := WithUnique(func(ctx context.Context) error {
			t.Fatal("inner job should not run")
			return nil
		}, "test", 0, locker)(context.Background())
		if err != nil {
			t.Fatalf("got %v, want nil when duplicate discarded", err)
		}
	})

	t.Run("normal", func(tt *testing.T) {
		var called atomic.Bool
		locker := NewUniqueMemoryLocker()

		go WithUnique(func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			called.Store(true)
			return nil
		}, "test", 1*time.Minute, locker)(context.Background())

		// Allow goroutine to run.
		time.Sleep(10 * time.Millisecond)

		// This job should not fire since the uniqueness of initial
		// job is set to 1m, and the "work" is taking 50ms.
		go WithUnique(func(ctx context.Context) error {
			t.Error("WithUnique(): job should not fire")
			return nil
		}, "test", 1*time.Minute, locker)(context.Background())

		time.Sleep(60 * time.Millisecond)
		if !called.Load() {
			t.Error("WithUnique(): job should have been called")
		}
	})

	t.Run("expired", func(t *testing.T) {
		var calls atomic.Int32
		locker := NewUniqueMemoryLocker()

		// The lock on this job should be released since it
		// expires 50ms from now, but job takes 500ms.
		go WithUnique(func(ctx context.Context) error {
			time.Sleep(500 * time.Millisecond)
			calls.Add(1)
			return nil
		}, "test", 50*time.Millisecond, locker)(context.Background())

		// Allow goroutine to start and acquire lock.
		time.Sleep(20 * time.Millisecond)

		// Wait for lock to expire.
		time.Sleep(50 * time.Millisecond)

		// One of these jobs should run because lock expired. The other is
		// deduplicated by WithUnique(..., 0, ...) while the first one is running.
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				WithUnique(func(ctx context.Context) error {
					// Keep lock briefly so concurrent duplicate is deduped deterministically.
					time.Sleep(20 * time.Millisecond)
					calls.Add(1)
					return nil
				}, "test", 0, locker)(context.Background())
			}()
		}

		wg.Wait()

		// Only count the post-expiry runs (first long job still running).
		// Depending on scheduler timing, one or both post-expiry attempts may run.
		if got := calls.Load(); got < 1 || got > 2 {
			t.Errorf("WithUnique(): got %d calls, want 1..2", got)
		}
	})

	t.Run("manual_touch_keeps_lock_alive_during_long_run", func(t *testing.T) {
		locker := NewUniqueMemoryLocker()
		window := 120 * time.Millisecond
		started := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)

		go func() {
			done <- WithUnique(func(ctx context.Context) error {
				close(started)
				if err := TouchLock(ctx, window); err != nil {
					t.Errorf("TouchLock(): got %v, want nil with renewable locker", err)
				}
				time.Sleep(75 * time.Millisecond)
				if err := TouchLock(ctx, window); err != nil {
					t.Errorf("TouchLock(): got %v, want nil on subsequent touch", err)
				}
				time.Sleep(75 * time.Millisecond)
				<-release
				return nil
			}, "test-touch-unique", window, locker)(context.Background())
		}()

		<-started
		time.Sleep(170 * time.Millisecond) // Past initial window but renewed.

		err := WithUnique(func(ctx context.Context) error {
			t.Error("WithUnique(): duplicate should remain blocked while manual touch is active")
			return nil
		}, "test-touch-unique", window, locker)(ContextWithContentionTry(context.Background()))
		if !errors.Is(err, ErrUniqueContended) {
			t.Fatalf("WithUnique(): got %v, want %v while first run active", err, ErrUniqueContended)
		}

		close(release)
		if err := <-done; err != nil {
			t.Fatalf("WithUnique(): first run got %v, want nil", err)
		}
	})

	t.Run("zero_duration", func(t *testing.T) {
		var called atomic.Bool
		locker := NewUniqueMemoryLocker()

		// Zero duration means lock doesn't expire until job completes.
		go WithUnique(func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			called.Store(true)
			return nil
		}, "test", time.Duration(0), locker)(context.Background())

		// Allow goroutine to run.
		time.Sleep(10 * time.Millisecond)

		// This job should not fire since the lock doesn't expire (zero duration).
		go WithUnique(func(ctx context.Context) error {
			t.Error("WithUnique(): job should not fire with zero duration lock")
			return nil
		}, "test", time.Duration(0), locker)(context.Background())

		time.Sleep(60 * time.Millisecond)
		if !called.Load() {
			t.Error("WithUnique(): job should have been called")
		}
	})

	t.Run("touch_unavailable_without_renewable_unique_context", func(t *testing.T) {
		locker := newNonRenewableLocker()
		err := WithUnique(func(ctx context.Context) error {
			return TouchLock(ctx, 10*time.Millisecond)
		}, "test-non-renewable-touch", 50*time.Millisecond, locker)(context.Background())
		if !errors.Is(err, ErrTouchLockUnavailable) {
			t.Fatalf("WithUnique(): got %v, want %v", err, ErrTouchLockUnavailable)
		}
	})

	t.Run("release_duplicate", func(t *testing.T) {
		lockedErr := errors.New("unique locked")
		var calls atomic.Int32
		locker := NewUniqueMemoryLocker()
		queue := NewQueue(1, 1, 10)
		queue.Start()
		defer queue.Stop(true)

		uniqueInner := WithUnique(func(ctx context.Context) error {
			if calls.Add(1) == 1 {
				time.Sleep(30 * time.Millisecond)
			}
			return nil
		}, "test", 0, locker)
		job := WithRelease(
			func(ctx context.Context) error {
				err := uniqueInner(ContextWithContentionTry(ctx))
				if errors.Is(err, ErrUniqueContended) {
					return lockedErr
				}
				return err
			},
			queue, 40*time.Millisecond, 1, func(err error) bool {
				return errors.Is(err, lockedErr)
			},
		)

		go job(context.Background())
		time.Sleep(10 * time.Millisecond)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithUnique(): got %v, want nil on release", err)
		}

		deadline := time.After(200 * time.Millisecond)
		for calls.Load() < 2 {
			select {
			case <-deadline:
				t.Fatalf("WithUnique(): got %d calls, want released duplicate to run", calls.Load())
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
	})

	t.Run("contention_try_duplicate_returns_unique_contended", func(t *testing.T) {
		locker := NewUniqueMemoryLocker()

		go WithUnique(func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		}, "test", 0, locker)(context.Background())

		time.Sleep(10 * time.Millisecond)

		err := WithUnique(func(ctx context.Context) error {
			t.Error("WithUnique(): duplicate should not run")
			return nil
		}, "test", 0, locker)(ContextWithContentionTry(context.Background()))
		if !errors.Is(err, ErrUniqueContended) {
			t.Fatalf("WithUnique(): got %v, want %v", err, ErrUniqueContended)
		}
	})

	t.Run("acquire_fail_returns_unique_contended_with_contention_try", func(t *testing.T) {
		locker := &acquireFailLocker{}

		err := WithUnique(func(ctx context.Context) error {
			t.Error("WithUnique(): job should not run when acquire fails")
			return nil
		}, "test", time.Minute, locker)(ContextWithContentionTry(context.Background()))

		if !errors.Is(err, ErrUniqueContended) {
			t.Fatalf("WithUnique(): got %v, want %v on acquire fail", err, ErrUniqueContended)
		}
		if got := locker.acquireCalls.Load(); got != 1 {
			t.Fatalf("WithUnique(): acquire calls got %d, want 1", got)
		}
	})

}

func TestWithUniqueWindow(t *testing.T) {
	t.Run("custom_token_generator_is_used", func(t *testing.T) {
		locker := NewUniqueMemoryLocker()
		window := 200 * time.Millisecond
		started := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)

		go func() {
			done <- WithUniqueWindow(func(ctx context.Context) error {
				close(started)
				<-release
				return nil
			}, "test-custom-token", window, locker, WithUniqueTokenGenerator(func() string {
				return "custom-token"
			}))(context.Background())
		}()

		<-started
		lock, ok, err := locker.Get(context.Background(), "test-custom-token")
		if err != nil {
			t.Fatalf("Get(): %v", err)
		}
		if !ok {
			t.Fatal("WithUniqueWindow(): expected lock to exist while job is running")
		}
		if lock.Token != "custom-token" {
			t.Fatalf("WithUniqueWindow(): got token %q, want %q", lock.Token, "custom-token")
		}

		close(release)
		if err := <-done; err != nil {
			t.Fatalf("WithUniqueWindow(): first run got %v, want nil", err)
		}
	})

	t.Run("empty_custom_token_falls_back_to_default_generator", func(t *testing.T) {
		locker := NewUniqueMemoryLocker()
		window := 200 * time.Millisecond
		started := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)

		go func() {
			done <- WithUniqueWindow(func(ctx context.Context) error {
				close(started)
				<-release
				return nil
			}, "test-empty-token-fallback", window, locker, WithUniqueTokenGenerator(func() string {
				return ""
			}))(context.Background())
		}()

		<-started
		lock, ok, err := locker.Get(context.Background(), "test-empty-token-fallback")
		if err != nil {
			t.Fatalf("Get(): %v", err)
		}
		if !ok {
			t.Fatal("WithUniqueWindow(): expected lock to exist while job is running")
		}
		if lock.Token == "" {
			t.Fatal("WithUniqueWindow(): expected fallback token to be non-empty")
		}

		close(release)
		if err := <-done; err != nil {
			t.Fatalf("WithUniqueWindow(): first run got %v, want nil", err)
		}
	})

	t.Run("manual_touch_keeps_lock_alive_during_long_run", func(t *testing.T) {
		locker := NewUniqueMemoryLocker()
		window := 120 * time.Millisecond
		started := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)

		go func() {
			done <- WithUniqueWindow(func(ctx context.Context) error {
				close(started)
				if err := TouchLock(ctx, window); err != nil {
					t.Errorf("TouchLock(): got %v, want nil with renewable locker", err)
				}
				time.Sleep(75 * time.Millisecond)
				if err := TouchLock(ctx, window); err != nil {
					t.Errorf("TouchLock(): got %v, want nil on subsequent touch", err)
				}
				time.Sleep(75 * time.Millisecond)
				<-release
				return nil
			}, "test-heartbeat", window, locker)(context.Background())
		}()

		<-started
		time.Sleep(170 * time.Millisecond) // Past initial window but renewed.

		err := WithUniqueWindow(func(ctx context.Context) error {
			t.Error("WithUniqueWindow(): duplicate should remain blocked while manual touch is active")
			return nil
		}, "test-heartbeat", window, locker)(ContextWithContentionTry(context.Background()))
		if !errors.Is(err, ErrUniqueContended) {
			t.Fatalf("WithUniqueWindow(): got %v, want %v while first run active", err, ErrUniqueContended)
		}

		close(release)
		if err := <-done; err != nil {
			t.Fatalf("WithUniqueWindow(): first run got %v, want nil", err)
		}
	})

	t.Run("renewable_locker_does_not_auto_touch", func(t *testing.T) {
		locker := NewUniqueMemoryLocker()
		window := 80 * time.Millisecond
		started := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32

		go func() {
			_ = WithUniqueWindow(func(ctx context.Context) error {
				if calls.Add(1) == 1 {
					close(started)
					<-release
				}
				return nil
			}, "test-renewable-no-auto", window, locker)(context.Background())
		}()

		<-started
		time.Sleep(120 * time.Millisecond) // Past initial window.

		err := WithUniqueWindow(func(ctx context.Context) error {
			calls.Add(1)
			return nil
		}, "test-renewable-no-auto", window, locker)(ContextWithContentionTry(context.Background()))
		if err != nil {
			t.Fatalf("WithUniqueWindow(): got %v, want nil when window expires without manual touch", err)
		}

		if got := calls.Load(); got < 2 {
			t.Fatalf("WithUniqueWindow(): got %d calls, want second run after window expiry", got)
		}

		close(release)
	})

	t.Run("non_renewable_locker_keeps_original_expiry_behavior", func(t *testing.T) {
		locker := newNonRenewableLocker()
		window := 80 * time.Millisecond
		started := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32

		go func() {
			_ = WithUniqueWindow(func(ctx context.Context) error {
				if calls.Add(1) == 1 {
					close(started)
					<-release
				}
				return nil
			}, "test-non-renewable", window, locker)(context.Background())
		}()

		<-started
		time.Sleep(120 * time.Millisecond) // Past initial window.

		err := WithUniqueWindow(func(ctx context.Context) error {
			calls.Add(1)
			return nil
		}, "test-non-renewable", window, locker)(ContextWithContentionTry(context.Background()))
		if err != nil {
			t.Fatalf("WithUniqueWindow(): got %v, want nil when window has expired on non-renewable locker", err)
		}

		if got := calls.Load(); got < 2 {
			t.Fatalf("WithUniqueWindow(): got %d calls, want second run after window expiry", got)
		}

		close(release)
	})

	t.Run("touch_lock_returns_unavailable_without_touch_context", func(t *testing.T) {
		err := TouchLock(context.Background(), 10*time.Millisecond)
		if !errors.Is(err, ErrTouchLockUnavailable) {
			t.Fatalf("TouchLock(): got %v, want %v", err, ErrTouchLockUnavailable)
		}
	})

	t.Run("lock_persists_after_job_completes", func(t *testing.T) {
		var calls int
		var mu sync.Mutex
		locker := NewUniqueMemoryLocker()
		window := 100 * time.Millisecond

		// First job completes quickly (10ms).
		err := WithUniqueWindow(func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		}, "test", window, locker)(context.Background())

		if err != nil {
			t.Errorf("WithUniqueWindow(): got %v, want nil (first job)", err)
		}

		// Job completed, but lock should still be active.
		// Try to run duplicate immediately after completion.
		err = WithUniqueWindow(func(ctx context.Context) error {
			t.Error("WithUniqueWindow(): duplicate should be blocked even after job completes")
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		}, "test", window, locker)(context.Background())
		if err != nil {
			t.Errorf("WithUniqueWindow(): got %v, want nil (duplicate job discarded)", err)
		}

		mu.Lock()
		if calls != 1 {
			t.Errorf("WithUniqueWindow(): got %d calls, want 1 (duplicate should be discarded)", calls)
		}
		mu.Unlock()

		// Wait for window to expire.
		time.Sleep(110 * time.Millisecond)

		// Now should be able to run again.
		err = WithUniqueWindow(func(ctx context.Context) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		}, "test", window, locker)(context.Background())
		if err != nil {
			t.Errorf("WithUniqueWindow(): got %v, want nil (job after window)", err)
		}

		mu.Lock()
		if calls != 2 {
			t.Errorf("WithUniqueWindow(): got %d calls, want 2 (should run after window expires)", calls)
		}
		mu.Unlock()
	})

	t.Run("multiple_duplicates_blocked", func(t *testing.T) {
		var calls int
		var mu sync.Mutex
		locker := NewUniqueMemoryLocker()
		window := 50 * time.Millisecond

		// Run first job.
		WithUniqueWindow(func(ctx context.Context) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		}, "test", window, locker)(context.Background())

		// Try multiple duplicates within window.
		for range 5 {
			err := WithUniqueWindow(func(ctx context.Context) error {
				mu.Lock()
				calls++
				mu.Unlock()
				return nil
			}, "test", window, locker)(context.Background())
			if err != nil {
				t.Fatalf("WithUniqueWindow(): got %v, want nil", err)
			}
		}

		mu.Lock()
		if calls != 1 {
			t.Errorf("WithUniqueWindow(): got %d calls, want 1 (all duplicates should be blocked)", calls)
		}
		mu.Unlock()
	})

	t.Run("contention_try_duplicate_returns_unique_contended", func(t *testing.T) {
		var calls atomic.Int32
		locker := NewUniqueMemoryLocker()

		window := 50 * time.Millisecond
		job := WithUniqueWindow(func(ctx context.Context) error {
			calls.Add(1)
			return nil
		}, "test", window, locker)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithUniqueWindow(): got %v, want nil (first job)", err)
		}

		err := WithUniqueWindow(func(ctx context.Context) error {
			calls.Add(1)
			return nil
		}, "test", window, locker)(ContextWithContentionTry(context.Background()))
		if !errors.Is(err, ErrUniqueContended) {
			t.Fatalf("WithUniqueWindow(): got %v, want %v", err, ErrUniqueContended)
		}

		if got := calls.Load(); got != 1 {
			t.Fatalf("WithUniqueWindow(): got %d calls, want duplicate not to run", got)
		}
	})

	t.Run("acquire_fail_returns_unique_contended_with_contention_try", func(t *testing.T) {
		locker := &acquireFailLocker{}

		err := WithUniqueWindow(func(ctx context.Context) error {
			t.Error("WithUniqueWindow(): job should not run when acquire fails")
			return nil
		}, "test", time.Minute, locker)(ContextWithContentionTry(context.Background()))

		if !errors.Is(err, ErrUniqueContended) {
			t.Fatalf("WithUniqueWindow(): got %v, want %v on acquire fail", err, ErrUniqueContended)
		}
		if got := locker.acquireCalls.Load(); got != 1 {
			t.Fatalf("WithUniqueWindow(): acquire calls got %d, want 1", got)
		}
	})

}
