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

func (l *acquireFailLocker) Exists(key string) bool {
	return false
}

func (l *acquireFailLocker) Get(key string) (LockValue[struct{}], bool) {
	return LockValue[struct{}]{}, false
}

func (l *acquireFailLocker) Acquire(key string, lock LockValue[struct{}]) bool {
	l.acquireCalls.Add(1)
	return false
}

func (l *acquireFailLocker) Release(key string) bool {
	return false
}

func (l *acquireFailLocker) ForceRelease(key string) {}

func (l *acquireFailLocker) Aquire(key string, lock LockValue[struct{}]) bool {
	return l.Acquire(key, lock)
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
}

func TestWithUnique(t *testing.T) {
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

	t.Run("release_duplicate", func(t *testing.T) {
		lockedErr := errors.New("unique locked")
		var calls atomic.Int32
		locker := NewUniqueMemoryLocker()
		queue := NewQueue(1, 1, 10)
		queue.Start()
		defer queue.Stop(true)

		job := WithUnique(func(ctx context.Context) error {
			if calls.Add(1) == 1 {
				time.Sleep(30 * time.Millisecond)
			}
			return nil
		}, "test", 0, locker, WithUniqueLockedError(lockedErr))
		job = WithRelease(job, queue, 40*time.Millisecond, 1, func(err error) bool {
			return errors.Is(err, lockedErr)
		})

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

	t.Run("locked_error", func(t *testing.T) {
		lockedErr := errors.New("unique locked")
		locker := NewUniqueMemoryLocker()

		go WithUnique(func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		}, "test", 0, locker)(context.Background())

		time.Sleep(10 * time.Millisecond)

		err := WithUnique(func(ctx context.Context) error {
			t.Error("WithUnique(): duplicate should not run")
			return nil
		}, "test", 0, locker, WithUniqueLockedError(lockedErr))(context.Background())
		if !errors.Is(err, lockedErr) {
			t.Fatalf("WithUnique(): got %v, want locked error", err)
		}
	})

	t.Run("locked_error_when_acquire_race_lost", func(t *testing.T) {
		lockedErr := errors.New("unique locked")
		locker := &acquireFailLocker{}

		err := WithUnique(func(ctx context.Context) error {
			t.Error("WithUnique(): job should not run when acquire fails")
			return nil
		}, "test", time.Minute, locker, WithUniqueLockedError(lockedErr))(context.Background())

		if !errors.Is(err, lockedErr) {
			t.Fatalf("WithUnique(): got %v, want locked error on acquire fail", err)
		}
		if got := locker.acquireCalls.Load(); got != 1 {
			t.Fatalf("WithUnique(): acquire calls got %d, want 1", got)
		}
	})
}

func TestWithUniqueWindow(t *testing.T) {
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
			t.Errorf("WithUniqueWindow(): got %v, want nil (duplicate job)", err)
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
			WithUniqueWindow(func(ctx context.Context) error {
				mu.Lock()
				calls++
				mu.Unlock()
				return nil
			}, "test", window, locker)(context.Background())
		}

		mu.Lock()
		if calls != 1 {
			t.Errorf("WithUniqueWindow(): got %d calls, want 1 (all duplicates should be blocked)", calls)
		}
		mu.Unlock()
	})

	t.Run("locked_error", func(t *testing.T) {
		lockedErr := errors.New("unique window locked")
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
		}, "test", window, locker, WithUniqueLockedError(lockedErr))(context.Background())
		if !errors.Is(err, lockedErr) {
			t.Fatalf("WithUniqueWindow(): got %v, want locked error", err)
		}

		if got := calls.Load(); got != 1 {
			t.Fatalf("WithUniqueWindow(): got %d calls, want duplicate not to run", got)
		}
	})

	t.Run("locked_error_when_acquire_race_lost", func(t *testing.T) {
		lockedErr := errors.New("unique window locked")
		locker := &acquireFailLocker{}

		err := WithUniqueWindow(func(ctx context.Context) error {
			t.Error("WithUniqueWindow(): job should not run when acquire fails")
			return nil
		}, "test", time.Minute, locker, WithUniqueLockedError(lockedErr))(context.Background())

		if !errors.Is(err, lockedErr) {
			t.Fatalf("WithUniqueWindow(): got %v, want locked error on acquire fail", err)
		}
		if got := locker.acquireCalls.Load(); got != 1 {
			t.Fatalf("WithUniqueWindow(): acquire calls got %d, want 1", got)
		}
	})
}
