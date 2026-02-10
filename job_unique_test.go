package cq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWithoutOverlap(t *testing.T) {
	var wg sync.WaitGroup              // Waitgroup for jobs.
	locker := NewOverlapMemoryLocker() // Memory locker for WithoutOverlap job.
	runs := 10                         // Number of times to run jobs.
	amountBase := 10                   // Base amount.
	amounto := amountBase              // Amount for overlap func.
	amountno := amountBase             // Amount for no overlap func.
	decrement := 4                     // Amount to decrement by.
	want := amountBase % decrement     // Based on how many times amount can be cleanly decremented.

	jobo := func(i int) Job {
		return WithoutOverlap(func(ctx context.Context) error {
			defer wg.Done()
			ac := amounto // Copy amount.
			if i%3 == 0 {
				// Simulate "work" which could mean the copy is outdated.
				time.Sleep(10 * time.Millisecond)
			}
			if ac < decrement {
				return nil
			}
			amounto -= decrement
			return nil
		}, "jobo", locker)
	}

	jobno := func(i int) Job {
		return func(ctx context.Context) error {
			defer wg.Done()
			ac := amountno // Copy amount.
			if i%3 == 0 {
				// Simulate "work" which could mean the copy is outdated.
				time.Sleep(10 * time.Millisecond)
			}
			if ac < decrement {
				return nil
			}
			amountno -= decrement
			return nil
		}
	}

	wg.Add(runs * 2)
	go func() {
		for i := 0; i < runs; i++ {
			go jobo(i)(context.Background())
		}
	}()
	go func() {
		for i := 0; i < runs; i++ {
			go jobno(i)(context.Background())
		}
	}()
	wg.Wait()

	if amounto != want {
		// Locks should ensure the value matches our want.
		t.Errorf("WithoutOverlap: got amounto %v, want %v", amounto, want)
	}
	if amountno > 0 {
		// Without locks would cause the amount to go below 0 due to the copy.
		t.Errorf("WithoutOverlap: got amountno %v, want < 0", amountno)
	}
}

func TestWithUnique(t *testing.T) {
	t.Run("normal", func(tt *testing.T) {
		var called bool
		locker := NewUniqueMemoryLocker()

		go WithUnique(func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			called = true
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
		if !called {
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

		// These jobs should run because lock expired.
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				WithUnique(func(ctx context.Context) error {
					calls.Add(1)
					return nil
				}, "test", 0, locker)(context.Background())
			}()
		}

		wg.Wait()

		// Only count the 2 jobs that ran after expiry (first job still running).
		if got := calls.Load(); got != 2 {
			t.Errorf("WithUnique(): got %d calls, want 2", got)
		}
	})

	t.Run("zero_duration", func(t *testing.T) {
		var called bool
		locker := NewUniqueMemoryLocker()

		// Zero duration means lock doesn't expire until job completes.
		go WithUnique(func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			called = true
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
		if !called {
			t.Error("WithUnique(): job should have been called")
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
		for i := 0; i < 5; i++ {
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
}
