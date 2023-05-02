package cq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestJobStateString(t *testing.T) {
	tests := []struct {
		want  string
		state JobState
	}{
		{
			want:  "created",
			state: JobStateCreated,
		},
		{
			want:  "pending",
			state: JobStatePending,
		},
		{
			want:  "active",
			state: JobStateActive,
		},
		{
			want:  "failed",
			state: JobStateFailed,
		},
		{
			want:  "completed",
			state: JobStateCompleted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Errorf("JobState.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultBackoffFunc(t *testing.T) {
	tests := []struct {
		name    string
		retries int
		want    time.Duration
	}{
		{
			name:    "no-retries",
			retries: 0,
			want:    time.Duration(1) * time.Second,
		},
		{
			name:    "one-retry",
			retries: 1,
			want:    time.Duration(1) * time.Second,
		},
		{
			name:    "two-retries",
			retries: 2,
			want:    time.Duration(2) * time.Second,
		},
		{
			name:    "three-retries",
			retries: 3,
			want:    time.Duration(4) * time.Second,
		},
		{
			name:    "four-retries",
			retries: 4,
			want:    time.Duration(8) * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultBackoffFunc(tt.retries); got != tt.want {
				t.Errorf("defaultBackoffFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWithResultHandler(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		var cran bool // Did complete run?
		var fran bool // Did fail run?

		job := WithResultHandler(
			func() error {
				return nil
			}, func() {
				cran = true
			}, func(err error) {
				fran = true
			},
		)
		job()

		if !cran {
			t.Error("WithResultHandler(): completed handler: should not have executed")
		}
		if fran {
			t.Error("WithResultHandler(): failed handler: should have executed")
		}
	})

	t.Run("failed", func(t *testing.T) {
		var cran bool // Did complete run?
		var fran bool // Did fail run?

		job := WithResultHandler(
			func() error {
				return errors.New("error")
			}, func() {
				cran = true
			}, func(err error) {
				fran = true
			},
		)
		job()

		if !fran {
			t.Error("WithResultHandler(): failed handler: should not have executed")
		}
		if cran {
			t.Error("WithResultHandler(): completed handler: should have executed")
		}
	})
}

func TestWithRetry(t *testing.T) {
	var calls int // Number of times job was called.
	retries := 2  // Number of retries to do.

	job := WithRetry(func() error {
		calls++
		return errors.New("error")
	}, retries)
	job()
	if calls != retries {
		t.Errorf("WithRetry(): job ran %v times, want %v", calls, retries)
	}
}

func TestWithBackoff(t *testing.T) {
	retries := 2                             // Number of retries.
	tlimit := time.Duration(4) * time.Second // One retry = 1 second, two = 2 seconds... (1s + 2s) + (1s buffer) = limit.

	ctx, ctxc := context.WithTimeout(context.TODO(), tlimit)
	defer ctxc()

	done := make(chan error, 1)
	go func() {
		job := WithRetry(WithBackoff(func() error {
			return errors.New("error")
		}, nil), retries)
		done <- job()
	}()
	select {
	case <-ctx.Done():
		t.Errorf("WithBackoff(): should have completed within %v for %v retries", tlimit, retries)
	case <-done:
		return
	}
}

func TestWithTimeout(t *testing.T) {
	want := context.DeadlineExceeded
	slimit := time.Duration(2) * time.Second // Job sleep.
	tlimit := time.Duration(1) * time.Second // Timeout.

	done := make(chan error, 1)
	go func() {
		job := WithTimeout(func() error {
			time.Sleep(slimit)
			return nil
		}, tlimit)
		done <- job()
	}()
	if err := <-done; !errors.Is(err, want) {
		t.Errorf("WithTimeout(): error was %v, want %v", err, want)
	}
}

func TestWithDeadline(t *testing.T) {
	want := context.DeadlineExceeded
	slimit := time.Duration(2) * time.Second                 // Job sleep.
	tlimit := time.Now().Add(time.Duration(1) * time.Second) // Deadline.

	done := make(chan error, 1)
	go func() {
		job := WithDeadline(func() error {
			time.Sleep(slimit)
			return nil
		}, tlimit)
		done <- job()
	}()
	if err := <-done; !errors.Is(err, want) {
		t.Errorf("WithDeadline(): error was %v, want %v", err, want)
	}
}

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
		return WithoutOverlap(func() error {
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
		return func() error {
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
		for i := 0; i < runs; i += 1 {
			go jobo(i)()
		}
	}()
	go func() {
		for i := 0; i < runs; i += 1 {
			go jobno(i)()
		}
	}()
	wg.Wait()

	if amounto != want {
		// Locks should ensure the value matches our want.
		t.Errorf("amounto = %v, want %v", amounto, want)
	}
	if amountno > 0 {
		// Without locks would cause the amount to go below 0 due to the copy.
		t.Errorf("amountno = %v, want <0", amountno)
	}
}

func TestWithUnqiue(t *testing.T) {
	t.Run("normal", func(tt *testing.T) {
		var called bool
		locker := NewUniqueMemoryLocker()

		go WithUnique(func() error {
			time.Sleep(50 * time.Millisecond)
			called = true
			return nil
		}, "test", 1*time.Minute, locker)()
		// Allow goroutine to run.
		time.Sleep(10 * time.Millisecond)
		// This job should not fire since the uniqueness of initial
		// job is set to 1m, and the "work" is taking 50ms.
		go WithUnique(func() error {
			t.Error("WithUnique: job should not fire")
			return nil
		}, "test", 1*time.Minute, locker)()

		time.Sleep(60 * time.Millisecond)
		if !called {
			t.Error("WithUnique: job should have been called")
		}
	})

	t.Run("expired", func(t *testing.T) {
		var calls int
		locker := NewUniqueMemoryLocker()
		want := 2

		// The lock on this job should be released since it
		// expires 10ms from now.
		go WithUnique(func() error {
			time.Sleep(500 * time.Millisecond)
			calls++
			return nil
		}, "test", 10*time.Millisecond, locker)()
		// Allow goroutine to run.
		time.Sleep(10 * time.Millisecond)
		for i := 0; i < 2; i += 1 {
			// Each job should run fine.
			go WithUnique(func() error {
				calls++
				return nil
			}, "test", 0*time.Millisecond, locker)()
		}

		time.Sleep(20 * time.Millisecond)
		if calls != want {
			t.Errorf("WithUnique: calls: got %v, want %v", calls, want)
		}
	})
}
