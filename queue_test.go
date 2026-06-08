package cq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewQueueValidation(t *testing.T) {
	tests := []struct {
		name      string
		wmin      int
		wmax      int
		cap       int
		wantPanic string
	}{
		{
			name:      "negative wmin",
			wmin:      -1,
			wmax:      10,
			cap:       100,
			wantPanic: "cq: wmin must be >= 0",
		},
		{
			name:      "wmax less than wmin",
			wmin:      10,
			wmax:      5,
			cap:       100,
			wantPanic: "cq: wmax must be >= wmin",
		},
		{
			name:      "negative cap",
			wmin:      1,
			wmax:      10,
			cap:       -1,
			wantPanic: "cq: cap must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("NewQueue(%d, %d, %d) did not panic", tt.wmin, tt.wmax, tt.cap)
					return
				}
				if r != tt.wantPanic {
					t.Errorf("NewQueue(%d, %d, %d): got panic %v, want %v", tt.wmin, tt.wmax, tt.cap, r, tt.wantPanic)
				}
			}()
			NewQueue(tt.wmin, tt.wmax, tt.cap)
		})
	}
}

func TestNewQueueValidValues(t *testing.T) {
	// These should not panic.
	tests := []struct {
		name string
		wmin int
		wmax int
		cap  int
	}{
		{"zero workers", 0, 0, 0},
		{"equal min max", 5, 5, 10},
		{"normal values", 1, 10, 100},
		{"zero cap", 1, 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQueue(tt.wmin, tt.wmax, tt.cap)
			if q == nil {
				t.Errorf("NewQueue(%d, %d, %d) returned nil", tt.wmin, tt.wmax, tt.cap)
			}
		})
	}
}

func TestQueueGetters(t *testing.T) {
	tests := []struct {
		name string
		want func(*Queue) error
	}{
		{
			name: "capacity",
			want: func(q *Queue) error {
				want := 1
				if c := q.Capacity(); c != want {
					return fmt.Errorf("Capacity(): got %v, want %v", c, want)
				}
				return nil
			},
		},
		{
			name: "running",
			want: func(q *Queue) error {
				want := 1
				if rw := q.RunningWorkers(); rw != want {
					return fmt.Errorf("RunningWorkers(): got %v, want %v", rw, want)
				}
				return nil
			},
		},
		{
			name: "idle",
			want: func(q *Queue) error {
				want := 1
				if iw := q.IdleWorkers(); iw != want {
					return fmt.Errorf("IdleWorkers(): got %v, want %v", iw, want)
				}
				return nil
			},
		},
		{
			name: "range",
			want: func(q *Queue) error {
				wantMin := 1
				wantMax := 2
				wmin, wmax := q.WorkerRange()
				if wmin != wantMin || wmax != wantMax {
					return fmt.Errorf("WorkerRange(): got %v:%v, want %v:%v", wmin, wmax, wantMin, wantMax)
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		q := NewQueue(1, 2, 1)
		q.Start()
		defer q.Stop(false)

		t.Run(tt.name, func(t *testing.T) {
			if err := tt.want(q); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestQueueEnqueue(t *testing.T) {
	var called atomic.Bool // Was job called?
	q := NewQueue(1, 1, 1)
	q.Start()

	q.Enqueue(func(ctx context.Context) error {
		called.Store(true)
		return nil
	})
	q.Stop(true)

	if !called.Load() {
		t.Error("Enqueue(): expected job to enqueued and executed")
	}
}

func TestQueueDelayEnqueue(t *testing.T) {
	delay := time.Duration(500 * time.Millisecond)
	sleep := time.Duration(600 * time.Millisecond)

	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)
	q.DelayEnqueue(
		func(ctx context.Context) error {
			return nil
		},
		delay,
	)

	if njc := q.TallyOf(JobStateCreated); njc != 0 {
		t.Errorf("DelayQueue(): TallyOf(JobStateCreated): got %v, want 0 (job not created yet)", njc)
	}
	time.Sleep(sleep) // Give job time to enqueue and run.
	if njc := q.TallyOf(JobStateCreated); njc != 1 {
		t.Errorf("DelayQueue(): TallyOf(JobStateCreated): got %v, want 1", njc)
	}
}

func TestQueueEnqueueBatch(t *testing.T) {
	q := NewQueue(2, 5, 10)
	q.Start()
	defer q.Stop(true)

	var called1 atomic.Bool
	var called2 atomic.Bool
	var called3 atomic.Bool

	jobs := []Job{
		func(ctx context.Context) error {
			called1.Store(true)
			return nil
		},
		func(ctx context.Context) error {
			called2.Store(true)
			return nil
		},
		func(ctx context.Context) error {
			called3.Store(true)
			return nil
		},
	}

	q.EnqueueBatch(jobs)

	// Wait for all jobs to complete.
	time.Sleep(100 * time.Millisecond)

	if !called1.Load() || !called2.Load() || !called3.Load() {
		t.Errorf(
			"EnqueueBatch(): jobs not all executed: called1=%v, called2=%v, called3=%v",
			called1.Load(), called2.Load(), called3.Load(),
		)
	}

	// Check tallies.
	if created := q.TallyOf(JobStateCreated); created != 3 {
		t.Errorf("EnqueueBatch(): TallyOf(JobStateCreated): got %d, want 3", created)
	}
	if completed := q.TallyOf(JobStateCompleted); completed != 3 {
		t.Errorf("EnqueueBatch(): TallyOf(JobStateCompleted): got %d, want 3", completed)
	}
}

func TestQueueDelayEnqueueBatch(t *testing.T) {
	delay := time.Duration(500 * time.Millisecond)
	sleep := time.Duration(600 * time.Millisecond)

	q := NewQueue(2, 5, 10)
	q.Start()
	defer q.Stop(true)

	jobs := []Job{
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
	}

	q.DelayEnqueueBatch(jobs, delay)

	// Jobs should not be created yet.
	if njc := q.TallyOf(JobStateCreated); njc != 0 {
		t.Errorf("DelayEnqueueBatch(): TallyOf(JobStateCreated): got %v, want 0 (jobs not created yet)", njc)
	}

	// Wait for jobs to enqueue and run.
	time.Sleep(sleep)

	// All jobs should now be created.
	if njc := q.TallyOf(JobStateCreated); njc != 3 {
		t.Errorf("DelayEnqueueBatch(): TallyOf(JobStateCreated): got %v, want 3", njc)
	}
	if completed := q.TallyOf(JobStateCompleted); completed != 3 {
		t.Errorf("DelayEnqueueBatch(): TallyOf(JobStateCompleted): got %d, want 3", completed)
	}
}

func TestQueueEnqueueIfStopped(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	q.Stop(true)

	job := func(ctx context.Context) error { return nil }
	if ok := q.TryEnqueue(job); ok {
		t.Error("TryEnqueue(): got true, want false")
	}
}

func TestQueueEnqueueOrError(t *testing.T) {
	t.Run("stopped", func(t *testing.T) {
		q := NewQueue(1, 1, 1)
		q.Start()
		q.Stop(true)

		err := q.EnqueueOrError(func(ctx context.Context) error { return nil })
		if !errors.Is(err, ErrQueueStopped) {
			t.Fatalf("EnqueueOrError(): got %v, want %v", err, ErrQueueStopped)
		}
	})

	t.Run("paused reject", func(t *testing.T) {
		q := NewQueue(1, 1, 1, WithPauseBehavior(PauseReject))
		q.Start()
		defer q.Stop(true)

		if err := q.Pause(); err != nil {
			t.Fatalf("Pause(): unexpected error: %v", err)
		}

		err := q.EnqueueOrError(func(ctx context.Context) error { return nil })
		if !errors.Is(err, ErrQueuePaused) {
			t.Fatalf("EnqueueOrError(): got %v, want %v", err, ErrQueuePaused)
		}
	})
}

func TestQueueEnqueueContext(t *testing.T) {
	t.Run("returns_context_error_when_full", func(t *testing.T) {
		// No workers: first job fills the queue; second blocks until context timeout.
		q := NewQueue(0, 0, 1)
		q.Start()
		defer q.Stop(false)

		if err := q.EnqueueOrError(func(ctx context.Context) error { return nil }); err != nil {
			t.Fatalf("EnqueueOrError(): unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()

		err := q.EnqueueContext(ctx, func(ctx context.Context) error { return nil })
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("EnqueueContext(): got %v, want %v", err, context.DeadlineExceeded)
		}
	})

	t.Run("succeeds_when_space_frees_before_deadline", func(t *testing.T) {
		q := NewQueue(1, 1, 1)
		q.Start()
		defer q.Stop(true)

		block := make(chan struct{})
		var ranThird atomic.Bool

		// Job 1 occupies the single worker.
		q.Enqueue(func(ctx context.Context) error {
			<-block
			return nil
		})
		// Job 2 occupies the single buffer slot.
		q.Enqueue(func(ctx context.Context) error { return nil })

		errCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			errCh <- q.EnqueueContext(ctx, func(ctx context.Context) error {
				ranThird.Store(true)
				return nil
			})
		}()

		time.Sleep(25 * time.Millisecond)
		close(block) // Frees worker and then queue slot for the EnqueueContext call.

		if err := <-errCh; err != nil {
			t.Fatalf("EnqueueContext(): got %v, want nil", err)
		}

		waitFor(t, 500*time.Millisecond, func() bool {
			return ranThird.Load()
		})
	})
}

func TestQueueTryEnqueueOrError(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		// No workers: jobs only occupy channel capacity.
		q := NewQueue(0, 0, 1)
		q.Start()
		defer q.Stop(false)

		ok, err := q.TryEnqueueOrError(func(ctx context.Context) error { return nil })
		if !ok || err != nil {
			t.Fatalf("TryEnqueueOrError(): first enqueue got (%v, %v), want (true, nil)", ok, err)
		}

		ok, err = q.TryEnqueueOrError(func(ctx context.Context) error { return nil })
		if ok {
			t.Fatal("TryEnqueueOrError(): got ok=true on full queue, want false")
		}
		if !errors.Is(err, ErrQueueFull) {
			t.Fatalf("TryEnqueueOrError(): got %v, want %v", err, ErrQueueFull)
		}
	})
}

func TestQueueStopContext(t *testing.T) {
	t.Run("returns_nil_when_jobs_finish_before_deadline", func(t *testing.T) {
		q := NewQueue(1, 1, 1)
		q.Start()

		var called atomic.Bool
		q.Enqueue(func(ctx context.Context) error {
			called.Store(true)
			return nil
		})

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		if err := q.StopContext(ctx); err != nil {
			t.Fatalf("StopContext(): got %v, want nil", err)
		}
		if !called.Load() {
			t.Fatal("StopContext(): expected job to run before shutdown")
		}
	})

	t.Run("returns_deadline_exceeded_when_job_hangs", func(t *testing.T) {
		q := NewQueue(1, 1, 1)
		q.Start()

		release := make(chan struct{})
		q.Enqueue(func(ctx context.Context) error {
			<-release
			return nil
		})

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()

		err := q.StopContext(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("StopContext(): got %v, want %v", err, context.DeadlineExceeded)
		}

		// Unblock hanging job goroutine to avoid test leakage.
		close(release)
	})

	t.Run("returns_stopped_on_repeat_calls", func(t *testing.T) {
		q := NewQueue(1, 1, 1)
		q.Start()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		if err := q.StopContext(ctx); err != nil {
			t.Fatalf("StopContext(): first call got %v, want nil", err)
		}
		if err := q.StopContext(ctx); !errors.Is(err, ErrQueueStopped) {
			t.Fatalf("StopContext(): second call got %v, want %v", err, ErrQueueStopped)
		}
	})
}

func TestQueueStopTimeout(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()

	release := make(chan struct{})
	q.Enqueue(func(ctx context.Context) error {
		<-release
		return nil
	})

	err := q.StopTimeout(30 * time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopTimeout(): got %v, want %v", err, context.DeadlineExceeded)
	}

	close(release)
}

func TestQueueTallies(t *testing.T) {
	tests := []struct {
		state JobState
		job   Job
		want  func(*Queue) error
	}{
		{
			state: JobStateCompleted,
			job: func(ctx context.Context) error {
				return nil
			},
			want: func(q *Queue) error {
				want := 1
				if jc := q.TallyOf(JobStateCompleted); jc != want {
					return fmt.Errorf("TallyOf(JobStateCompleted): got %v, want %v", jc, want)
				}
				if jc := q.TallyOf(JobStateCreated); jc != want {
					return fmt.Errorf("TallyOf(JobStateCreated): got %v, want %v", jc, want)
				}
				return nil
			},
		},
		{
			state: JobStateFailed,
			job: func(ctx context.Context) error {
				return errors.New("error")
			},
			want: func(q *Queue) error {
				want := 1
				if jc := q.TallyOf(JobStateFailed); jc != want {
					return fmt.Errorf("TallyOf(JobStateFailed): got %v, want %v", jc, want)
				}
				if jc := q.TallyOf(JobStateCreated); jc != want {
					return fmt.Errorf("TallyOf(JobStateCreated): got %v, want %v", jc, want)
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			q := NewQueue(1, 1, 1)
			q.Start()
			q.Enqueue(tt.job)
			q.Stop(true)
			if err := tt.want(q); err != nil {
				t.Errorf("%T: %v", tt.state, err)
			}
		})
	}
}

func TestQueuePanic(t *testing.T) {
	var called atomic.Bool

	q := NewQueue(1, 1, 1, WithPanicHandler(func(err any) {
		called.Store(true)
	}))
	q.Start()
	q.Enqueue(func(ctx context.Context) error {
		panic("panic")
	})
	q.Stop(true)

	if !called.Load() {
		t.Error("WithPanicHandler(): should have executed")
	}
}

func TestIdleWorkerTick(t *testing.T) {
	var atMax bool                                                  // Running workers at max?
	wmin, wmax := 1, 2                                              // Min. & max. workers.
	jobs := 50                                                      // Number of jobs.
	delay := time.Duration(50 * time.Millisecond)                   // Delay of each job.
	idle := time.Duration(150 * time.Millisecond)                   // Idle tick.
	sleep := (time.Duration(jobs * int(delay))) + (2 * time.Second) // Check delay with 2s buffer.

	q := NewQueue(wmin, wmax, 0, WithWorkerIdleTick(idle))
	q.Start()
	defer q.Stop(true)

	for i := 0; i < jobs; i++ {
		q.Enqueue(func(ctx context.Context) error {
			time.Sleep(delay)
			return nil
		})

		// At some point, we should have wmax workers.
		if !atMax {
			if rw := q.RunningWorkers(); rw == wmax {
				atMax = true
			}
		}
	}

	if !atMax {
		// Because so many jobs were spammed, this should be more than worker min.
		t.Error("Queue: did not spin up the desired workers")
	}
	time.Sleep(sleep) // Let the cleanup happen.
	// Should be back to worker min.
	if nrw := q.RunningWorkers(); nrw != wmin {
		t.Errorf("RunningWorkers(): got %v, want %v", nrw, wmin)
	}
}

func TestQueueSetWorkerRangeValidation(t *testing.T) {
	q := NewQueue(0, 1, 1)
	q.Start()
	defer q.Stop(false)

	if err := q.SetWorkerRange(-1, 1); err == nil {
		t.Fatal("SetWorkerRange(): expected error for negative min")
	}
	if err := q.SetWorkerRange(2, 1); err == nil {
		t.Fatal("SetWorkerRange(): expected error for max < min")
	}
}

func TestQueueSetWorkerRangeScaleUpMin(t *testing.T) {
	q := NewQueue(0, 1, 1)
	q.Start()
	defer q.Stop(false)

	if err := q.SetWorkerRange(2, 2); err != nil {
		t.Fatalf("SetWorkerRange(): unexpected error: %v", err)
	}

	waitFor(t, 500*time.Millisecond, func() bool {
		return q.RunningWorkers() == 2 && q.IdleWorkers() == 2
	})

	wmin, wmax := q.WorkerRange()
	if wmin != 2 || wmax != 2 {
		t.Fatalf("WorkerRange(): got %d:%d, want 2:2", wmin, wmax)
	}
}

func TestQueueSetWorkerRangeScaleDownMaxDoesNotPreemptActive(t *testing.T) {
	release := make(chan struct{})
	var started atomic.Int32

	q := NewQueue(0, 3, 10, WithWorkerIdleTick(20*time.Millisecond))
	q.Start()
	defer q.Stop(true)

	job := func(ctx context.Context) error {
		started.Add(1)
		<-release
		return nil
	}

	q.Enqueue(job)
	q.Enqueue(job)
	q.Enqueue(job)

	waitFor(t, 1*time.Second, func() bool {
		return started.Load() == 3 && q.RunningWorkers() == 3
	})

	if err := q.SetWorkerRange(0, 1); err != nil {
		t.Fatalf("SetWorkerRange(): unexpected error: %v", err)
	}

	// Active workers should not be preempted just because max was reduced.
	time.Sleep(100 * time.Millisecond)
	if got := q.RunningWorkers(); got != 3 {
		t.Fatalf("RunningWorkers(): got %d while jobs active, want 3", got)
	}

	close(release)

	waitFor(t, 1*time.Second, func() bool {
		return q.RunningWorkers() == 1
	})
}

func waitFor(t *testing.T, timeout time.Duration, pred func() bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tk := time.NewTicker(10 * time.Millisecond)
	defer tk.Stop()

	for {
		if pred() {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatal("condition not met before timeout")
		case <-tk.C:
		}
	}
}
