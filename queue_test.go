package cq

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

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
					return fmt.Errorf("Capacity() = %v, want %v", c, want)
				}
				return nil
			},
		},
		{
			name: "running",
			want: func(q *Queue) error {
				want := 1
				if rw := q.RunningWorkers(); rw != want {
					return fmt.Errorf("RunningWorkers() = %v, want %v", rw, want)
				}
				return nil
			},
		},
		{
			name: "idle",
			want: func(q *Queue) error {
				want := 1
				if iw := q.IdleWorkers(); iw != want {
					return fmt.Errorf("IdleWorkers() = %v, want %v", iw, want)
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
					return fmt.Errorf("WorkerRange() = %v:%v, want %v:%v", wmin, wmax, wantMin, wantMax)
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
	var called bool // Was job called?
	q := NewQueue(1, 1, 1)
	q.Start()

	q.Enqueue(func() error {
		called = true
		return nil
	})
	q.Stop(true)

	if !called {
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
		func() error {
			return nil
		},
		delay,
	)

	if njc := q.TallyOf(JobStateCreated); njc != 0 {
		t.Errorf("DelayQueue(): TallyOf(JobStateCreated): should not have created job yet, got %v, want 0", njc)
	}
	time.Sleep(sleep) // Give job time to enqueue and run.
	if njc := q.TallyOf(JobStateCreated); njc != 1 {
		t.Errorf("DelayQueue(): TallyOf(JobStateCreated): should have created job, got %v, want 1", njc)
	}
}

func TestQueueEnqueueMaxed(t *testing.T) {
	jobs := 150                                    // Number of jobs.
	delay := time.Duration(500 * time.Millisecond) // Job sleep.
	sleep := time.Duration(1 * time.Second)        // Check sleep.
	var pMsg string                                // Panic message.

	q := NewQueue(1, 1, 0, WithPanicHandler(func(err any) {
		pMsg = fmt.Sprintf("%v", err)
	}))
	q.Start()

	// Spam the queue with 0 capacity.
	for i := 0; i < jobs; i += 1 {
		go func() {
			q.Enqueue(func() error {
				time.Sleep(delay)
				return nil
			})
		}()
	}
	q.Stop(false)

	time.Sleep(sleep) // Allow panic to run.
	if pMsg != "send on closed channel" {
		t.Error("WithPanicHandler(): expected panic to happen")
	}
}

func TestQueueEnqueueIfStopped(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	q.Stop(true)

	job := func() error { return nil }
	if ok := q.TryEnqueue(job); ok {
		t.Error("TryEnqueue() = true, want false")
	}
}

func TestQueueTallies(t *testing.T) {
	tests := []struct {
		state JobState
		job   Job
		want  func(*Queue) error
	}{
		{
			state: JobStateCompleted,
			job: func() error {
				return nil
			},
			want: func(q *Queue) error {
				want := 1
				if jc := q.TallyOf(JobStateCompleted); jc != want {
					return fmt.Errorf("TallyOf(JobStateCompleted) = %v, want %v", jc, want)
				}
				if jc := q.TallyOf(JobStateCreated); jc != want {
					return fmt.Errorf("TallyOf(JobStateCreated) = %v, want %v", jc, want)
				}
				return nil
			},
		},
		{
			state: JobStateFailed,
			job: func() error {
				return errors.New("error")
			},
			want: func(q *Queue) error {
				want := 1
				if jc := q.TallyOf(JobStateFailed); jc != want {
					return fmt.Errorf("TallyOf(JobStateFailed) = %v, want %v", jc, want)
				}
				if jc := q.TallyOf(JobStateCreated); jc != want {
					return fmt.Errorf("TallyOf(JobStateCreated) = %v, want %v", jc, want)
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
	var called bool

	q := NewQueue(1, 1, 1, WithPanicHandler(func(err any) {
		called = true
	}))
	q.Start()
	q.Enqueue(func() error {
		panic("panic")
	})
	q.Stop(true)

	if !called {
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

	for i := 0; i < jobs; i += 1 {
		q.Enqueue(func() error {
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
		t.Errorf("RunningWorkers() = %v, want %v", nrw, wmin)
	}
}
