package cq

import (
	"context"
	"errors"
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
		var cran bool
		var fran bool
		job := WithResultHandler(func() error {
			return nil
		}, func() {
			cran = true
		}, func(err error) {
			fran = true
		})
		job()

		if !cran {
			t.Error("WithResultHandler should have ran complete handler, was not ran")
		}
		if fran {
			t.Error("WithResultHandler should have not ran fail handler, was ran")
		}
	})

	t.Run("failed", func(t *testing.T) {
		var cran bool
		var fran bool
		job := WithResultHandler(func() error {
			return errors.New("error")
		}, func() {
			cran = true
		}, func(err error) {
			fran = true
		})
		job()

		if !fran {
			t.Error("WithResultHandler should have ran fail handler, was not ran")
		}
		if cran {
			t.Error("WithResultHandler should have not ran complete handler, was ran")
		}
	})
}

func TestWithRetry(t *testing.T) {
	var calls int
	retries := 2
	job := WithRetry(func() error {
		calls++
		return errors.New("error")
	}, retries)
	job()

	if calls != 2 {
		t.Errorf("WithRetry ran job %v times, want %v", calls, retries)
	}
}

func TestWithBackoff(t *testing.T) {
	retries := 3
	tlimit := time.Duration(4) * time.Second // one retry will wait 1 second, two will wait 2 seconds, (1 + 2) + (1s buffer) = limit
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
		t.Errorf("Backoff should have completed within %v for %v retries", tlimit, retries)
	case <-done:
		return
	}
}

func TestWithTimeout(t *testing.T) {
	want := context.DeadlineExceeded
	slimit := time.Duration(2) * time.Second
	tlimit := time.Duration(1) * time.Second

	done := make(chan error, 1)
	go func() {
		job := WithTimeout(func() error {
			time.Sleep(slimit)
			return nil
		}, tlimit)
		done <- job()
	}()
	if err := <-done; !errors.Is(err, want) {
		t.Errorf("WithTimeout error was %v, want %v", err, want)
	}
}

func TestWithDeadline(t *testing.T) {
	want := context.DeadlineExceeded
	slimit := time.Duration(2) * time.Second
	tlimit := time.Now().Add(time.Duration(1) * time.Second)

	done := make(chan error, 1)
	go func() {
		job := WithDeadline(func() error {
			time.Sleep(slimit)
			return nil
		}, tlimit)
		done <- job()
	}()
	if err := <-done; !errors.Is(err, want) {
		t.Errorf("WithDeadline error was %v, want %v", err, want)
	}
}
