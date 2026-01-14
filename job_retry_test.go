package cq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithRetry(t *testing.T) {
	var calls int // Number of times job was called.
	retries := 2  // Number of retries to do.

	job := WithRetry(func(ctx context.Context) error {
		calls++
		return errors.New("error")
	}, retries)
	if err := job(context.Background()); err == nil {
		t.Error("WithRetry(): job should have errored")
	}
	if calls != retries {
		t.Errorf("WithRetry(): job ran %v times, want %v", calls, retries)
	}
}

func TestWithBackoff(t *testing.T) {
	retries := 2                             // Number of retries.
	tlimit := time.Duration(4 * time.Second) // One retry = 1 second, two = 2 seconds... (1s + 2s) + (1s buffer) = limit.

	ctx, ctxc := context.WithTimeout(context.TODO(), tlimit)
	defer ctxc()

	done := make(chan error)
	go func() {
		job := WithRetry(WithBackoff(func(ctx context.Context) error {
			return errors.New("error")
		}, nil), retries)
		done <- job(context.Background())
	}()
	select {
	case <-ctx.Done():
		t.Errorf("WithBackoff(): should have completed within %v for %v retries", tlimit, retries)
	case <-done:
		return
	}
}
