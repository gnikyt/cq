package cq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithTimeout(t *testing.T) {
	want := context.DeadlineExceeded
	slimit := time.Duration(2 * time.Second) // Job sleep.
	tlimit := time.Duration(1 * time.Second) // Timeout.

	done := make(chan error)
	go func() {
		job := WithTimeout(func(ctx context.Context) error {
			time.Sleep(slimit)
			return nil
		}, tlimit)
		done <- job(context.Background())
	}()
	if err := <-done; !errors.Is(err, want) {
		t.Errorf("WithTimeout(): error was %v, want %v", err, want)
	}
}

func TestWithDeadline(t *testing.T) {
	want := context.DeadlineExceeded
	slimit := time.Duration(2 * time.Second)                 // Job sleep.
	tlimit := time.Now().Add(time.Duration(1 * time.Second)) // Deadline.

	done := make(chan error)
	go func() {
		job := WithDeadline(func(ctx context.Context) error {
			time.Sleep(slimit)
			return nil
		}, tlimit)
		done <- job(context.Background())
	}()
	if err := <-done; !errors.Is(err, want) {
		t.Errorf("WithDeadline(): error was %v, want %v", err, want)
	}
}
