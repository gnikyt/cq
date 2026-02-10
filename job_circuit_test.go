package cq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker(t *testing.T) {
	t.Run("state_method", func(t *testing.T) {
		cb := NewCircuitBreaker(2, 50*time.Millisecond)

		if cb.State() != "closed" {
			t.Errorf("initial state: got %s, want closed", cb.State())
		}

		job := WithCircuitBreaker(func(ctx context.Context) error {
			return errors.New("fail")
		}, cb)

		job(context.Background())
		job(context.Background())

		if cb.State() != "open" {
			t.Errorf("after failures: got %s, want open", cb.State())
		}

		time.Sleep(60 * time.Millisecond)

		if cb.State() != "half-open" {
			t.Errorf("after cooldown: got %s, want half-open", cb.State())
		}
	})

	t.Run("opens_after_threshold", func(t *testing.T) {
		cb := NewCircuitBreaker(3, 100*time.Millisecond)
		jobErr := errors.New("fail")

		job := WithCircuitBreaker(func(ctx context.Context) error {
			return jobErr
		}, cb)

		// First 3 failures should return the job error.
		for i := range 3 {
			err := job(context.Background())
			if !errors.Is(err, jobErr) {
				t.Errorf("call %d: got %v, want %v", i, err, jobErr)
			}
		}

		// Circuit should now be open.
		if !cb.IsOpen() {
			t.Error("circuit should be open after threshold")
		}

		// Next call should return ErrCircuitOpen.
		err := job(context.Background())
		if !errors.Is(err, ErrCircuitOpen) {
			t.Errorf("got %v, want ErrCircuitOpen", err)
		}
	})

	t.Run("closes_after_cooldown", func(t *testing.T) {
		cb := NewCircuitBreaker(2, 50*time.Millisecond)
		jobErr := errors.New("fail")

		job := WithCircuitBreaker(func(ctx context.Context) error {
			return jobErr
		}, cb)

		// Trigger circuit open.
		job(context.Background())
		job(context.Background())

		if !cb.IsOpen() {
			t.Error("circuit should be open")
		}

		// Wait for cooldown.
		time.Sleep(60 * time.Millisecond)

		if cb.IsOpen() {
			t.Error("circuit should be closed after cooldown")
		}

		// Should be able to call again.
		err := job(context.Background())
		if !errors.Is(err, jobErr) {
			t.Errorf("got %v, want %v", err, jobErr)
		}
	})

	t.Run("resets_on_success", func(t *testing.T) {
		cb := NewCircuitBreaker(3, 100*time.Millisecond)
		var shouldFail bool
		jobErr := errors.New("fail")

		job := WithCircuitBreaker(func(ctx context.Context) error {
			if shouldFail {
				return jobErr
			}
			return nil
		}, cb)

		// 2 failures.
		shouldFail = true
		job(context.Background())
		job(context.Background())

		// 1 success resets count.
		shouldFail = false
		job(context.Background())

		// 2 more failures shouldn't open (count reset).
		shouldFail = true
		job(context.Background())
		job(context.Background())

		if cb.IsOpen() {
			t.Error("circuit should not be open after reset")
		}
	})

	t.Run("success_passes_through", func(t *testing.T) {
		cb := NewCircuitBreaker(3, 100*time.Millisecond)

		job := WithCircuitBreaker(func(ctx context.Context) error {
			return nil
		}, cb)

		err := job(context.Background())
		if err != nil {
			t.Errorf("got %v, want nil", err)
		}
	})

	t.Run("half_open_allows_one", func(t *testing.T) {
		cb := NewCircuitBreaker(2, 50*time.Millisecond)
		jobErr := errors.New("fail")
		var calls int

		job := WithCircuitBreaker(func(ctx context.Context) error {
			calls++
			return jobErr
		}, cb)

		// Open the circuit.
		job(context.Background())
		job(context.Background())

		// Wait for cooldown.
		time.Sleep(60 * time.Millisecond)

		// First call should go through (half-open test).
		err1 := job(context.Background())
		if !errors.Is(err1, jobErr) {
			t.Errorf("half-open call: got %v, want %v", err1, jobErr)
		}

		// Second call should be rejected (still half-open, test failed).
		err2 := job(context.Background())
		if !errors.Is(err2, ErrCircuitOpen) {
			t.Errorf("second call: got %v, want ErrCircuitOpen", err2)
		}

		if calls != 3 {
			t.Errorf("got %d calls, want 3", calls)
		}
	})

	t.Run("half_open_disabled", func(t *testing.T) {
		cb := NewCircuitBreaker(2, 50*time.Millisecond)
		cb.SetHalfOpen(false)
		jobErr := errors.New("fail")
		var calls int

		job := WithCircuitBreaker(func(ctx context.Context) error {
			calls++
			return jobErr
		}, cb)

		// Open the circuit.
		job(context.Background())
		job(context.Background())

		// Wait for cooldown.
		time.Sleep(60 * time.Millisecond)

		// With half-open disabled, all calls should go through after cooldown.
		job(context.Background())
		job(context.Background())

		if calls != 4 {
			t.Errorf("got %d calls, want 4", calls)
		}
	})

	t.Run("half_open_success_closes", func(t *testing.T) {
		cb := NewCircuitBreaker(2, 50*time.Millisecond)
		var shouldFail bool

		job := WithCircuitBreaker(func(ctx context.Context) error {
			if shouldFail {
				return errors.New("fail")
			}
			return nil
		}, cb)

		// Open the circuit.
		shouldFail = true
		job(context.Background())
		job(context.Background())

		// Wait for cooldown.
		time.Sleep(60 * time.Millisecond)

		// Half-open test succeeds.
		shouldFail = false
		err := job(context.Background())
		if err != nil {
			t.Errorf("half-open success: got %v, want nil", err)
		}

		// Circuit should be closed, next call should work.
		err = job(context.Background())
		if err != nil {
			t.Errorf("after close: got %v, want nil", err)
		}
	})
}
