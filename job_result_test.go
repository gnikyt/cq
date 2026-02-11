package cq

import (
	"context"
	"errors"
	"testing"
)

func TestWithResultHandler(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		var cran bool // Did complete run?
		var fran bool // Did fail run?

		job := WithResultHandler(
			func(ctx context.Context) error {
				return nil
			}, func() {
				cran = true
			}, func(err error) {
				fran = true
			},
		)
		if err := job(context.Background()); err != nil {
			t.Errorf("WithResultHandler(): got %v, want nil", err)
		}
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
			func(ctx context.Context) error {
				return errors.New("error")
			}, func() {
				cran = true
			}, func(err error) {
				fran = true
			},
		)
		if err := job(context.Background()); err == nil {
			t.Error("WithResultHandler(): job should have errored")
		}

		if !fran {
			t.Error("WithResultHandler(): failed handler: should not have executed")
		}
		if cran {
			t.Error("WithResultHandler(): completed handler: should have executed")
		}
	})
}

func TestWithOutcome(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		var completed bool // Did complete run?
		var failed bool    // Did fail run?
		var discarded bool // Did discard run?

		job := WithOutcome(
			func(ctx context.Context) error {
				return nil
			},
			func() {
				completed = true
			},
			func(error) {
				failed = true
			},
			func(error) {
				discarded = true
			},
		)
		if err := job(context.Background()); err != nil {
			t.Errorf("got %v, want nil", err)
		}
		if !completed || failed || discarded {
			t.Errorf("completed=%v failed=%v discarded=%v", completed, failed, discarded)
		}
	})

	t.Run("failed", func(t *testing.T) {
		var completed bool // Did complete run?
		var failed bool    // Did fail run?
		var discarded bool // Did discard run?

		job := WithOutcome(
			func(ctx context.Context) error {
				return errors.New("err")
			},
			func() {
				completed = true
			},
			func(error) {
				failed = true
			},
			func(error) {
				discarded = true
			},
		)
		if err := job(context.Background()); err == nil {
			t.Error("want error")
		}
		if !failed || completed || discarded {
			t.Errorf("completed=%v failed=%v discarded=%v", completed, failed, discarded)
		}
	})

	t.Run("discarded", func(t *testing.T) {
		var completed bool   // Did complete run?
		var failed bool      // Did fail run?
		var discarded bool   // Did discard run?
		var discardArg error // Discard reason

		job := WithOutcome(
			func(ctx context.Context) error {
				return AsDiscard(errors.New("skip"))
			},
			func() {
				completed = true
			},
			func(error) {
				failed = true
			},
			func(err error) {
				discarded = true
				discardArg = err
			},
		)
		if err := job(context.Background()); err != nil {
			t.Errorf("WithOutcome(discard) should return nil to queue, got %v", err)
		}
		if !discarded || completed || failed {
			t.Errorf("completed=%v failed=%v discarded=%v", completed, failed, discarded)
		}
		if discardArg == nil || discardArg.Error() != "skip" {
			t.Errorf("OnDiscarded got %v", discardArg)
		}
	})

	t.Run("discarded nil reason", func(t *testing.T) {
		var discarded bool
		job := WithOutcome(
			func(ctx context.Context) error {
				return AsDiscard(nil)
			},
			nil,
			nil,
			func(err error) {
				discarded = true
			},
		)
		if err := job(context.Background()); err != nil {
			t.Errorf("got %v, want nil", err)
		}
		if !discarded {
			t.Error("OnDiscarded should have run")
		}
	})

	t.Run("all nil callbacks", func(t *testing.T) {
		job := WithOutcome(
			func(ctx context.Context) error {
				return nil
			},
			nil,
			nil,
			nil,
		)
		if err := job(context.Background()); err != nil {
			t.Errorf("got %v, want nil", err)
		}
	})
}
