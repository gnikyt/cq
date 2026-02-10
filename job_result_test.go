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
