package cq

import (
	"context"
	"errors"
	"testing"
)

func TestWithBatch(t *testing.T) {
	t.Run("all_success", func(t *testing.T) {
		var completed bool
		var completedErrors []error
		var progressCalls int

		jobs := []Job{
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error { return nil },
		}

		batchJobs, state := WithBatch(
			jobs,
			WithBatchOnComplete(func(errs []error) {
				completed = true
				completedErrors = errs
			}),
			WithBatchOnProgress(func(c, t int) { progressCalls++ }),
		)

		for _, job := range batchJobs {
			if err := job(context.Background()); err != nil {
				t.Errorf("WithBatch(): got %v, want nil", err)
			}
		}

		if !completed {
			t.Error("WithBatch(): onComplete should have been called")
		}
		if len(completedErrors) != 0 {
			t.Errorf("WithBatch(): should have no errors, got %d", len(completedErrors))
		}
		if progressCalls != 3 {
			t.Errorf("WithBatch(): progress calls: got %d, want 3", progressCalls)
		}
		if int(state.CompletedJobs.Load()) != 3 {
			t.Errorf("WithBatch(): completed jobs: got %d, want 3", state.CompletedJobs.Load())
		}
		if int(state.FailedJobs.Load()) != 0 {
			t.Errorf("WithBatch(): failed jobs: got %d, want 0", state.FailedJobs.Load())
		}
	})

	t.Run("with_failure", func(t *testing.T) {
		var completed bool
		var completedErrors []error

		jobs := []Job{
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error { return errors.New("job2 error") },
			func(ctx context.Context) error { return errors.New("job3 error") },
		}

		batchJobs, state := WithBatch(
			jobs,
			WithBatchOnComplete(func(errs []error) {
				completed = true
				completedErrors = errs
			}),
		)

		for _, job := range batchJobs {
			_ = job(context.Background())
		}

		if !completed {
			t.Error("WithBatch(): onComplete should have been called")
		}
		if len(completedErrors) != 2 {
			t.Errorf("WithBatch(): errors: got %d, want 2", len(completedErrors))
		}
		if int(state.CompletedJobs.Load()) != 3 {
			t.Errorf("WithBatch(): completed jobs: got %d, want 3", state.CompletedJobs.Load())
		}
		if int(state.FailedJobs.Load()) != 2 {
			t.Errorf("WithBatch(): failed jobs: got %d, want 2", state.FailedJobs.Load())
		}
	})

	t.Run("empty_jobs", func(t *testing.T) {
		jobs, state := WithBatch([]Job{})
		if jobs != nil || state != nil {
			t.Error("WithBatch(): empty jobs should return nil")
		}
	})

	t.Run("done_channel_closes_on_completion", func(t *testing.T) {
		jobs := []Job{
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error { return nil },
		}

		batchJobs, state := WithBatch(jobs)

		select {
		case <-state.Done():
			t.Error("WithBatch(): Done() should not be closed before jobs run")
		default:
		}

		for _, job := range batchJobs {
			_ = job(context.Background())
		}

		select {
		case <-state.Done():
		default:
			t.Error("WithBatch(): Done() should be closed after all jobs finish")
		}
	})

	t.Run("snapshot_reflects_progress", func(t *testing.T) {
		ctx := context.Background()
		jobs := []Job{
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error { return errors.New("boom") },
		}

		batchJobs, state := WithBatch(jobs, WithBatchID("batch-snap"))

		// Init is lazy: no record exists until the first job runs.
		if _, err := state.Snapshot(ctx); !errors.Is(err, ErrBatchNotFound) {
			t.Errorf("Snapshot() before run: got %v, want ErrBatchNotFound", err)
		}

		_ = batchJobs[0](ctx)
		rec, err := state.Snapshot(ctx)
		if err != nil {
			t.Fatalf("Snapshot() after first job: %v", err)
		}
		if rec.Completed != 1 || rec.Failed != 0 || rec.Done {
			t.Errorf("Snapshot() after 1: got completed=%d failed=%d done=%v, want 1/0/false", rec.Completed, rec.Failed, rec.Done)
		}

		_ = batchJobs[1](ctx)
		rec, _ = state.Snapshot(ctx)
		if rec.Completed != 2 || rec.Failed != 1 || !rec.Done {
			t.Errorf("Snapshot() after 2: got completed=%d failed=%d done=%v, want 2/1/true", rec.Completed, rec.Failed, rec.Done)
		}
		if len(rec.Errors) != 1 || rec.Errors[0] != "boom" {
			t.Errorf("Snapshot().Errors: got %v, want [\"boom\"]", rec.Errors)
		}
	})

	t.Run("snapshot_not_found_after_delete", func(t *testing.T) {
		ctx := context.Background()
		store := NewMemoryBatchStore()
		batchJobs, state := WithBatch(
			[]Job{func(ctx context.Context) error { return nil }},
			WithBatchStore(store),
		)
		_ = batchJobs[0](ctx)

		_ = store.Delete(ctx, state.ID())

		_, err := state.Snapshot(ctx)
		if !errors.Is(err, ErrBatchNotFound) {
			t.Errorf("Snapshot() after delete: got %v, want ErrBatchNotFound", err)
		}

		done, err := state.IsDone(ctx)
		if err != nil {
			t.Errorf("IsDone() after delete: unexpected err %v", err)
		}
		if done {
			t.Error("IsDone() after delete: got true, want false")
		}
	})

	t.Run("is_done", func(t *testing.T) {
		ctx := context.Background()
		jobs := []Job{func(ctx context.Context) error { return nil }}
		batchJobs, state := WithBatch(jobs)

		if done, _ := state.IsDone(ctx); done {
			t.Error("IsDone(): should be false before jobs run")
		}
		_ = batchJobs[0](ctx)
		if done, _ := state.IsDone(ctx); !done {
			t.Error("IsDone(): should be true after all jobs run")
		}
	})

	t.Run("custom_id_and_namespace", func(t *testing.T) {
		_, state := WithBatch(
			[]Job{func(ctx context.Context) error { return nil }},
			WithBatchID("import-42"),
			WithBatchNamespace("tenant-7"),
		)
		if got, want := state.ID(), "tenant-7:import-42"; got != want {
			t.Errorf("ID(): got %q, want %q", got, want)
		}
	})

	t.Run("auto_generated_ids_are_unique", func(t *testing.T) {
		_, a := WithBatch([]Job{func(ctx context.Context) error { return nil }})
		_, b := WithBatch([]Job{func(ctx context.Context) error { return nil }})
		if a.ID() == b.ID() {
			t.Errorf("auto IDs should differ: both = %q", a.ID())
		}
	})

	t.Run("custom_store", func(t *testing.T) {
		store := NewMemoryBatchStore()
		batchJobs, state := WithBatch(
			[]Job{
				func(ctx context.Context) error { return nil },
				func(ctx context.Context) error { return nil },
			},
			WithBatchID("shared"),
			WithBatchStore(store),
		)

		ctx := context.Background()
		for _, j := range batchJobs {
			_ = j(ctx)
		}

		// Cross-handle lookup: another caller queries the same store.
		rec, exists, err := store.Load(ctx, state.ID())
		if err != nil || !exists {
			t.Fatalf("store.Load(): err=%v exists=%v", err, exists)
		}
		if !rec.Done || rec.Completed != 2 {
			t.Errorf("store.Load(): got done=%v completed=%d, want true/2", rec.Done, rec.Completed)
		}
	})

	t.Run("init_failure_short_circuits_jobs", func(t *testing.T) {
		store := &failingInitStore{err: errors.New("db down")}

		var completeCalls int
		batchJobs, state := WithBatch(
			[]Job{
				func(ctx context.Context) error { return nil },
				func(ctx context.Context) error { return nil },
			},
			WithBatchStore(store),
			WithBatchOnComplete(func([]error) { completeCalls++ }),
		)

		for _, j := range batchJobs {
			err := j(context.Background())
			if !errors.Is(err, ErrBatchInitFailed) {
				t.Errorf("expected ErrBatchInitFailed, got %v", err)
			}
		}

		// onComplete must still fire exactly once even when Init fails.
		if completeCalls != 1 {
			t.Errorf("onComplete: got %d calls, want 1", completeCalls)
		}
		select {
		case <-state.Done():
		default:
			t.Error("Done() should close after all jobs short-circuit")
		}
	})
}

// failingInitStore is a BatchStore whose Init always fails.
type failingInitStore struct {
	err error
}

func (f *failingInitStore) Init(_ context.Context, _ string, _ int) error { return f.err }
func (f *failingInitStore) RecordResult(_ context.Context, _ string, _ error) (BatchRecord, bool, error) {
	return BatchRecord{}, false, nil
}
func (f *failingInitStore) Load(_ context.Context, _ string) (BatchRecord, bool, error) {
	return BatchRecord{}, false, nil
}
func (f *failingInitStore) Delete(_ context.Context, _ string) error { return nil }
