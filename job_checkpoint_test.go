package cq

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestWithCheckpoint(t *testing.T) {
	t.Run("marks_done_and_skips_repeated_step", func(t *testing.T) {
		store := NewMemoryCheckpointStore()
		var calls int

		job := WithCheckpoint(func(context.Context) error {
			calls++
			return nil
		}, "step-a", store)

		ctx := contextWithMeta(context.Background(), JobMeta{ID: "job-1"})
		if err := job(ctx); err != nil {
			t.Fatalf("first run: got err=%v, want nil", err)
		}
		if err := job(ctx); err != nil {
			t.Fatalf("second run: got err=%v, want nil", err)
		}
		if calls != 1 {
			t.Fatalf("got calls=%d, want 1", calls)
		}
	})

	t.Run("stores_data_and_marks_done_on_success", func(t *testing.T) {
		store := NewMemoryCheckpointStore()
		job := WithCheckpoint(func(ctx context.Context) error {
			ok := SetCheckpointData(ctx, []byte("state-1"))
			if !ok {
				t.Fatal("expected checkpoint context")
			}
			return nil
		}, "step-a", store)

		ctx := contextWithMeta(context.Background(), JobMeta{ID: "job-1"})
		if err := job(ctx); err != nil {
			t.Fatalf("run: got err=%v, want nil", err)
		}

		cp, exists, err := store.Load(context.Background(), "job-1:step-a")
		if err != nil {
			t.Fatalf("get: got err=%v, want nil", err)
		}
		if !exists {
			t.Fatal("expected checkpoint to exist")
		}
		if !cp.Done {
			t.Fatal("expected checkpoint done=true")
		}
		if !bytes.Equal(cp.Data, []byte("state-1")) {
			t.Fatalf("got data=%q, want %q", string(cp.Data), "state-1")
		}
	})

	t.Run("does_not_mark_done_on_job_error_by_default", func(t *testing.T) {
		store := NewMemoryCheckpointStore()
		var calls int

		jobErr := errors.New("boom")
		job := WithCheckpoint(func(context.Context) error {
			calls++
			return jobErr
		}, "step-a", store)

		ctx := contextWithMeta(context.Background(), JobMeta{ID: "job-1"})
		err := job(ctx)
		if !errors.Is(err, jobErr) {
			t.Fatalf("first run: got err=%v, want %v", err, jobErr)
		}
		err = job(ctx)
		if !errors.Is(err, jobErr) {
			t.Fatalf("second run: got err=%v, want %v", err, jobErr)
		}
		if calls != 2 {
			t.Fatalf("got calls=%d, want 2", calls)
		}

		cp, exists, err := store.Load(context.Background(), "job-1:step-a")
		if err != nil {
			t.Fatalf("get: got err=%v, want nil", err)
		}
		if exists && cp.Done {
			t.Fatal("expected checkpoint not marked done after failure")
		}
	})

	t.Run("can_persist_data_on_error_for_resume", func(t *testing.T) {
		store := NewMemoryCheckpointStore()
		var calls int
		job := WithCheckpoint(func(ctx context.Context) error {
			calls++
			if calls == 1 {
				if ok := SetCheckpointData(ctx, []byte("partial-progress")); !ok {
					t.Fatal("expected checkpoint context")
				}
				return errors.New("transient")
			}
			prev := CheckpointDataFromContext(ctx)
			if !bytes.Equal(prev, []byte("partial-progress")) {
				t.Fatalf("got resume data=%q, want %q", string(prev), "partial-progress")
			}
			return nil
		}, "step-a", store, WithCheckpointSaveOnFailure())

		ctx := contextWithMeta(context.Background(), JobMeta{ID: "job-1"})
		if err := job(ctx); err == nil {
			t.Fatal("first run: expected error")
		}
		if err := job(ctx); err != nil {
			t.Fatalf("second run: got err=%v, want nil", err)
		}

		cp, exists, err := store.Load(context.Background(), "job-1:step-a")
		if err != nil {
			t.Fatalf("get: got err=%v, want nil", err)
		}
		if !exists || !cp.Done {
			t.Fatal("expected done checkpoint after successful retry")
		}
	})

	t.Run("supports_json_helpers_for_typed_payloads", func(t *testing.T) {
		store := NewMemoryCheckpointStore()
		type resumeState struct {
			Offset int `json:"offset"`
		}

		var calls int
		job := WithCheckpoint(func(ctx context.Context) error {
			calls++
			if calls == 1 {
				ok, err := SetCheckpointDataAsJSON(ctx, resumeState{Offset: 2})
				if err != nil {
					t.Fatalf("marshal: got err=%v, want nil", err)
				}
				if !ok {
					t.Fatal("expected checkpoint context")
				}
				return errors.New("retry")
			}

			state, ok, err := CheckpointDataAsJSON[resumeState](ctx)
			if err != nil {
				t.Fatalf("unmarshal: got err=%v, want nil", err)
			}
			if !ok {
				t.Fatal("expected resume state")
			}
			if state.Offset != 2 {
				t.Fatalf("got offset=%d, want 2", state.Offset)
			}
			return nil
		}, "step-a", store, WithCheckpointSaveOnFailure())

		ctx := contextWithMeta(context.Background(), JobMeta{ID: "job-typed"})
		if err := job(ctx); err == nil {
			t.Fatal("first run: expected error")
		}
		if err := job(ctx); err != nil {
			t.Fatalf("second run: got err=%v, want nil", err)
		}
	})

	t.Run("strict_mode_fails_when_key_unavailable", func(t *testing.T) {
		store := NewMemoryCheckpointStore()
		called := false
		job := WithCheckpoint(func(context.Context) error {
			called = true
			return nil
		}, "step-a", store)

		err := job(context.Background())
		if !errors.Is(err, ErrCheckpointKeyUnavailable) {
			t.Fatalf("got err=%v, want %v", err, ErrCheckpointKeyUnavailable)
		}
		if called {
			t.Fatal("job should not execute when key is unavailable in strict mode")
		}
	})

	t.Run("best_effort_runs_when_key_unavailable", func(t *testing.T) {
		store := NewMemoryCheckpointStore()
		var calls int
		job := WithCheckpoint(func(context.Context) error {
			calls++
			return nil
		}, "step-a", store, WithCheckpointBestEffort())

		if err := job(context.Background()); err != nil {
			t.Fatalf("got err=%v, want nil", err)
		}
		if calls != 1 {
			t.Fatalf("got calls=%d, want 1", calls)
		}
	})

	t.Run("delete_on_success_removes_checkpoint", func(t *testing.T) {
		store := NewMemoryCheckpointStore()
		job := WithCheckpoint(func(ctx context.Context) error {
			SetCheckpointData(ctx, []byte("transient"))
			return nil
		}, "step-a", store, WithCheckpointDeleteOnSuccess())

		ctx := contextWithMeta(context.Background(), JobMeta{ID: "job-1"})
		if err := job(ctx); err != nil {
			t.Fatalf("run: got err=%v, want nil", err)
		}
		_, exists, err := store.Load(context.Background(), "job-1:step-a")
		if err != nil {
			t.Fatalf("get: got err=%v, want nil", err)
		}
		if exists {
			t.Fatal("expected checkpoint to be deleted")
		}
	})

	t.Run("allows_key_namespace_and_custom_key_function", func(t *testing.T) {
		store := &captureCheckpointStore{}
		job := WithCheckpoint(func(context.Context) error { return nil }, "step-a", store,
			WithCheckpointNamespace("orders"),
			WithCheckpointKeyFunc(func(context.Context, string) (string, bool) {
				return "wf-42:step-a", true
			}),
		)

		if err := job(context.Background()); err != nil {
			t.Fatalf("got err=%v, want nil", err)
		}
		if store.lastCheckedKey != "orders:wf-42:step-a" {
			t.Fatalf("checked key=%q, want %q", store.lastCheckedKey, "orders:wf-42:step-a")
		}
		if store.lastMarkedKey != "orders:wf-42:step-a" {
			t.Fatalf("marked key=%q, want %q", store.lastMarkedKey, "orders:wf-42:step-a")
		}
		if !store.lastSet.Done {
			t.Fatal("expected set checkpoint to be done")
		}
	})
}

func TestWithCheckpoint_StoreErrors(t *testing.T) {
	t.Run("strict_mode_propagates_get_errors", func(t *testing.T) {
		store := &errorCheckpointStore{getErr: errors.New("store down")}
		job := WithCheckpoint(func(context.Context) error { return nil }, "step-a", store,
			WithCheckpointKeyFunc(func(context.Context, string) (string, bool) {
				return "wf-1:step-a", true
			}),
		)

		err := job(context.Background())
		if !errors.Is(err, ErrCheckpointCheckFailed) {
			t.Fatalf("got err=%v, want %v", err, ErrCheckpointCheckFailed)
		}
	})

	t.Run("strict_mode_propagates_set_errors", func(t *testing.T) {
		store := &errorCheckpointStore{setErr: errors.New("write failed")}
		job := WithCheckpoint(func(context.Context) error { return nil }, "step-a", store,
			WithCheckpointKeyFunc(func(context.Context, string) (string, bool) {
				return "wf-1:step-a", true
			}),
		)

		err := job(context.Background())
		if !errors.Is(err, ErrCheckpointMarkFailed) {
			t.Fatalf("got err=%v, want %v", err, ErrCheckpointMarkFailed)
		}
	})

	t.Run("strict_mode_propagates_delete_errors", func(t *testing.T) {
		store := &errorCheckpointStore{deleteErr: errors.New("delete failed")}
		job := WithCheckpoint(func(context.Context) error { return nil }, "step-a", store,
			WithCheckpointDeleteOnSuccess(),
			WithCheckpointKeyFunc(func(context.Context, string) (string, bool) {
				return "wf-1:step-a", true
			}),
		)

		err := job(context.Background())
		if !errors.Is(err, ErrCheckpointDeleteFailed) {
			t.Fatalf("got err=%v, want %v", err, ErrCheckpointDeleteFailed)
		}
	})

	t.Run("best_effort_ignores_get_errors", func(t *testing.T) {
		var calls int
		store := &errorCheckpointStore{getErr: errors.New("store down")}
		job := WithCheckpoint(func(context.Context) error {
			calls++
			return nil
		}, "step-a", store,
			WithCheckpointBestEffort(),
			WithCheckpointKeyFunc(func(context.Context, string) (string, bool) {
				return "wf-1:step-a", true
			}),
		)

		if err := job(context.Background()); err != nil {
			t.Fatalf("got err=%v, want nil", err)
		}
		if calls != 1 {
			t.Fatalf("got calls=%d, want 1", calls)
		}
	})
}

type captureCheckpointStore struct {
	lastCheckedKey string
	lastMarkedKey  string
	lastSet        Checkpoint
	checkpoint     Checkpoint
	exists         bool
}

func (s *captureCheckpointStore) Load(_ context.Context, key string) (Checkpoint, bool, error) {
	s.lastCheckedKey = key
	return s.checkpoint, s.exists, nil
}

func (s *captureCheckpointStore) Store(_ context.Context, key string, cp Checkpoint) error {
	s.lastMarkedKey = key
	s.lastSet = cp
	s.checkpoint = cp
	s.exists = true
	return nil
}

func (s *captureCheckpointStore) Delete(_ context.Context, _ string) error {
	s.exists = false
	s.checkpoint = Checkpoint{}
	return nil
}

type errorCheckpointStore struct {
	getErr    error
	setErr    error
	deleteErr error
}

func (s *errorCheckpointStore) Load(_ context.Context, _ string) (Checkpoint, bool, error) {
	if s.getErr != nil {
		return Checkpoint{}, false, s.getErr
	}
	return Checkpoint{}, false, nil
}

func (s *errorCheckpointStore) Store(_ context.Context, _ string, _ Checkpoint) error {
	return s.setErr
}

func (s *errorCheckpointStore) Delete(_ context.Context, _ string) error {
	return s.deleteErr
}
