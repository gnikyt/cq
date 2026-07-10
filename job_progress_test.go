package cq

import (
	"context"
	"sync"
	"testing"
)

func TestProgressFraction(t *testing.T) {
	cases := []struct {
		p      Progress
		want   float64
		wantOK bool
	}{
		{Progress{Completed: 25, Total: 100}, 0.25, true},
		{Progress{Completed: 0, Total: 100}, 0, true},
		{Progress{Completed: 100, Total: 100}, 1, true},
		{Progress{Completed: 150, Total: 100}, 1, true}, // Clamped high.
		{Progress{Completed: -10, Total: 100}, 0, true}, // Clamped low.
		{Progress{Completed: 50, Total: 0}, 0, false},   // Unknown total.
		{Progress{Completed: 50, Total: -1}, 0, false},  // Invalid total.
		{Progress{Stage: "transforming"}, 0, false},     // Stage-only progress.
	}
	for _, tc := range cases {
		got, ok := tc.p.Fraction()
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("Fraction(%+v): got (%v, %v), want (%v, %v)", tc.p, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestSetProgressWithoutContext(t *testing.T) {
	if SetProgress(context.Background(), Progress{Completed: 1}) {
		t.Fatal("expected SetProgress to return false without progress context")
	}
	if _, ok := ProgressFromContext(context.Background()); ok {
		t.Fatal("expected no progress without progress context")
	}
}

// captureProgressReporter records every reported update.
type captureProgressReporter struct {
	mu      sync.Mutex
	metas   []JobMeta
	updates []Progress
}

func (c *captureProgressReporter) ReportProgress(_ context.Context, meta JobMeta, progress Progress) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metas = append(c.metas, meta)
	c.updates = append(c.updates, progress)
}

func TestWithProgressReportsUpdates(t *testing.T) {
	reporter := &captureProgressReporter{}
	meta := JobMeta{ID: "job-1", Name: "import"}

	job := WithProgress(func(ctx context.Context) error {
		if !SetProgress(ctx, Progress{Completed: 1, Total: 3, Stage: "fetch"}) {
			t.Error("expected progress context to be available")
		}
		if !SetProgress(ctx, Progress{Completed: 2, Total: 3, Stage: "transform"}) {
			t.Error("expected progress context to be available")
		}

		latest, ok := ProgressFromContext(ctx)
		if !ok {
			t.Fatal("expected latest progress to be readable")
		}
		if latest.Completed != 2 || latest.Stage != "transform" {
			t.Errorf("unexpected latest progress: %+v", latest)
		}
		return nil
	}, reporter)

	if err := job(contextWithMeta(context.Background(), meta)); err != nil {
		t.Fatalf("unexpected job error: %v", err)
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.updates) != 2 {
		t.Fatalf("got %d reported updates, want 2", len(reporter.updates))
	}
	if reporter.updates[0].Stage != "fetch" || reporter.updates[1].Stage != "transform" {
		t.Errorf("unexpected update order: %+v", reporter.updates)
	}
	for _, m := range reporter.metas {
		if m.ID != "job-1" {
			t.Errorf("got meta ID %q, want job-1", m.ID)
		}
	}
}

func TestWithProgressNilReporter(t *testing.T) {
	job := WithProgress(func(ctx context.Context) error {
		if !SetProgress(ctx, Progress{Completed: 5, Total: 10}) {
			t.Error("expected progress context even without a reporter")
		}
		p, ok := ProgressFromContext(ctx)
		if !ok || p.Completed != 5 {
			t.Errorf("ProgressFromContext(): got %+v ok=%v, want Completed=5", p, ok)
		}
		return nil
	}, nil)
	if err := job(context.Background()); err != nil {
		t.Fatalf("unexpected job error: %v", err)
	}
}

func TestWithProgressOnQueue(t *testing.T) {
	queue := NewQueue(1, 2, 16)
	queue.Start()
	defer queue.Stop(true)

	reporter := NewMemoryProgressReporter()
	job := WithProgress(func(ctx context.Context) error {
		SetProgress(ctx, Progress{Completed: 10, Total: 10, Stage: "done"})
		return nil
	}, reporter)

	handle, err := queue.Submit(context.Background(), job, WithJobID("progress-job"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}

	p, ok := reporter.Progress("progress-job")
	if !ok {
		t.Fatal("expected stored progress for job ID")
	}
	if f, known := p.Fraction(); !known || f != 1 || p.Stage != "done" {
		t.Errorf("unexpected stored progress: %+v", p)
	}

	reporter.Delete("progress-job")
	if _, ok := reporter.Progress("progress-job"); ok {
		t.Error("expected progress entry to be deleted")
	}
}

func TestMemoryProgressReporterIgnoresEmptyID(t *testing.T) {
	reporter := NewMemoryProgressReporter()
	reporter.ReportProgress(context.Background(), JobMeta{}, Progress{Completed: 1})
	if _, ok := reporter.Progress(""); ok {
		t.Fatal("expected updates without job ID to be ignored")
	}
}
