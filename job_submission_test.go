package cq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestQueueSubmit_AttributesAndSuccessResult(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)

	attributes := map[string]string{"source": "sqs"}
	var gotMeta JobMeta
	handle, err := q.Submit(context.Background(), func(ctx context.Context) error {
		gotMeta = MetaFromContext(ctx)
		return nil
	},
		WithJobID("message-123"),
		WithJobName("process-message"),
		WithJobAttributes(attributes),
	)
	if err != nil {
		t.Fatalf("Submit(): got err=%v, want nil", err)
	}

	attributes["source"] = "changed"
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(): got err=%v, want nil", err)
	}

	if gotMeta.ID != "message-123" {
		t.Fatalf("job meta ID: got %q, want %q", gotMeta.ID, "message-123")
	}
	if gotMeta.Name != "process-message" {
		t.Fatalf("job meta name: got %q, want %q", gotMeta.Name, "process-message")
	}
	if gotMeta.Attributes["source"] != "sqs" {
		t.Fatalf("job attribute source: got %q, want %q", gotMeta.Attributes["source"], "sqs")
	}

	result, ok := handle.Result()
	if !ok {
		t.Fatal("Result(): got ok=false, want true")
	}
	if result.Err != nil {
		t.Fatalf("Result().Err: got %v, want nil", result.Err)
	}
	if result.StartedAt.IsZero() || result.FinishedAt.IsZero() {
		t.Fatal("Result(): expected execution timestamps")
	}
}

func TestQueueSubmit_FailureAndPanicResults(t *testing.T) {
	q := NewQueue(1, 1, 2)
	q.Start()
	defer q.Stop(true)

	wantErr := errors.New("failed")
	failed, err := q.Submit(context.Background(), func(context.Context) error {
		return wantErr
	})
	if err != nil {
		t.Fatalf("Submit(failure): got err=%v, want nil", err)
	}
	if err := failed.Wait(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Wait(failure): got %v, want %v", err, wantErr)
	}

	panicked, err := q.Submit(context.Background(), func(context.Context) error {
		panic("boom")
	})
	if err != nil {
		t.Fatalf("Submit(panic): got err=%v, want nil", err)
	}
	err = panicked.Wait(context.Background())
	var panicErr *PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("Wait(panic): got %T %v, want *PanicError", err, err)
	}
	if panicErr.Origin != PanicOriginJob {
		t.Fatalf("Wait(panic) origin: got %q, want %q", panicErr.Origin, PanicOriginJob)
	}
}

func TestPanicError_UnwrapsErrorValue(t *testing.T) {
	want := errors.New("panic cause")
	err := &PanicError{Value: want, Origin: PanicOriginJob}
	if !errors.Is(err, want) {
		t.Fatalf("errors.Is(): got false, want true")
	}
}

func TestJobHandle_WaitContextDoesNotCancelJob(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)

	release := make(chan struct{})
	handle, err := q.Submit(context.Background(), func(context.Context) error {
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("Submit(): got err=%v, want nil", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := handle.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait(timeout): got %v, want %v", err, context.DeadlineExceeded)
	}

	close(release)
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(after release): got err=%v, want nil", err)
	}
}

func TestJobHandle_CancelPendingPreventsExecution(t *testing.T) {
	q := NewQueue(0, 0, 1)
	q.Start()
	defer q.Stop(false)

	ran := make(chan struct{}, 1)
	handle := mustSubmit(t, q, func(context.Context) error {
		ran <- struct{}{}
		return nil
	})

	if !handle.Cancel() {
		t.Fatal("Cancel(): got false, want true")
	}
	if handle.Cancel() {
		t.Fatal("Cancel(second): got true, want false")
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrJobCancelled) {
		t.Fatalf("Wait(): got %v, want %v", err, ErrJobCancelled)
	}
	select {
	case <-ran:
		t.Fatal("cancelled pending job executed")
	default:
	}
}

func TestJobHandle_CancelRunningSignalsContext(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)

	started := make(chan struct{})
	cause := make(chan error, 1)
	handle := mustSubmit(t, q, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	})
	<-started

	if !handle.Cancel() {
		t.Fatal("Cancel(): got false, want true")
	}
	if handle.Cancel() {
		t.Fatal("Cancel(second): got true, want false")
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrJobCancelled) {
		t.Fatalf("Wait(): got %v, want %v", err, ErrJobCancelled)
	}
	if err := <-cause; !errors.Is(err, ErrJobCancelled) {
		t.Fatalf("context cause: got %v, want %v", err, ErrJobCancelled)
	}
	// Handle completion is published just before the worker updates queue tallies.
	waitFor(t, time.Second, func() bool {
		return q.TallyOf(JobStateCancelled) == 1
	})
	if got := q.TallyOf(JobStateFailed); got != 0 {
		t.Fatalf("TallyOf(JobStateFailed): got %d, want 0", got)
	}
	if got := q.Stats().CancelledJobs; got != 1 {
		t.Fatalf("Stats().CancelledJobs: got %d, want 1", got)
	}
}

func TestJobHandle_CancelRunningWaitsForJobExit(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)

	started := make(chan struct{})
	release := make(chan struct{})
	handle := mustSubmit(t, q, func(context.Context) error {
		close(started)
		<-release
		return nil
	})
	<-started

	if !handle.Cancel() {
		t.Fatal("Cancel(): got false, want true")
	}
	select {
	case <-handle.Done():
		t.Fatal("Done() closed before running job exited")
	default:
	}

	close(release)
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(): got %v, want nil", err)
	}
	if handle.Cancel() {
		t.Fatal("Cancel(completed): got true, want false")
	}
}

func TestJobHandle_CancelDelayedPreventsSubmission(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)

	ran := make(chan struct{}, 1)
	handle, err := q.SubmitAfter(context.Background(), func(context.Context) error {
		ran <- struct{}{}
		return nil
	}, time.Hour)
	if err != nil {
		t.Fatalf("SubmitAfter(): got err=%v, want nil", err)
	}

	if !handle.Cancel() {
		t.Fatal("Cancel(): got false, want true")
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrJobCancelled) {
		t.Fatalf("Wait(): got %v, want %v", err, ErrJobCancelled)
	}
	select {
	case <-ran:
		t.Fatal("cancelled delayed job executed")
	default:
	}
}

func TestQueueSubmit_NonBlockingAndAbandoned(t *testing.T) {
	q := NewQueue(0, 0, 1)
	q.Start()

	pending, err := q.Submit(context.Background(), func(context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Submit(first): got err=%v, want nil", err)
	}

	rejected, err := q.Submit(context.Background(), func(context.Context) error {
		return nil
	}, WithNonBlocking())
	if rejected != nil {
		t.Fatal("Submit(non-blocking): got handle, want nil")
	}
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Submit(non-blocking): got err=%v, want %v", err, ErrQueueFull)
	}

	q.Stop(false)
	if err := pending.Wait(context.Background()); !errors.Is(err, ErrJobAbandoned) {
		t.Fatalf("Wait(abandoned): got %v, want %v", err, ErrJobAbandoned)
	}
}

func TestQueueSubmitAfter_ReportsFutureRejection(t *testing.T) {
	q := NewQueue(1, 1, 1, WithPauseBehavior(PauseReject))
	q.Start()
	defer q.Stop(true)

	handle, err := q.SubmitAfter(context.Background(), func(context.Context) error {
		return nil
	}, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("SubmitAfter(): got err=%v, want nil", err)
	}
	if err := q.Pause(); err != nil {
		t.Fatalf("Pause(): got err=%v, want nil", err)
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrQueuePaused) {
		t.Fatalf("Wait(): got %v, want %v", err, ErrQueuePaused)
	}
}

func TestQueueSubmitAfter_ReportsFutureFullQueue(t *testing.T) {
	q := NewQueue(0, 0, 1)
	q.Start()
	defer q.Stop(false)

	mustSubmit(t, q, func(context.Context) error { return nil })
	handle, err := q.SubmitAfter(context.Background(), func(context.Context) error {
		return nil
	}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("SubmitAfter(): got err=%v, want nil", err)
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Wait(): got %v, want %v", err, ErrQueueFull)
	}
}

func TestQueueSubmitAfter_ReportsQueueStop(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()

	handle, err := q.SubmitAfter(context.Background(), func(context.Context) error {
		return nil
	}, time.Hour)
	if err != nil {
		t.Fatalf("SubmitAfter(): got err=%v, want nil", err)
	}
	q.Stop(true)

	if err := handle.Wait(context.Background()); !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("Wait(): got %v, want %v", err, ErrQueueStopped)
	}
}

func TestQueueSubmitBatch_PartialAcceptance(t *testing.T) {
	q := NewQueue(0, 0, 1)
	q.Start()
	defer q.Stop(false)

	jobs := []Job{
		func(context.Context) error { return nil },
		func(context.Context) error { return nil },
	}
	handles, err := q.SubmitBatch(context.Background(), jobs, WithNonBlocking())
	if len(handles) != 1 {
		t.Fatalf("SubmitBatch(): got %d handles, want 1", len(handles))
	}
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("SubmitBatch(): got err=%v, want %v", err, ErrQueueFull)
	}
}

func TestQueueSubmit_HooksReceiveSubmissionAttributes(t *testing.T) {
	events := make(chan JobEvent, 1)
	q := NewQueue(1, 1, 1, WithHooks(Hooks{
		OnSuccess: func(event JobEvent) {
			events <- event
		},
	}))
	q.Start()
	defer q.Stop(true)

	handle, err := q.Submit(context.Background(), func(context.Context) error {
		return nil
	}, WithJobName("sync"), WithJobAttribute("tenant", "a"))
	if err != nil {
		t.Fatalf("Submit(): got err=%v, want nil", err)
	}
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(): got err=%v, want nil", err)
	}

	event := <-events
	if event.Name != "sync" || event.Attributes["tenant"] != "a" {
		t.Fatalf("event attributes: got name=%q attributes=%v", event.Name, event.Attributes)
	}
}
