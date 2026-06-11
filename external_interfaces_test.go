package cq

import (
	"context"
	"errors"
	"testing"
)

type contextKey string

type recordingLocker struct {
	*MemoryLocker[struct{}]
	acquireErr error
	releaseErr error
	releaseCtx context.Context
}

func (l *recordingLocker) Acquire(ctx context.Context, key string, lock LockValue[struct{}]) (bool, error) {
	if l.acquireErr != nil {
		return false, l.acquireErr
	}
	return l.MemoryLocker.Acquire(ctx, key, lock)
}

func (l *recordingLocker) Release(ctx context.Context, key string) (bool, error) {
	l.releaseCtx = ctx
	if l.releaseErr != nil {
		return false, l.releaseErr
	}
	return l.MemoryLocker.Release(ctx, key)
}

func (l *recordingLocker) ReleaseIfOwner(ctx context.Context, key string, token string) (bool, error) {
	l.releaseCtx = ctx
	if l.releaseErr != nil {
		return false, l.releaseErr
	}
	return l.MemoryLocker.ReleaseIfOwner(ctx, key, token)
}

type recordingLimiter struct {
	releaseCtx context.Context
	releaseErr error
}

func (l *recordingLimiter) Acquire(context.Context, string) error {
	return nil
}

func (l *recordingLimiter) Release(ctx context.Context, _ string) error {
	l.releaseCtx = ctx
	return l.releaseErr
}

func TestWithUnique_PropagatesLockerError(t *testing.T) {
	want := errors.New("locker unavailable")
	locker := &recordingLocker{
		MemoryLocker: NewMemoryLocker[struct{}](),
		acquireErr:   want,
	}

	err := WithUnique(func(context.Context) error { return nil }, "key", 0, locker)(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("WithUnique(): got %v, want %v", err, want)
	}
}

func TestWithUnique_ReleaseUsesUncancelledContext(t *testing.T) {
	locker := &recordingLocker{MemoryLocker: NewMemoryLocker[struct{}]()}
	ctx, cancel := context.WithCancel(context.Background())

	err := WithUnique(func(context.Context) error {
		cancel()
		return context.Canceled
	}, "key", 0, locker)(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WithUnique(): got %v, want %v", err, context.Canceled)
	}
	if locker.releaseCtx == nil || locker.releaseCtx.Err() != nil {
		t.Fatalf("Release context: got %v, want non-cancelled context", locker.releaseCtx)
	}
}

func TestWithUnique_PropagatesReleaseError(t *testing.T) {
	want := errors.New("locker release unavailable")
	locker := &recordingLocker{
		MemoryLocker: NewMemoryLocker[struct{}](),
		releaseErr:   want,
	}

	err := WithUnique(func(context.Context) error { return nil }, "key", 0, locker)(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("WithUnique(): got %v, want %v", err, want)
	}
}

func TestWithConcurrencyByKey_ReleaseUsesUncancelledContext(t *testing.T) {
	limiter := &recordingLimiter{}
	ctx, cancel := context.WithCancel(context.Background())

	err := WithConcurrencyByKey(func(context.Context) error {
		cancel()
		return context.Canceled
	}, "key", limiter)(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WithConcurrencyByKey(): got %v, want %v", err, context.Canceled)
	}
	if limiter.releaseCtx == nil || limiter.releaseCtx.Err() != nil {
		t.Fatalf("Release context: got %v, want non-cancelled context", limiter.releaseCtx)
	}
}

func TestWithConcurrencyByKey_PropagatesReleaseError(t *testing.T) {
	want := errors.New("limiter release unavailable")
	limiter := &recordingLimiter{releaseErr: want}

	err := WithConcurrencyByKey(func(context.Context) error { return nil }, "key", limiter)(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("WithConcurrencyByKey(): got %v, want %v", err, want)
	}
}

func TestQueueHooks_ReceiveContexts(t *testing.T) {
	const key contextKey = "request"
	enqueueValue := make(chan string, 1)
	startMeta := make(chan JobMeta, 1)
	q := NewQueue(1, 1, 1, WithHooks(Hooks{
		OnEnqueue: func(ctx context.Context, _ JobEvent) {
			enqueueValue <- ctx.Value(key).(string)
		},
		OnStart: func(ctx context.Context, _ JobEvent) {
			startMeta <- MetaFromContext(ctx)
		},
	}))
	q.Start()
	defer q.Stop(true)

	ctx := context.WithValue(context.Background(), key, "value")
	handle, err := q.Submit(ctx, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(): %v", err)
	}

	if value := <-enqueueValue; value != "value" {
		t.Fatalf("enqueue hook context value: got %q, want %q", value, "value")
	}
	if meta := <-startMeta; meta.ID != handle.ID() {
		t.Fatalf("start hook metadata ID: got %q, want %q", meta.ID, handle.ID())
	}
}
