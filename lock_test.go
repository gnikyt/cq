package cq

import (
	"context"
	"testing"
	"time"
)

type coreTestLocker struct {
	locks map[string]LockValue[string]
}

func (l *coreTestLocker) Acquire(_ context.Context, key string, lock LockValue[string]) (bool, error) {
	if current, ok := l.locks[key]; ok && !current.IsExpired() {
		return false, nil
	}
	l.locks[key] = lock
	return true, nil
}

func (l *coreTestLocker) Release(_ context.Context, key string) (bool, error) {
	if _, ok := l.locks[key]; !ok {
		return false, nil
	}
	delete(l.locks, key)
	return true, nil
}

func TestLockValueIsExpired(t *testing.T) {
	if (LockValue[string]{}).IsExpired() {
		t.Fatal("zero expiration should not be expired")
	}
	if !(LockValue[string]{ExpiresAt: time.Now().Add(-time.Second)}).IsExpired() {
		t.Fatal("past expiration should be expired")
	}
}

func TestMemoryLockerCoreOperations(t *testing.T) {
	ctx := context.Background()
	locker := NewMemoryLocker[string]()
	lock := LockValue[string]{
		Value:     "value",
		ExpiresAt: time.Now().Add(time.Minute),
	}

	acquired, err := locker.Acquire(ctx, "key", lock)
	if err != nil || !acquired {
		t.Fatalf("Acquire(): got (%v, %v), want (true, nil)", acquired, err)
	}
	if acquired, err := locker.Acquire(ctx, "key", lock); err != nil || acquired {
		t.Fatalf("Acquire(existing): got (%v, %v), want (false, nil)", acquired, err)
	}

	got, exists, err := locker.Get(ctx, "key")
	if err != nil || !exists || got.Value != "value" {
		t.Fatalf("Get(): got (%+v, %v, %v)", got, exists, err)
	}

	released, err := locker.Release(ctx, "key")
	if err != nil || !released {
		t.Fatalf("Release(): got (%v, %v), want (true, nil)", released, err)
	}
	if _, exists, err := locker.Get(ctx, "key"); err != nil || exists {
		t.Fatalf("Get(released): got (exists=%v, err=%v), want (false, nil)", exists, err)
	}
}

func TestMemoryLockerReplacesExpiredLock(t *testing.T) {
	ctx := context.Background()
	locker := NewMemoryLocker[string]()
	_, _ = locker.Acquire(ctx, "key", LockValue[string]{ExpiresAt: time.Now().Add(-time.Second)})

	acquired, err := locker.Acquire(ctx, "key", LockValue[string]{
		Value:     "new",
		ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil || !acquired {
		t.Fatalf("Acquire(expired): got (%v, %v), want (true, nil)", acquired, err)
	}
}

func TestMemoryLockerRenewalOperations(t *testing.T) {
	ctx := context.Background()
	locker := NewMemoryLocker[string]()
	_, _ = locker.Acquire(ctx, "key", LockValue[string]{
		Token:     "owner",
		ExpiresAt: time.Now().Add(time.Minute),
	})

	if renewed, err := locker.Touch(ctx, "key", "other", time.Now().Add(2*time.Minute)); err != nil || renewed {
		t.Fatalf("Touch(other): got (%v, %v), want (false, nil)", renewed, err)
	}
	if renewed, err := locker.Touch(ctx, "key", "owner", time.Now().Add(2*time.Minute)); err != nil || !renewed {
		t.Fatalf("Touch(owner): got (%v, %v), want (true, nil)", renewed, err)
	}
	if released, err := locker.ReleaseIfOwner(ctx, "key", "other"); err != nil || released {
		t.Fatalf("ReleaseIfOwner(other): got (%v, %v), want (false, nil)", released, err)
	}
	if released, err := locker.ReleaseIfOwner(ctx, "key", "owner"); err != nil || !released {
		t.Fatalf("ReleaseIfOwner(owner): got (%v, %v), want (true, nil)", released, err)
	}
}

func TestMemoryLockerManagementOperations(t *testing.T) {
	ctx := context.Background()
	locker := NewMemoryLocker[string]()
	_, _ = locker.Acquire(ctx, "expired", LockValue[string]{ExpiresAt: time.Now().Add(-time.Second)})
	_, _ = locker.Acquire(ctx, "active", LockValue[string]{ExpiresAt: time.Now().Add(time.Minute)})

	removed, err := locker.Cleanup(ctx)
	if err != nil || removed != 1 {
		t.Fatalf("Cleanup(): got (%d, %v), want (1, nil)", removed, err)
	}
	if err := locker.ForceRelease(ctx, "active"); err != nil {
		t.Fatalf("ForceRelease(): %v", err)
	}
}

func TestLockerCapabilityInterfaces(t *testing.T) {
	var core Locker[string] = &coreTestLocker{locks: make(map[string]LockValue[string])}
	var readable ReadLocker[string] = NewMemoryLocker[string]()
	var renewable RenewableLocker = NewMemoryLocker[string]()
	var cleanable CleanableLocker = NewMemoryLocker[string]()
	var forceReleaser ForceReleaser = NewMemoryLocker[string]()
	var managed ManagedLocker[string] = NewMemoryLocker[string]()

	if core == nil || readable == nil || renewable == nil || cleanable == nil || forceReleaser == nil || managed == nil {
		t.Fatal("expected capability implementations")
	}
}
