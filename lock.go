package cq

import (
	"context"
	"sync"
	"time"
)

// LockValue stores a lock value and optional expiration for a key.
type LockValue[T any] struct {
	Value     T         // Value stored in the lock.
	ExpiresAt time.Time // Expiration time of the lock.
	Token     string    // Optional token for owner-safe operations.
}

// IsExpired checks if the lock is expired.
func (lv LockValue[T]) IsExpired() bool {
	exp := lv.ExpiresAt
	if exp.IsZero() {
		// No expire time for lock, assume forever.
		exp = time.Now().Add(1 * time.Second)
	}
	return exp.Before(time.Now())
}

// Locker provides atomic lock acquisition and release.
type Locker[T any] interface {
	Acquire(ctx context.Context, key string, lock LockValue[T]) (acquired bool, err error)
	Release(ctx context.Context, key string) (released bool, err error)
}

// LockReader optionally retrieves lock state.
type LockReader[T any] interface {
	Get(ctx context.Context, key string) (lock LockValue[T], exists bool, err error)
}

// ReadLocker combines lock ownership operations with lock inspection.
type ReadLocker[T any] interface {
	Locker[T]
	LockReader[T]
}

// ForceReleaser optionally removes locks without owner checks.
type ForceReleaser interface {
	ForceRelease(ctx context.Context, key string) error
}

// RenewableLocker optionally provides owner-safe lease renewal operations.
type RenewableLocker interface {
	Touch(ctx context.Context, key string, token string, expiresAt time.Time) (renewed bool, err error)
	ReleaseIfOwner(ctx context.Context, key string, token string) (released bool, err error)
}

// CleanableLocker optionally removes expired locks.
type CleanableLocker interface {
	Cleanup(ctx context.Context) (removed int, err error)
}

// ManagedLocker provides the complete optional lock-management capability set.
type ManagedLocker[T any] interface {
	ReadLocker[T]
	ForceReleaser
	RenewableLocker
	CleanableLocker
}

// MemoryLocker is an in-memory Locker implementation backed by sync.Map.
type MemoryLocker[T any] struct {
	locks sync.Map
}

// NewMemoryLocker creates a new MemoryLocker for type T.
// T is the lock value type used by LockValue.
func NewMemoryLocker[T any]() *MemoryLocker[T] {
	return &MemoryLocker[T]{}
}

// NewUniqueMemoryLocker creates a new MemoryLocker instance for
// use with WithUnique... simply for quicker setup.
func NewUniqueMemoryLocker() *MemoryLocker[struct{}] {
	return NewMemoryLocker[struct{}]()
}

// NewOverlapMemoryLocker creates a MemoryLocker for use with WithoutOverlap.
func NewOverlapMemoryLocker() *MemoryLocker[struct{}] {
	return NewMemoryLocker[struct{}]()
}

// Get loads a lock for key.
// Returns the lock and whether it exists. If missing, returns an empty LockValue.
func (ml *MemoryLocker[T]) Get(ctx context.Context, key string) (LockValue[T], bool, error) {
	if err := ctx.Err(); err != nil {
		return LockValue[T]{}, false, err
	}
	lock, ok := ml.locks.Load(key)
	if !ok {
		return LockValue[T]{}, ok, nil
	}
	lv := lock.(LockValue[T])
	return lv, ok, nil
}

// Acquire attempts to claim a lock for key.
// It fails if a non-expired lock already exists. Otherwise it stores the new lock.
func (ml *MemoryLocker[T]) Acquire(ctx context.Context, key string, lock LockValue[T]) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	for {
		existing, ok := ml.locks.Load(key)
		if !ok {
			_, loaded := ml.locks.LoadOrStore(key, lock)
			if !loaded {
				return true, nil // First writer wins atomically.
			}
			continue // Lost a race to create... re-check current value.
		}

		current := existing.(LockValue[T])
		if !current.IsExpired() {
			return false, nil // Lock is still valid, skip.
		}

		// Lock exists but is expired... replace it if still unchanged.
		if ml.locks.CompareAndSwap(key, current, lock) {
			return true, nil // Successfully replaced expired lock.
		}
	}
}

// Release removes the lock for key.
// Returns true if a lock existed and was removed.
func (ml *MemoryLocker[T]) Release(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	for {
		existing, ok := ml.locks.Load(key)
		if !ok {
			return false, nil
		}
		if ml.locks.CompareAndDelete(key, existing) {
			return true, nil
		}
	}
}

// ReleaseIfOwner removes the lock for key only when the provided token matches.
func (ml *MemoryLocker[T]) ReleaseIfOwner(ctx context.Context, key string, token string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	for {
		existing, ok := ml.locks.Load(key)
		if !ok {
			return false, nil
		}

		current := existing.(LockValue[T])
		if current.Token != token {
			return false, nil
		}

		if ml.locks.CompareAndDelete(key, current) {
			return true, nil
		}
	}
}

// Touch updates a lock expiration when the provided owner token matches.
// It returns false if the lock is missing, expired, token-mismatched, or replaced concurrently.
func (ml *MemoryLocker[T]) Touch(ctx context.Context, key string, token string, expiresAt time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if expiresAt.IsZero() || !expiresAt.After(time.Now()) {
		return false, nil
	}

	for {
		existing, ok := ml.locks.Load(key)
		if !ok {
			return false, nil
		}

		current := existing.(LockValue[T])
		if current.IsExpired() || current.Token != token {
			return false, nil
		}

		next := current
		next.ExpiresAt = expiresAt

		if ml.locks.CompareAndSwap(key, current, next) {
			return true, nil
		}
	}
}

// ForceRelease removes the lock for key without existence checks.
func (ml *MemoryLocker[T]) ForceRelease(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ml.locks.Delete(key)
	return nil
}

// Cleanup removes all expired locks from memory and returns the count of removed locks.
// This is useful for preventing memory buildup in long-running applications with many
// unique lock keys. Safe to call concurrently with other Locker operations.
// You can call this in a ticker for example, to cleanup expired locks.
func (ml *MemoryLocker[T]) Cleanup(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var removed int
	ml.locks.Range(func(key any, value any) bool {
		lv := value.(LockValue[T])
		if lv.IsExpired() && ml.locks.CompareAndDelete(key, value) {
			removed++
		}
		return true
	})
	return removed, nil
}
