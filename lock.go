package cq

import (
	"sync"
	"time"
)

// LockValue stores a lock value and optional expiration for a key.
type LockValue[T any] struct {
	Value     T
	ExpiresAt time.Time
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

// Locker represents a lock manager.
type Locker[T any] interface {
	Exists(key string) bool
	Get(key string) (LockValue[T], bool)
	Acquire(key string, lock LockValue[T]) bool
	Release(key string) bool
	ForceRelease(key string)

	// Deprecated: Use Acquire instead. This method exists for backwards compatibility.
	// It was a typo in the original implementation.
	Aquire(key string, lock LockValue[T]) bool
}

// CleanableLocker extends Locker with cleanup capabilities for removing expired locks.
// This is optional... lockers that handle expiration automatically (like Redis) don't need it.
type CleanableLocker[T any] interface {
	Locker[T]
	Cleanup() int
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

// NewOverlapMemoryLocker creates a new MemoryLocker instance for
// use with WithoutOverlap... simply for quicker setup.
func NewOverlapMemoryLocker() *MemoryLocker[*sync.Mutex] {
	return NewMemoryLocker[*sync.Mutex]()
}

// Get loads a lock for key.
// Returns the lock and whether it exists. If missing, returns an empty LockValue.
func (ml *MemoryLocker[T]) Get(key string) (LockValue[T], bool) {
	lock, ok := ml.locks.Load(key)
	if !ok {
		return LockValue[T]{}, ok
	}
	lv := lock.(LockValue[T])
	return lv, ok
}

// Exists reports whether a lock exists for key.
func (ml *MemoryLocker[T]) Exists(key string) (ok bool) {
	_, ok = ml.Get(key)
	return
}

// Acquire attempts to claim a lock for key.
// It fails if a non-expired lock already exists; otherwise it stores the new lock.
func (ml *MemoryLocker[T]) Acquire(key string, lock LockValue[T]) bool {
	for {
		existing, ok := ml.locks.Load(key)
		if !ok {
			_, loaded := ml.locks.LoadOrStore(key, lock)
			if !loaded {
				return true // First writer wins atomically.
			}
			continue // Lost a race to create... re-check current value.
		}

		current := existing.(LockValue[T])
		if !current.IsExpired() {
			return false // Lock is still valid, skip.
		}

		// Lock exists but is expired... replace it if still unchanged.
		if ml.locks.CompareAndSwap(key, current, lock) {
			return true // Successfully replaced expired lock.
		}
	}
}

// Aquire is deprecated. Use Acquire instead.
// Deprecated: Use Acquire instead. This method exists for backwards compatibility.
// It was a typo in the original implementation.
func (ml *MemoryLocker[T]) Aquire(key string, lock LockValue[T]) bool {
	return ml.Acquire(key, lock)
}

// Release removes the lock for key.
// Returns true if a lock existed and was removed.
func (ml *MemoryLocker[T]) Release(key string) bool {
	if !ml.Exists(key) {
		return false
	}
	ml.ForceRelease(key)
	return true
}

// ForceRelease removes the lock for key without existence checks.
func (ml *MemoryLocker[T]) ForceRelease(key string) {
	ml.locks.Delete(key)
}

// Cleanup removes all expired locks from memory and returns the count of removed locks.
// This is useful for preventing memory buildup in long-running applications with many
// unique lock keys. Safe to call concurrently with other Locker operations.
// You can call this in a ticker for example, to cleanup expired locks.
func (ml *MemoryLocker[T]) Cleanup() int {
	var removed int
	ml.locks.Range(func(key any, value any) bool {
		lv := value.(LockValue[T])
		if lv.IsExpired() {
			ml.locks.Delete(key)
			removed++
		}
		return true
	})
	return removed
}
