package cq

import (
	"sync"
	"time"
)

// LockValue reprecents the value an expiration of a lock for a key.
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

// Locker reprecents a lock manager.
type Locker[T any] interface {
	Exists(key string) bool
	Get(key string) (LockValue[T], bool)
	Aquire(key string, lock LockValue[T]) bool
	Release(key string) bool
	ForceRelease(key string)
}

// CleanableLocker extends Locker with cleanup capabilities for removing expired locks.
// This is optional... lockers that handle expiration automatically (like Redis) don't need it.
type CleanableLocker[T any] interface {
	Locker[T]
	Cleanup() int
}

// MemoryLocker is memory-based Locker implementation utilizing sync map.
type MemoryLocker[T any] struct {
	locks sync.Map
}

// NewMemoryLocker creates a new MemoryLocker instance for a type.
// The type, T, reprecents the type of Value for a LockValue.
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

// Get will load a lock from key.
// It will return a lock and a boolean reprecenting existance.
// Should a lock not exist for the key, an empty LockValue will be
// returned.
func (ml *MemoryLocker[T]) Get(key string) (LockValue[T], bool) {
	lock, ok := ml.locks.Load(key)
	if !ok {
		return LockValue[T]{}, ok
	}
	lv := lock.(LockValue[T])
	return lv, ok
}

// Exists will check if a LockValue exists for a key.
func (ml *MemoryLocker[T]) Exists(key string) (ok bool) {
	_, ok = ml.Get(key)
	return
}

// Aquire will attempt to aquire a lock for a given key.
// If a lock exists, and the expiration is in future, it will not
// allow for a new lock.
// If a lock exists, and the expiration is past, it will allow
// for a new lock.
// If a lock does not exist, it will allow for a new lock.
func (ml *MemoryLocker[T]) Aquire(key string, lock LockValue[T]) bool {
	if lock, ok := ml.Get(key); ok {
		if !lock.IsExpired() {
			return false
		}
	}
	ml.locks.Store(key, lock)
	return true
}

// Release will attempt to remove the lock for a given key.
// If a lock exists for a given key, it will release (remove) it
// and return true.
// If a lock does not exist for a given key, it will return false.
func (ml *MemoryLocker[T]) Release(key string) bool {
	if !ml.Exists(key) {
		return false
	}
	ml.ForceRelease(key)
	return true
}

// ForceRelease will release (remove) a lock for a given key.
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
