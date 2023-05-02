package cq

import (
	"sync"
	"time"
)

// LockValue reprecents the value an expiration of a lock for a key.
type LockValue[T interface{}] struct {
	Value     T
	ExpiresAt time.Time
}

// Locker reprecents a lock manager.
type Locker[T interface{}] interface {
	Exists(key string) bool
	Get(key string) (LockValue[T], bool)
	Aquire(key string, lock LockValue[T]) bool
	Release(key string) bool
	ForceRelease(key string)
}

// MemoryLocker is memory-based Locker implementation utilizing sync map.
type MemoryLocker[T any] struct {
	locks sync.Map
}

// NewMemoryLocker creates a new MemoryLocker instance for a type.
// The type, T, reprecents the type of Value for a LockValue.
func NewMemoryLocker[T interface{}]() *MemoryLocker[T] {
	return &MemoryLocker[T]{}
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
		exp := lock.ExpiresAt
		if exp.IsZero() {
			// No expire time for lock, assume forever.
			exp = time.Now().Add(1 * time.Second)
		}
		if exp.After(time.Now()) {
			// Not yet expired.
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
