package cq

import (
	"testing"
	"time"
)

// CustomLocker is a test implementation without Cleanup to verify backward compatibility.
type CustomLocker struct {
	locks map[string]LockValue[string]
}

func (c *CustomLocker) Exists(key string) bool {
	_, ok := c.locks[key]
	return ok
}

func (c *CustomLocker) Get(key string) (LockValue[string], bool) {
	lv, ok := c.locks[key]
	return lv, ok
}

func (c *CustomLocker) Acquire(key string, lock LockValue[string]) bool {
	if lv, ok := c.locks[key]; ok && !lv.IsExpired() {
		return false
	}
	c.locks[key] = lock
	return true
}

// Aquire is deprecated. Use Acquire instead.
func (c *CustomLocker) Aquire(key string, lock LockValue[string]) bool {
	return c.Acquire(key, lock)
}

func (c *CustomLocker) Release(key string) bool {
	if !c.Exists(key) {
		return false
	}
	delete(c.locks, key)
	return true
}

func (c *CustomLocker) ForceRelease(key string) {
	delete(c.locks, key)
}

func TestMemoryLockerExists(t *testing.T) {
	ml := NewMemoryLocker[string]()
	if ok := ml.Exists("non-existant"); ok {
		t.Error("Exists() = true, want false")
	}
}

func TestMemoryLockerGet(t *testing.T) {
	ml := NewMemoryLocker[string]()

	t.Run("non-existant", func(t *testing.T) {
		// Should be not found, thus no real expire time.
		lock, ok := ml.Get("non-existant")
		if !lock.ExpiresAt.IsZero() || ok {
			t.Errorf("Get() = (%v, true), want %v, false", lock.ExpiresAt, time.Time{})
		}
	})
	t.Run("exists", func(t *testing.T) {
		// Should allow since none exist yet.
		tn := time.Now().Add(1 * time.Minute)
		lock := LockValue[string]{
			ExpiresAt: tn,
			Value:     "",
		}
		if ok := ml.Acquire("exists", lock); !ok {
			t.Error("Acquire() = false, want true")
		}
		// Should be found.
		lv, ok := ml.Get("exists")
		if lv.ExpiresAt != lock.ExpiresAt || !ok {
			t.Errorf("Get() = (%v, false), want (%v, true)", lv, lock)
		}
	})
}

func TestMemoryLockerAcquire(t *testing.T) {
	ml := NewMemoryLocker[string]()
	lock := LockValue[string]{
		ExpiresAt: time.Now().Add(1 * time.Minute),
		Value:     "",
	}

	// Should allow since none exist yet.
	if ok := ml.Acquire("test", lock); !ok {
		t.Error("Acquire() = false, want true")
	}
	// Should not allow since lock now exists.
	if ok := ml.Acquire("test", lock); ok {
		t.Error("Acquire() = true, want false")
	}
}

func TestMemoryLockerAcquireExpired(t *testing.T) {
	ml := NewMemoryLocker[string]()
	lexpp := LockValue[string]{
		ExpiresAt: time.Now().Add(-1 * time.Minute), // Expiration in past.
		Value:     "",
	}
	lexpf := LockValue[string]{
		ExpiresAt: time.Now().Add(1 * time.Minute), // Expiration in future.
		Value:     "",
	}

	// Should allow since none exist yet.
	if ok := ml.Acquire("test", lexpp); !ok {
		t.Error("Acquire() = false, want true")
	}
	// Should allow since lock should be expired.
	if ok := ml.Acquire("test", lexpf); !ok {
		t.Error("Acquire() = false, want true")
	}
}

func TestMemoryLockerRelease(t *testing.T) {
	ml := NewMemoryLocker[string]()
	lock := LockValue[string]{
		ExpiresAt: time.Now().Add(1 * time.Minute),
		Value:     "",
	}

	// Should allow since none exist yet.
	if ok := ml.Acquire("test", lock); !ok {
		t.Error("Acquire() = false, want true")
	}
	// Should allow.
	if ok := ml.Release("test"); !ok {
		t.Error("Release() = false, want true")
	}
}

func TestMemoryLockerCleanup(t *testing.T) {
	t.Run("removes_expired_locks", func(t *testing.T) {
		ml := NewMemoryLocker[string]()

		// Add expired locks.
		ml.Acquire("expired1", LockValue[string]{
			ExpiresAt: time.Now().Add(-1 * time.Minute),
			Value:     "test1",
		})
		ml.Acquire("expired2", LockValue[string]{
			ExpiresAt: time.Now().Add(-2 * time.Minute),
			Value:     "test2",
		})

		// Add active locks.
		ml.Acquire("active1", LockValue[string]{
			ExpiresAt: time.Now().Add(1 * time.Minute),
			Value:     "test3",
		})
		ml.Acquire("active2", LockValue[string]{
			ExpiresAt: time.Now().Add(2 * time.Minute),
			Value:     "test4",
		})

		// Cleanup should remove 2 expired locks.
		removed := ml.Cleanup()
		if removed != 2 {
			t.Errorf("Cleanup(): got %d removed, want 2", removed)
		}

		// Expired locks should be gone.
		if ml.Exists("expired1") {
			t.Error("Cleanup(): expired1 should not exist")
		}
		if ml.Exists("expired2") {
			t.Error("Cleanup(): expired2 should not exist")
		}

		// Active locks should still exist.
		if !ml.Exists("active1") {
			t.Error("Cleanup(): active1 should exist")
		}
		if !ml.Exists("active2") {
			t.Error("Cleanup(): active2 should exist")
		}
	})

	t.Run("returns_zero_when_no_expired", func(t *testing.T) {
		ml := NewMemoryLocker[string]()

		// Add only active locks.
		ml.Acquire("active1", LockValue[string]{
			ExpiresAt: time.Now().Add(1 * time.Minute),
			Value:     "test1",
		})
		ml.Acquire("active2", LockValue[string]{
			ExpiresAt: time.Now().Add(2 * time.Minute),
			Value:     "test2",
		})

		// Cleanup should remove nothing.
		removed := ml.Cleanup()
		if removed != 0 {
			t.Errorf("Cleanup(): got %d removed, want 0", removed)
		}

		// All locks should still exist.
		if !ml.Exists("active1") || !ml.Exists("active2") {
			t.Error("Cleanup(): active locks should not be removed")
		}
	})

	t.Run("handles_empty_locker", func(t *testing.T) {
		ml := NewMemoryLocker[string]()

		// Cleanup on empty locker should return 0.
		removed := ml.Cleanup()
		if removed != 0 {
			t.Errorf("Cleanup(): got %d removed, want 0 (empty locker)", removed)
		}
	})

	t.Run("handles_zero_time_locks", func(t *testing.T) {
		ml := NewMemoryLocker[string]()

		// Zero time locks never expire.
		ml.Acquire("forever", LockValue[string]{
			ExpiresAt: time.Time{},
			Value:     "test",
		})

		// Cleanup should not remove it.
		removed := ml.Cleanup()
		if removed != 0 {
			t.Errorf("Cleanup(): got %d removed, want 0 (zero time locks don't expire)", removed)
		}

		if !ml.Exists("forever") {
			t.Error("Cleanup(): zero time lock should not be removed")
		}
	})
}

func TestCustomLockerBackwardCompatibility(t *testing.T) {
	// Custom locker without Cleanup should still work as Locker[T].
	var locker Locker[string] = &CustomLocker{
		locks: make(map[string]LockValue[string]),
	}

	// Should work fine without Cleanup method.
	lock := LockValue[string]{
		ExpiresAt: time.Now().Add(1 * time.Minute),
		Value:     "test",
	}

	if !locker.Acquire("test", lock) {
		t.Error("CustomLocker: Acquire() should work")
	}

	if !locker.Exists("test") {
		t.Error("CustomLocker: Exists() should work")
	}

	if !locker.Release("test") {
		t.Error("CustomLocker: Release() should work")
	}
}

func TestDeprecatedAquireBackwardCompatibility(t *testing.T) {
	ml := NewMemoryLocker[string]()
	lock := LockValue[string]{
		ExpiresAt: time.Now().Add(1 * time.Minute),
		Value:     "test",
	}

	// Deprecated Aquire should still work.
	if ok := ml.Aquire("test", lock); !ok {
		t.Error("Aquire() (deprecated) = false, want true")
	}

	// Should not allow since lock now exists.
	if ok := ml.Aquire("test", lock); ok {
		t.Error("Aquire() (deprecated) = true, want false")
	}
}

func TestCleanableLockerInterface(t *testing.T) {
	// MemoryLocker should implement CleanableLocker.
	var locker CleanableLocker[string] = NewMemoryLocker[string]()

	lock := LockValue[string]{
		ExpiresAt: time.Now().Add(-1 * time.Minute),
		Value:     "test",
	}

	locker.Acquire("expired", lock)

	// Should be able to call Cleanup.
	removed := locker.Cleanup()
	if removed != 1 {
		t.Errorf("CleanableLocker: Cleanup(): got %d removed, want 1", removed)
	}
}

func TestCleanupWithTypeAssertion(t *testing.T) {
	// Demonstrate optional cleanup pattern.
	var locker Locker[string] = NewMemoryLocker[string]()

	lock := LockValue[string]{
		ExpiresAt: time.Now().Add(-1 * time.Minute),
		Value:     "test",
	}

	locker.Acquire("expired", lock)

	// Optional cleanup using type assertion.
	if cleaner, ok := locker.(CleanableLocker[string]); ok {
		removed := cleaner.Cleanup()
		if removed != 1 {
			t.Errorf("Type assertion cleanup: got %d removed, want 1", removed)
		}
	} else {
		t.Error("MemoryLocker should implement CleanableLocker")
	}
}
