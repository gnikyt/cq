package cq

import (
	"testing"
	"time"
)

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
		if ok := ml.Aquire("exists", lock); !ok {
			t.Error("Aquire() = false, want true")
		}
		// Should be found.
		lv, ok := ml.Get("exists")
		if lv.ExpiresAt != lock.ExpiresAt || !ok {
			t.Errorf("Get() = (%v, false), want (%v, true)", lv, lock)
		}
	})
}

func TestMemoryLockerAquire(t *testing.T) {
	ml := NewMemoryLocker[string]()
	lock := LockValue[string]{
		ExpiresAt: time.Now().Add(1 * time.Minute),
		Value:     "",
	}

	// Should allow since none exist yet.
	if ok := ml.Aquire("test", lock); !ok {
		t.Error("Aquire() = false, want true")
	}
	// Should not allow since lock now exists.
	if ok := ml.Aquire("test", lock); ok {
		t.Error("Aquire() = true, want false")
	}
}

func TestMemoryLockerAquireExpired(t *testing.T) {
	ml := NewMemoryLocker[string]()
	lexpp := LockValue[string]{
		ExpiresAt: time.Now().Add(-1 * time.Minute), // Expireation in past.
		Value:     "",
	}
	lexpf := LockValue[string]{
		ExpiresAt: time.Now().Add(1 * time.Minute), // Expiration in future.
		Value:     "",
	}

	// Should allow since none exist yet.
	if ok := ml.Aquire("test", lexpp); !ok {
		t.Error("Aquire() = false, want true")
	}
	// Should allow since lock should be expired.
	if ok := ml.Aquire("test", lexpf); !ok {
		t.Error("Aquire() = false, want true")
	}
}

func TestMemoryLockerRelease(t *testing.T) {
	ml := NewMemoryLocker[string]()
	lock := LockValue[string]{
		ExpiresAt: time.Now().Add(1 * time.Minute),
		Value:     "",
	}

	// Should allow since none exist yet.
	if ok := ml.Aquire("test", lock); !ok {
		t.Error("Aquire() = false, want true")
	}
	// Should allow.
	if ok := ml.Release("test"); !ok {
		t.Error("Release() = false, want true")
	}
}
