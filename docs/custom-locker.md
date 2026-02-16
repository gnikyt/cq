# Custom Locker

The `WithUnique` and `WithoutOverlap` wrappers require a `Locker` implementation. The built-in `MemoryLocker` works for single-instance applications, but distributed systems need a shared lock store like Redis or a database.

## Interface

Implement the `Locker[T]` interface:

```go
type Locker[T any] interface {
	Exists(key string) bool
	Get(key string) (LockValue[T], bool)
	Acquire(key string, lock LockValue[T]) bool
	Release(key string) bool
	ForceRelease(key string)
}
```

Optionally implement `CleanableLocker[T]` if your store doesn't handle expiration automatically:

```go
type CleanableLocker[T any] interface {
	Locker[T]
	Cleanup() int // Remove expired locks, return count removed.
}
```

#### Redis Example

```go
type RedisLocker struct {
	client *redis.Client
}

func (r *RedisLocker) Exists(key string) bool {
	return r.client.Exists(context.Background(), key).Val() > 0
}

func (r *RedisLocker) Get(key string) (cq.LockValue[struct{}], bool) {
	_, err := r.client.Get(context.Background(), key).Result()
	if err == redis.Nil {
		return cq.LockValue[struct{}]{}, false
	}
	return cq.LockValue[struct{}]{}, err == nil
}

func (r *RedisLocker) Acquire(key string, lock cq.LockValue[struct{}]) bool {
	ttl := time.Until(lock.ExpiresAt)
	if ttl <= 0 {
		ttl = time.Minute // Default TTL if no expiration set.
	}
	// SET NX ensures atomic acquire.
	ok, _ := r.client.SetNX(context.Background(), key, "1", ttl).Result()
	return ok
}

func (r *RedisLocker) Release(key string) bool {
	return r.client.Del(context.Background(), key).Val() > 0
}

func (r *RedisLocker) ForceRelease(key string) {
	_ := r.Release(key)
}

// Usage with WithUnique.
locker := &RedisLocker{client: redisClient}
job := cq.WithUnique(actualJob, "order:123", 5*time.Minute, locker)
```

#### SQLite Example

```go
type SQLiteLocker struct {
	db *sql.DB
}

func NewSQLiteLocker(db *sql.DB) *SQLiteLocker {
	db.Exec(`CREATE TABLE IF NOT EXISTS locks (
		key TEXT PRIMARY KEY,
		expires_at DATETIME
	)`)
	return &SQLiteLocker{db: db}
}

func (s *SQLiteLocker) Exists(key string) bool {
	var n int
	err := s.db.QueryRow(
		"SELECT 1 FROM locks WHERE key = ? AND expires_at > ?",
		key,
		time.Now(),
	).Scan(&n)
	return err == nil
}

func (s *SQLiteLocker) Get(key string) (cq.LockValue[struct{}], bool) {
	var expiresAt time.Time
	err := s.db.QueryRow(
		"SELECT expires_at FROM locks WHERE key = ? AND expires_at > ?",
		key,
		time.Now(),
	).Scan(&expiresAt)
	if err != nil {
		return cq.LockValue[struct{}]{}, false
	}
	return cq.LockValue[struct{}]{ExpiresAt: expiresAt}, true
}

func (s *SQLiteLocker) Acquire(key string, lock cq.LockValue[struct{}]) bool {
	// Use a transaction to ensure atomicity.
	tx, err := s.db.Begin()
	if err != nil {
		return false
	}
	defer tx.Rollback()

	// Delete expired lock if exists.
	tx.Exec("DELETE FROM locks WHERE key = ? AND expires_at <= ?", key, time.Now())

	// Try to insert new lock (fails if non-expired lock exists).
	_, err = tx.Exec("INSERT INTO locks (key, expires_at) VALUES (?, ?)", key, lock.ExpiresAt)
	if err != nil {
		return false // Lock exists and is not expired.
	}

	return tx.Commit() == nil
}

func (s *SQLiteLocker) Release(key string) bool {
	res, _ := s.db.Exec("DELETE FROM locks WHERE key = ?", key)
	affected, _ := res.RowsAffected()
	return affected > 0
}

func (s *SQLiteLocker) ForceRelease(key string) {
	_ := s.Release(key)
}

func (s *SQLiteLocker) Cleanup() int {
	res, _ := s.db.Exec("DELETE FROM locks WHERE expires_at <= ?", time.Now())
	affected, _ := res.RowsAffected()
	return int(affected)
}

// Usage with WithUnique.
locker := NewSQLiteLocker(db)
job := cq.WithUnique(actualJob, "order:123", 5*time.Minute, locker)
```
