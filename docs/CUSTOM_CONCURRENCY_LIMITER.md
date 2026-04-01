# Custom Key Concurrency Limiter

`WithConcurrencyByKey` accepts any `KeyConcurrencyLimiter` implementation.
The built-in `NewMemoryKeyConcurrencyLimiter` is a simple in-memory setup and works well for a single instance, but multi-instance usually need a distributed store.

## Interface

Implement the `KeyConcurrencyLimiter` interface:

```go
type KeyConcurrencyLimiter interface {
	Acquire(key string) error
	Release(key string)
}
```

## SQLite Example

Use a small table and transaction to reserve/release slots atomically.

```go
type SQLiteKeyConcurrencyLimiter struct {
	db    *sql.DB
	limit int
}

func NewSQLiteKeyConcurrencyLimiter(db *sql.DB, limit int) *SQLiteKeyConcurrencyLimiter {
	_, _ = db.Exec(`
		CREATE TABLE IF NOT EXISTS key_concurrency (
			key TEXT PRIMARY KEY,
			count INTEGER NOT NULL
		)
	`)
	return &SQLiteKeyConcurrencyLimiter{db: db, limit: limit}
}

func (l *SQLiteKeyConcurrencyLimiter) Acquire(key string) error {
	if l.limit <= 0 {
		return cq.ErrConcurrencyByKeyInvalidLimit
	}

	tx, err := l.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var current int
	err = tx.QueryRow("SELECT count FROM key_concurrency WHERE key = ?", key).Scan(&current)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if current >= l.limit {
		return cq.ErrConcurrencyByKeyLimited
	}

	if err == sql.ErrNoRows {
		_, err = tx.Exec("INSERT INTO key_concurrency (key, count) VALUES (?, 1)", key)
	} else {
		_, err = tx.Exec("UPDATE key_concurrency SET count = count + 1 WHERE key = ?", key)
	}
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (l *SQLiteKeyConcurrencyLimiter) Release(key string) {
	tx, err := l.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	var current int
	if err := tx.QueryRow("SELECT count FROM key_concurrency WHERE key = ?", key).Scan(&current); err != nil {
		return
	}
	if current <= 1 {
		_, _ = tx.Exec("DELETE FROM key_concurrency WHERE key = ?", key)
	} else {
		_, _ = tx.Exec("UPDATE key_concurrency SET count = count - 1 WHERE key = ?", key)
	}

	_ = tx.Commit()
}

// Usage with WithConcurrencyByKey.
limiter := NewSQLiteKeyConcurrencyLimiter(db, 5)
job := cq.WithConcurrencyByKey(actualJob, "customer:123", limiter)
```
