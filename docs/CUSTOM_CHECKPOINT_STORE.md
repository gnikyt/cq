# Custom Checkpoint Store

`WithCheckpoint` uses a `CheckpointStore` to persist step completion markers and optional step payload data. The built-in `MemoryCheckpointStore` works for single-instance applications, but distributed systems should use a shared store such as Redis or a database.

## Interface

Implement the `CheckpointStore` interface:

```go
type CheckpointStore interface {
	Load(ctx context.Context, key string) (checkpoint cq.Checkpoint, exists bool, err error)
	Store(ctx context.Context, key string, checkpoint cq.Checkpoint) error
	Delete(ctx context.Context, key string) error
}
```

`checkpoint.Done` controls skip behavior (`true` = step is skipped on future runs with same key).  
`checkpoint.Data` stores optional resume payload bytes.

## Key Design

Use deterministic keys that stay stable across retries/re-enqueues for the same logical workflow step:

- `tenant:<id>:workflow:<id>:step:<name>`
- `order:<id>:step:<name>`
- `idempotency:<request_id>:step:<name>`

When using `WithCheckpointNamespace("billing")`, keys are prefixed automatically (`billing:<resolved-key>`).

#### Redis Example

```go
type RedisCheckpointStore struct {
	client *redis.Client
}

func (r *RedisCheckpointStore) Load(ctx context.Context, key string) (cq.Checkpoint, bool, error) {
	out, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return cq.Checkpoint{}, false, err
	}
	if len(out) == 0 {
		return cq.Checkpoint{}, false, nil
	}
	cp := cq.Checkpoint{
		Done: out["done"] == "1",
		Data: []byte(out["data"]),
	}
	return cp, true, nil
}

func (r *RedisCheckpointStore) Store(ctx context.Context, key string, cp cq.Checkpoint) error {
	done := "0"
	if cp.Done {
		done = "1"
	}
	return r.client.HSet(ctx, key, map[string]any{
		"done": done,
		"data": string(cp.Data),
	}).Err()
}

func (r *RedisCheckpointStore) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// Usage with WithCheckpoint.
store := &RedisCheckpointStore{client: redisClient}
job := cq.WithCheckpoint(actualJob, "send-invoice", store)
```

#### SQLite Example

```go
type SQLiteCheckpointStore struct {
	db *sql.DB
}

func NewSQLiteCheckpointStore(db *sql.DB) *SQLiteCheckpointStore {
	db.Exec(`CREATE TABLE IF NOT EXISTS checkpoints (
		key TEXT PRIMARY KEY,
		done INTEGER NOT NULL DEFAULT 0,
		data BLOB
	)`)
	return &SQLiteCheckpointStore{db: db}
}

func (s *SQLiteCheckpointStore) Load(ctx context.Context, key string) (cq.Checkpoint, bool, error) {
	var done int
	var data []byte
	err := s.db.QueryRowContext(ctx, "SELECT done, data FROM checkpoints WHERE key = ?", key).Scan(&done, &data)
	if err == sql.ErrNoRows {
		return cq.Checkpoint{}, false, nil
	}
	if err != nil {
		return cq.Checkpoint{}, false, err
	}
	return cq.Checkpoint{Done: done == 1, Data: data}, true, nil
}

func (s *SQLiteCheckpointStore) Store(ctx context.Context, key string, cp cq.Checkpoint) error {
	done := 0
	if cp.Done {
		done = 1
	}
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO checkpoints (key, done, data)
		 VALUES (?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET done=excluded.done, data=excluded.data`,
		key,
		done,
		cp.Data,
	)
	return err
}

func (s *SQLiteCheckpointStore) Delete(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM checkpoints WHERE key = ?", key)
	return err
}

// Usage with WithCheckpoint.
store := NewSQLiteCheckpointStore(db)
job := cq.WithCheckpoint(actualJob, "send-invoice", store)
```

## Operational Notes

- In strict mode (default), `WithCheckpoint` returns an error when key resolution/checkpoint operations fail.
- In best-effort mode (`WithCheckpointBestEffort()`), failures fall back to executing the job without checkpoint enforcement.
- Use `WithCheckpointSaveOnFailure()` when failed runs should persist resume payload for retries.
- Inside checkpointed jobs, read/write payload via `cq.CheckpointDataFromContext(ctx)` and `cq.SetCheckpointData(ctx, data)`.
- Use `WithCheckpointDeleteOnSuccess()` when checkpoint cleanup should happen automatically after successful completion.
