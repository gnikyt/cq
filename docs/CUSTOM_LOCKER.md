# Custom Locker

`MemoryLocker` is suitable for one process. Implement the smallest interface
needed by a wrapper when using Redis, SQL, or another shared backend.

## Core Interface

```go
type Locker[T any] interface {
	Acquire(ctx context.Context, key string, lock LockValue[T]) (acquired bool, err error)
	Release(ctx context.Context, key string) (released bool, err error)
}

type LockReader[T any] interface {
	Get(ctx context.Context, key string) (lock LockValue[T], exists bool, err error)
}

type ReadLocker[T any] interface {
	Locker[T]
	LockReader[T]
}
```

`Acquire` must be atomic. Return `(false, nil)` for ordinary contention and a
non-nil error for backend failures. Methods should respect context cancellation
and deadlines.

`WithoutOverlap` requires `Locker[T]`. `WithUnique` and `WithUniqueWindow`
require `ReadLocker[T]` because they inspect existing lock state. Optional
capabilities are discovered through type assertions:

```go
type RenewableLocker interface {
	Touch(ctx context.Context, key, token string, expiresAt time.Time) (bool, error)
	ReleaseIfOwner(ctx context.Context, key, token string) (bool, error)
}

type ForceReleaser interface {
	ForceRelease(ctx context.Context, key string) error
}

type CleanableLocker interface {
	Cleanup(ctx context.Context) (removed int, err error)
}
```

`ManagedLocker[T]` is a convenience composite for backends implementing every
capability:

```go
type ManagedLocker[T any] interface {
	ReadLocker[T]
	ForceReleaser
	RenewableLocker
	CleanableLocker
}
```

Consumers should accept the smallest interface they need rather than requiring
`ManagedLocker`.

## Redis Example

```go
type RedisLocker struct {
	client *redis.Client
}

func (r *RedisLocker) Get(ctx context.Context, key string) (cq.LockValue[struct{}], bool, error) {
	token, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return cq.LockValue[struct{}]{}, false, nil
	}
	if err != nil {
		return cq.LockValue[struct{}]{}, false, err
	}
	return cq.LockValue[struct{}]{Token: token}, true, nil
}

func (r *RedisLocker) Acquire(ctx context.Context, key string, lock cq.LockValue[struct{}]) (bool, error) {
	ttl := time.Until(lock.ExpiresAt)
	if lock.ExpiresAt.IsZero() {
		ttl = 0
	}
	return r.client.SetNX(ctx, key, lock.Token, ttl).Result()
}

func (r *RedisLocker) Release(ctx context.Context, key string) (bool, error) {
	removed, err := r.client.Del(ctx, key).Result()
	return removed > 0, err
}
```

For owner-safe renewal, implement `Touch` and `ReleaseIfOwner` with an atomic
operation that verifies `LockValue.Token`.

## Behavior

- `WithUnique` releases its lock after execution.
- `WithUniqueWindow` leaves its lock until the configured window expires.
- `WithoutOverlap` waits for atomic acquisition while respecting job
  cancellation.
- Under contention-try behavior, `WithoutOverlap` makes one acquisition attempt.
- Deferred release receives a non-cancelled cleanup context so cancellation does
  not leak remote locks.
- Release failures are joined with the job result instead of being discarded.

The cleanup context preserves values but has no cancellation or deadline.
External implementations should enforce an appropriate backend or client
timeout.

`WithoutOverlap` intentionally holds its lock until the job returns. During
normal cancellation or shutdown, its deferred cleanup still releases the lock.
An abrupt process crash cannot run that cleanup. In-memory locks disappear with
the process, but a distributed lock may remain orphaned. Handling that failure
mode is a backend or operational policy, such as explicit stale-lock cleanup.
Use lease-based `WithUnique` behavior when automatic expiration and renewal are
part of the desired job semantics.

For external lockers, tune acquisition polling with an option:

```go
job := cq.WithoutOverlap(
	actualJob,
	"account:123",
	locker,
	cq.WithOverlapRetryInterval(250*time.Millisecond),
)
```

The default retry interval is `10ms`. Non-positive option values use the
default.
Contention-try behavior makes one attempt and does not use the retry interval.

Use `TouchLock(ctx, ttl)` inside a running unique job when the backend
implements `RenewableLocker`.
