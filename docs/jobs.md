# Job Wrappers

Jobs are functions with signature `func(ctx context.Context) error`.

Use this quick wrapper index to choose the right building block:

| Goal | Wrappers |
| --- | --- |
| Retry transient failures | `WithRetry`, `WithRetryIf`, `WithBackoff` |
| Bound runtime | `WithTimeout`, `WithDeadline` |
| Skip or deduplicate work | `WithSkipIf`, `WithUnique`, `WithoutOverlap` |
| Observe outcomes | `WithOutcome`, `WithTracing` |
| Build workflows | `WithChain`, `WithPipeline`, `WithBatch` |
| Defer and requeue | `WithRelease`, `WithReleaseSelf`, `WithRateLimitRelease` |
| Protect dependencies | `WithRateLimit`, `WithCircuitBreaker` |

#### Basic Function

**What it does:** Defines the base `Job` shape and a cancellation-aware implementation.
**When to use:** Any job implementation.
**Example:** See snippet below.
**Caveat:** Operationally, jobs stop promptly only if they check `ctx.Err()` or use context-aware operations.

```go
job := func(ctx context.Context) error {
	// Check for cancellation before doing work.
	if ctx.Err() != nil {
		return ctx.Err() // Was cancelled.
	}
	return doWork(ctx)
}
queue.Enqueue(job)
```

#### Job Metadata

**What it does:** Exposes per-job metadata through context (ID, enqueue time, attempt).
**When to use:** Structured logging, tracing correlation, retry-aware logic.
**Example:** See snippet below.
**Caveat:** Operationally, `Attempt` starts at 0 and increments only with retry wrappers.

```go
job := func(ctx context.Context) error {
	meta := cq.MetaFromContext(ctx)
	log.Printf(
		"job %s, attempt %d, queued %v ago",
		meta.ID, meta.Attempt, time.Since(meta.EnqueuedAt),
	)
	return doWork(ctx)
}
queue.Enqueue(job)
```

The `JobMeta` struct contains:
- `ID` - Unique identifier for the job
- `EnqueuedAt` - Timestamp when the job was enqueued
- `Attempt` - Current retry attempt (0-indexed, incremented by `WithRetry`)

#### Retries

**What it does:** Re-runs failed jobs for a bounded number of attempts.
**When to use:** Transient failures such as timeout, throttling, or temporary outages.
**Example:** See snippets below (`WithRetry` and `WithRetryIf`).
**Caveat:** Operationally, retries can duplicate side effects unless work is idempotent.

```go
// Retry 3 times.
job := cq.WithRetry(func(ctx context.Context) error {
	return fetchEndpoint(ctx)
}, 3)
queue.Enqueue(job)
```

Conditional retries allow you to retry only on specific errors:

```go
job := cq.WithRetryIf(func(ctx context.Context) error {
	err := callExternalAPI(ctx)
	if err == nil {
		return nil
	}
	if isValidationError(err) {
		return cq.AsPermanent(err) // Don't retry validation failures.
	}
	if isNetworkTimeout(err) {
		return cq.AsRetryable(err) // Explicitly marked retryable.
	}
	return err // Intentionally unwrapped (may still be retryable by predicate).
}, 5, func(err error) bool {
	if errors.Is(err, cq.ErrRetryable) {
		return true // Already marked as retryable.
	}

	// Retry if HTTP status is "too many requests".
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusTooManyRequests
})
```

#### Backoff

**What it does:** Adds delay strategy between retries to reduce pressure on dependencies.
**When to use:** Any retry loop that should avoid immediate hammering.
**Example:** See snippet below.
**Caveat:** Operationally, backoff has no effect unless composed with retries.

```go
// Retry 3 times with exponential backoff.
job := cq.WithRetry(
	cq.WithBackoff(actualJob, cq.ExponentialBackoff),
	3,
)
queue.Enqueue(job)
```

Built-in backoff functions: `ExponentialBackoff`, `FibonacciBackoff`, `JitterBackoff`

#### Outcome Handler

**What it does:** Runs callbacks for completion, failure, or discard outcomes.
**When to use:** Metrics, DLQ forwarding, notifications, and audit hooks.
**Example:** See snippets below.
**Caveat:** Operationally, callbacks run inline with job completion... throughput impact depends on callback implementation.

```go
job := cq.WithOutcome(
	actualJob,
	func() {
		log.Println("success")
	},
	func(err error) {
		log.Printf("failed: %v", err)
		// Example: send to SQS DLQ.
	},
	nil, // onDiscarded.
)
queue.Enqueue(job)
```

To handle discarded jobs (for example, idempotent duplicates), pass `onDiscarded`:

```go
job := cq.WithOutcome(
	func(ctx context.Context) error {
		if isDuplicateWork(ctx) {
			return cq.AsDiscard(errors.New("already processed"))
		}
		return actualJob(ctx)
	},
	nil, // onCompleted.
	nil, // onFailed.
	func(err error) {
		log.Printf("discarded: %v", err)
	},
)
queue.Enqueue(job)
```

To access job metadata in handlers, capture it inside the job:

```go
job := func(ctx context.Context) error {
	meta := cq.MetaFromContext(ctx)
	return cq.WithOutcome(
		actualJob,
		func() {
			log.Printf("job %s completed", meta.ID)
		},
		func(err error) {
			log.Printf("job %s failed: %v", meta.ID, err)
		},
		nil, // onDiscarded.
	)(ctx)
}
queue.Enqueue(job)
```

#### Outcome Markers

**What it does:** Marks errors as retryable, permanent, or discardable explicitly.
**When to use:** You need deterministic behavior with `WithRetry`/`WithRetryIf`, and explicit discard semantics with `WithOutcome`.
**Example:** See snippet below.
**Caveat:** Operationally, markers are consumed by retry wrappers (`WithRetry`, `WithRetryIf`) and by `WithOutcome` for discard handling.

```go
job := cq.WithRetryIf(
	func(ctx context.Context) error {
		err := callExternalAPI(ctx)
		if err == nil {
			return nil
		}

		var httpErr *HTTPError
		if errors.As(err, &httpErr) {
			switch {
			case httpErr.StatusCode == http.StatusTooManyRequests:
				return cq.AsRetryable(err)
			case httpErr.StatusCode >= http.StatusInternalServerError:
				return cq.AsRetryable(err)
			case httpErr.StatusCode >= http.StatusBadRequest:
				return cq.AsPermanent(err)
			}
		}

		return cq.AsPermanent(err)
	},
	5,
	func(err error) bool {
		return errors.Is(err, cq.ErrRetryable)
	},
)
queue.Enqueue(job)
```

Available outcome markers:

- `cq.ErrRetryable` / `cq.AsRetryable(err)` - Transient errors that may succeed on retry (example: network timeouts, rate limits, 5xx server errors).
- `cq.ErrPermanent` / `cq.AsPermanent(err)` - Errors that won't be fixed by retrying (example: validation errors, 4xx client errors, malformed input).
- `cq.ErrDiscard` / `cq.AsDiscard(err)` - Errors that should be discarded (example: duplicate processing, already completed, idempotent no-op).

#### Tracing

**What it does:** Emits lifecycle signals for timing and success/failure instrumentation.
**When to use:** Integrating observability platforms or custom telemetry.
**Example:** See snippets below.
**Caveat:** Operationally, wrapper placement changes measured duration scope (single attempt vs total retries).

```go
type myHook struct{}

func (h myHook) Start(ctx context.Context, name string) context.Context {
	return ctx
}

func (h myHook) Success(ctx context.Context, d time.Duration) {
	log.Printf("success: %s", d)
}

func (h myHook) Failure(ctx context.Context, err error, d time.Duration) {
	log.Printf("failure: %v (%s)", err, d)
}

job := cq.WithTracing(actualJob, "sync-products", myHook{})
queue.Enqueue(job)
```

Place tracing as the outermost wrapper to capture total execution time including retries:

```go
// Traceable job with up to 3 retry attempts on a 5 second timeout deadline.
job := cq.WithTracing(
	cq.WithRetry(
		cq.WithTimeout(actualJob, 5*time.Second),
		3,
	),
	"sync-products",
	myHook{},
)
```

To trace each retry attempt individually, place tracing inside the retry instead.

#### Skip If

**What it does:** Conditionally bypasses execution when a predicate returns true.
**When to use:** Feature flags, maintenance mode, or unmet preconditions.
**Example:** See snippet below.
**Caveat:** Operationally, skipped jobs return `nil` and count as intentional no-ops.

```go
job := cq.WithSkipIf(actualJob, func(ctx context.Context) bool {
	return !shouldProcess()
})
queue.Enqueue(job)
```

#### Timeout

**What it does:** Cancels a job context if execution exceeds a duration.
**When to use:** Preventing runaway jobs from occupying workers indefinitely.
**Example:** See snippet below.
**Caveat:** Operationally, timeout/deadline signals cancel the context, but job code stops only where it checks the context.

```go
job := cq.WithTimeout(actualJob, 5*time.Minute)
queue.Enqueue(job)
```

#### Deadline

**What it does:** Cancels a job context at a fixed absolute time.
**When to use:** Time-sensitive work with a hard business cutoff.
**Example:** See snippet below.
**Caveat:** Operationally, queue wait time consumes deadline budget, but that is expected given its a deadline.

```go
deadline := time.Date(2025, 12, 25, 16, 0, 0, 0, time.Local)
job := cq.WithDeadline(actualJob, deadline)
queue.Enqueue(job)
```

#### Overlap Prevention

**What it does:** Ensures only one job with the same key runs at a time.
**When to use:** Non-overlapping work such as account sync or balance mutation.
**Example:** See snippets below.
**Caveat:** Operationally, lock contention blocks workers... use `WithUnique` when drop-on-duplicate is preferred.

```go
locker := cq.NewOverlapMemoryLocker()
job := cq.WithoutOverlap(actualJob, "account-123", locker)
queue.Enqueue(job)
```

To cap how long the overlap lock is held, compose `WithTimeout` inside `WithoutOverlap`:

```go
locker := cq.NewOverlapMemoryLocker()
job := cq.WithoutOverlap(
	cq.WithTimeout(actualJob, 5*time.Minute),
	"account-123",
	locker,
)
queue.Enqueue(job)
```

This releases the overlap lock when the timeout wrapper returns, but note the underlying job goroutine may continue running until it respects `ctx.Done()`.

#### Unique Jobs

**What it does:** Deduplicates jobs by key within a configured time window.
**When to use:** Idempotent workloads where repeated work should be skipped.
**Example:** See snippets below.
**Caveat:** Operationally, uniqueness controls deduplication only, not run duration.

```go
locker := cq.NewUniqueMemoryLocker()
job := cq.WithUnique(actualJob, "index-products", 1*time.Hour, locker)
queue.Enqueue(job)
```

Combine with timeout to limit both uniqueness and execution time:

```go
locker := cq.NewUniqueMemoryLocker()
job := cq.WithUnique(
	cq.WithTimeout(actualJob, 1*time.Hour),
	"index-products",
	1*time.Hour,
	locker,
)
queue.Enqueue(job)
```

#### Chains

**What it does:** Executes a fixed sequence of jobs and stops on first error.
**When to use:** Ordered workflows with step dependencies.
**Example:** See snippet below.
**Caveat:** Operationally, chains provide no data handoff... use `WithPipeline` when sharing values.

```go
job := cq.WithChain(step1, step2, step3)
queue.Enqueue(job)
```

#### Pipeline

**What it does:** Executes sequential steps with typed channel-based data passing.
**When to use:** Multi-step flows where output from one step feeds the next.
**Example:** See snippet below.
**Caveat:** Operationally, pipeline steps must coordinate channel sends/receives... unmatched operations can block execution.

```go
step1 := func(ch chan int) cq.Job {
	return func(ctx context.Context) error {
		ch <- 42
		return nil
	}
}

step2 := func(ch chan int) cq.Job {
	return func(ctx context.Context) error {
		value := <-ch
		return process(value)
	}
}

job := cq.WithPipeline(step1, step2)
queue.Enqueue(job)
```

#### Batch

**What it does:** Wraps a job set with group-level progress and completion callbacks.
**When to use:** Bulk operations where aggregate completion and error tracking matter.
**Example:** See snippets below.
**Caveat:** Operationally, expensive batch callbacks can throttle completion throughput.

```go
jobs := []cq.Job{job1, job2, job3}
batchJobs, state := cq.WithBatch(
	jobs,
	func(errs []error) {
		log.Printf("done: %d errors", len(errs))
	}, // onComplete: fires once when all finish.
	func(done int, total int) {
		log.Printf(
			"%d/%d (%.0f%%)",
			done, total, float64(done)/float64(total)*100,
		)
	} // onProgress: fires after each job.
)
queue.EnqueueBatch(batchJobs)
```

To access job metadata in batch callbacks, use a sentinel error type to wrap errors with the job ID. The `onComplete` callback receives all errors from the batch, allowing you to extract IDs using `errors.As`:

```go
// Define a sentinel error type.
type JobError struct {
	JobID string
	Err   error
}

func (e *JobError) Error() string {
	return fmt.Sprintf("job %s: %v", e.JobID, e.Err)
}

func (e *JobError) Unwrap() error {
	return e.Err
}

// Wrap errors with job metadata.
jobs := make([]cq.Job, len(items))
for i, item := range items {
	jobs[i] = func(ctx context.Context) error {
		meta := cq.MetaFromContext(ctx)
		if err := process(item); err != nil {
			return &JobError{JobID: meta.ID, Err: err}
		}
		return nil
	}
}
batchJobs, _ := cq.WithBatch(
	jobs,
	func(errs []error) {
		for _, err := range errs {
			var jobErr *JobError
			if errors.As(err, &jobErr) {
				log.Printf("job %s failed: %v", jobErr.JobID, jobErr.Err)
			}
		}
	},
	nil,
)
queue.EnqueueBatch(batchJobs)
```

#### Release

**What it does:** Re-enqueues jobs after delay when a predicate-matched error occurs.
**When to use:** Retry-later semantics such as rate limits or temporary upstream failures.
**Example:** See snippet below.
**Caveat:** Operationally, releases can run indefinitely without `maxReleases` bounds.

```go
job := cq.WithRelease(
	actualJob,
	queue,             // Queue to re-enqueue into.
	30*time.Second,    // Delay before re-enqueue.
	3,                 // Max releases before giving up.
	func(err error) bool {
		return errors.Is(err, ErrRateLimited)
	}, // Predicate: which errors trigger release.
)
queue.Enqueue(job)
```

#### Release Self

**What it does:** Lets job code request its own delayed re-enqueue.
**When to use:** Jobs that decide at runtime to defer themselves.
**Example:** See snippets below.
**Caveat:** Operationally, multiple release requests in one run use last-write-wins delay.

```go
job := cq.WithReleaseSelf(func(ctx context.Context) error {
	// If upstream is throttling, ask to try again later.
	if isRateLimitedNow() {
		cq.RequestRelease(ctx, 30*time.Second)
		return nil
	}

	return doWork(ctx)
}, queue, 3) // maxReleases (0 = unlimited).

queue.Enqueue(job)
```

You can call `RequestRelease` multiple times in a single run to refine the delay. Only one release is scheduled for that run, and the last request wins:

```go
job := cq.WithReleaseSelf(func(ctx context.Context) error {
	cq.RequestRelease(ctx, 30*time.Second) // Initial estimate.
	if shouldRetrySooner() {
		cq.RequestRelease(ctx, 5*time.Second) // Last write wins.
	}
	return nil
}, queue, 3)

queue.Enqueue(job)
```

Logic:

- If a release request is made and release budget allows, the job is delayed and re-enqueued, and this run returns `nil`.
- If both an error and release request occur, release wins while budget allows.
- Multiple requests in one run use last-write-wins delay.
- `RequestRelease` returns `false` when no `WithReleaseSelf` context is present.

#### Recover

**What it does:** Converts panics in job code into returned errors.
**When to use:** You need custom panic handling per job path.
**Example:** See snippet below.
**Caveat:** Operationally, wrapper-level recovery plus queue recovery can duplicate error reporting.

```go
job := cq.WithRecover(func(ctx context.Context) error {
	panic("oops")
})
queue.Enqueue(job)
```

#### Tagged

**What it does:** Attaches tags so related jobs can be tracked or canceled together.
**When to use:** Multi-tenant workflows or operation-wide cancellation.
**Example:** See snippet below.
**Caveat:** Operationally, cancellation scope includes all jobs matching the tag.

```go
registry := cq.NewJobRegistry()
job := cq.WithTagged(actualJob, registry, "user:123", "export")
queue.Enqueue(job)

registry.CancelForTag("user:123")
// or:
registry.CancelForTag("export")
```

#### Rate Limit

**What it does:** Applies token-bucket throttling before job execution using Go builtins.
**When to use:** Respecting external quotas and protecting shared dependencies.
**Example:** See snippets below.
**Caveat:** Operationally, `WithRateLimit` blocks workers unless release mode is used.

```go
limiter := rate.NewLimiter(10, 5) // 10 per second, burst of 5.
job := cq.WithRateLimit(actualJob, limiter)
queue.Enqueue(job)
```

Use `WithRateLimitRelease` when you want to free workers instead of blocking them while waiting for limiter tokens:

```go
limiter := rate.NewLimiter(10, 5)
job := cq.WithRateLimitRelease(actualJob, limiter, queue, 3)
queue.Enqueue(job)
```

`WithRateLimitRelease` re-enqueues the job using the limiter reservation delay when rate limited, and returns `nil` immediately so workers can keep processing other jobs. `maxReleases` controls how many times this defer/re-enqueue can happen (`0` means unlimited). Once exhausted, it falls back to blocking `WithRateLimit` behavior.

#### Circuit Breaker

**What it does:** Short-circuits calls after consecutive failures to protect dependencies.
**When to use:** Isolating unstable upstreams and reducing cascading failures.
**Example:** See snippet below.
**Caveat:** Operationally, poor breaker thresholds can cause false opens or delayed protection.

The circuit has three states:
- **Closed**: Normal operation, jobs execute. Opens after threshold consecutive failures.
- **Open**: Jobs rejected immediately with `cq.ErrCircuitOpen`. Transitions to half-open after cooldown.
- **Half-open**: Allows one job through to test recovery. Success closes the circuit; failure reopens it.

Half-open is enabled by default. Use `cb.SetHalfOpen(false)` to disable it (circuit goes directly from open to closed after cooldown). Use `cb.State()` to check the current state.

```go
// Shared circuit breaker across all jobs calling a payment API.
paymentCB := cq.NewCircuitBreaker(5, 30*time.Second)

for _, orderID := range orderIDs {
	job := cq.WithCircuitBreaker(func(ctx context.Context) error {
		return processPayment(orderID)
	}, paymentCB)
	queue.Enqueue(job)
}

// If 5 consecutive jobs fail, the circuit opens.
// Remaining jobs return cq.ErrCircuitOpen immediately without attempting.
// After 30s cooldown, circuit closes and allows one job through to test recovery.
// If that job succeeds, circuit stays closed and failure count resets.
// If it fails, circuit opens again for another 30s cooldown.
```

#### Custom Wrapper

**What it does:** Shows how to build composable custom wrappers with the decorator pattern.
**When to use:** You need behavior not covered by built-in wrappers.
**Example:** See snippet below.
**Caveat:** Operationally, custom wrappers must preserve context propagation and error semantics.

```go
func withLogging(job cq.Job) cq.Job {
	return func(ctx context.Context) error {
		log.Println("starting")
		err := job(ctx)
		log.Printf("finished: %v", err)
		return err
	}
}

job := withLogging(actualJob)
queue.Enqueue(job)
```
