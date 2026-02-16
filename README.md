# Composable Queue

[![Testing](https://github.com/gnikyt/cq/actions/workflows/cq.yml/badge.svg)](https://github.com/gnikyt/cq/actions/workflows/cq.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/gnikyt/cq)](https://goreportcard.com/report/github.com/gnikyt/cq)
[![GoDoc](https://godoc.org/github.com/gnikyt/cq?status.svg)](https://godoc.org/github.com/gnikyt/cq)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A lightweight, auto-scaling queue for processing Go functions as jobs. Keep jobs simple, then compose behavior with wrappers for retries, timeouts, tracing, and more.

Inspired by Bull, Pond, Ants, and more.

## Features

- Auto-scaling worker pool (min/max workers)
- Composable job wrappers (retries, timeouts, backoffs, etc.)
- Priority queue with weighted dispatch
- Job scheduler for recurring and one-time jobs
- Job metadata (ID, enqueue time, attempt count)
- Circuit breaker for fault tolerance
- Job tagging and batch tracking
- Overlap prevention and uniqueness constraints
- Tracing hooks for observability
- Zero external dependencies for core functionality

## Feature Matrix

Use this as a quick guide before diving into detailed sections.

| Capability | Primary APIs | What it solves |
| --- | --- | --- |
| Queueing and workers | `NewQueue`, `Enqueue`, `Stop` | Run background jobs with auto-scaling workers |
| Reliability | `WithRetry`, `WithRetryIf`, `WithBackoff`, `WithRecover` | Handle transient failures and panic recovery |
| Time control | `WithTimeout`, `WithDeadline`, `DelayEnqueue` | Bound execution and schedule delayed runs |
| Flow orchestration | `WithChain`, `WithPipeline`, `WithBatch` | Build multi-step and grouped workflows |
| Concurrency safety | `WithoutOverlap`, `WithUnique` | Prevent overlap and deduplicate work |
| Deferral and release | `WithRelease`, `WithReleaseSelf`, `WithRateLimitRelease` | Re-enqueue instead of blocking workers |
| Rate and fault protection | `WithRateLimit`, `WithCircuitBreaker` | Protect upstream services under load/failure |
| Observability and outcomes | `WithTracing`, `WithOutcome`, `MetaFromContext` | Track attempts, durations, and final job outcomes |
| Recovery and durability hooks | `WithEnvelopeStore`, `RecoverEnvelopes`, `StartRecoveryLoop` | Persist lifecycle events and replay jobs |
| Prioritization and scheduling | `NewPriorityQueue`, `NewScheduler` | Prioritize urgent jobs and run recurring work |

## When to Use

- **Standalone**: Process jobs in-memory without external infrastructure. Great for CLI tools, internal services, or cases where Redis/SQS is unnecessary.
- **With external queues**: Use cq as the execution engine behind SQS, Redis, RabbitMQ, or any broker that feeds jobs.
- **Embedded**: Add background processing to an existing app without introducing new operational infrastructure.

---

## Table of Contents

- [Features](#features)
- [Feature Matrix](#feature-matrix)
- [When to Use](#when-to-use)
- [Quick Start](#quick-start)
- [Wrapper Composition](#wrapper-composition)
- [Testing](#testing)
- [Usage Guide](#usage-guide)
  - [Common Recipes](#common-recipes)
  - [Queue](#queue)
  - [Envelope Persistence and Recovery](#envelope-persistence-and-recovery)
  - [Jobs](#jobs)
  - [Priority Queue](#priority-queue)
  - [Scheduler](#scheduler)
  - [Custom Locker](#custom-locker)
- [Demo](#demo)

---

## Quick Start

```go
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/gnikyt/cq"
)

func doWork(ctx context.Context) error {
	meta := cq.MetaFromContext(ctx)
	log.Printf("job %s started, queued %v ago", meta.ID, time.Since(meta.EnqueuedAt))
	time.Sleep(2 * time.Second)
	log.Printf("job %s completed", meta.ID)
	return nil
}

func main() {
	// Listen for interrupt signals.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create queue with the signal context.
	queue := cq.NewQueue(1, 10, 100, cq.WithContext(ctx))
	queue.Start()

	// Enqueue work...
	queue.Enqueue(func(ctx context.Context) error {
		return doWork(ctx)
	})

	// Wait for shutdown signal.
	<-ctx.Done()

	// Stop queue, wait for in-flight jobs to finish.
	queue.Stop(true)
}
```

## Wrapper Composition

Wrappers let you add behavior to jobs without modifying the job itself. Compose them from **innermost to outermost** - the outermost wrapper runs first and controls the flow. This keeps job logic clean while adding retries, timeouts, tracing, and error handling declaratively.

```go
job := WithOutcome(              // 4. Outermost: catches final outcome.
	WithRetry(                   // 3. Retries on error.
		WithBackoff(             // 2. Adds delay between retries.
			WithTimeout(         // 1. Innermost: runs with timeout.
				actualJob,
				5*time.Minute,
			),
			ExponentialBackoff,
		),
		3,
	),
	onComplete,
	onFail,
	onDiscard,
)
```

**Execution flow:**
1. `WithOutcome` calls `WithRetry`
2. `WithRetry` calls `WithBackoff`
3. `WithBackoff` waits (if retry > 0), then calls `WithTimeout`
4. `WithTimeout` runs `actualJob` with a 5-minute timeout
5. If `actualJob` fails, control returns up the chain for retry logic
6. After all retries, `WithOutcome` receives the final outcome

---

## Testing

`make test`

```
ok      github.com/gnikyt/cq    17.586s coverage: 91.2% of statements
```

### Benchmarks

`make bench`

```
cpu: Apple M5
BenchmarkScenarios/100Req--10kJobs-10                             7    192443179 ns/op
BenchmarkScenarios/1kReq--1kJobs-10                               7    194722393 ns/op
BenchmarkScenarios/10kReq--100Jobs-10                             7    352322048 ns/op
BenchmarkSingleSteadyState-10                               3063700        393.4 ns/op
```

---

## Usage Guide

### Common Recipes

Use these first when you want practical defaults quickly.

#### Reliable API Call (timeout + retry + backoff)

**What it does:** Wraps a job with timeout, backoff, and retry to improve reliability.
**When to use:** Calling external APIs that can fail transiently.
**Example:** See snippet below.
**Caveat:** Operationally, timeout only cancels context, so job code must honor `ctx.Done()`.

```go
job := cq.WithRetry(
	cq.WithBackoff(
		cq.WithTimeout(fetchFromAPI, 10*time.Second), // Timeout after 10s.
		cq.ExponentialBackoff, // Back off exponentially.
	),
	3, // Number of retries.
)
queue.Enqueue(job)
```

#### Idempotent Work (unique + timeout)

**What it does:** Prevents duplicate execution for a key while bounding run time.
**When to use:** Idempotent operations like order processing or resource indexing.
**Example:** See snippet below.
**Caveat:** Operationally, duplicate jobs are discarded during the uniqueness window.

```go
locker := cq.NewUniqueMemoryLocker()
job := cq.WithUnique(
	cq.WithTimeout(processOrder, 30*time.Second), // Timeout after 30s.
	"order:123", // Unique string to compare against.
	5*time.Minute, // Unique window duration.
	locker,
)
queue.Enqueue(job)
```

#### Recurring Job (scheduler)

**What it does:** Runs the same job repeatedly at a fixed interval.
**When to use:** Periodic tasks such as syncing, cleanup, or report generation.
**Example:** See snippet below.
**Caveat:** Operationally, stop the scheduler on shutdown to avoid orphaned schedules.

```go
scheduler := cq.NewScheduler(context.Background(), queue)
defer scheduler.Stop()

_ = scheduler.Every("sync-products", 10*time.Minute, syncProductsJob)
```

### Queue

#### Creating a Queue

**What it does:** Initializes a queue with worker bounds and capacity.
**When to use:** Any workload that needs in-process background execution.
**Example:** See snippet below.
**Caveat:** Operationally, the queue accepts and runs work only after `Start()`.

```go
queue := cq.NewQueue(1, 100, 1000)
queue.Start()
defer queue.Stop(true)
```

Parameters: `NewQueue(minWorkers, maxWorkers, capacity)`

#### Enqueue Methods

**What it does:** Provides blocking, non-blocking, delayed, and batch enqueue APIs.
**When to use:** Different producer behaviors and delivery timing needs.
**Example:** See snippet below.
**Caveat:** Operationally, blocking enqueue applies backpressure when capacity is full.

```go
queue.Enqueue(job)                            // Blocking.
queue.TryEnqueue(job)                         // Non-blocking, returns bool.
queue.DelayEnqueue(job, 2*time.Minute)        // Delayed.
queue.EnqueueBatch(jobs)                      // Multiple jobs.
queue.DelayEnqueueBatch(jobs, 30*time.Second) // Delayed, multiple jobs.
```

#### Metrics

**What it does:** Exposes worker and job-state counters for runtime visibility.
**When to use:** Monitoring queue throughput, backlog, and failure trends.
**Example:** See snippet below.
**Caveat:** Operationally, metrics are point-in-time snapshots and should be sampled over intervals.

```go
queue.RunningWorkers()           // Current running workers.
queue.IdleWorkers()              // Current idle workers.
queue.Capacity()                 // Job channel capacity.
queue.WorkerRange()              // (min, max) workers.
queue.TallyOf(cq.JobStateFailed) // Count by state.

// Available job states for TallyOf:
// cq.JobStateCreated   - Total jobs accepted.
// cq.JobStatePending   - Jobs waiting in the queue.
// cq.JobStateActive    - Jobs currently executing.
// cq.JobStateFailed    - Jobs completed with error.
// cq.JobStateCompleted - Jobs completed successfully.
```

#### Options

**What it does:** Customizes queue behavior through optional configuration hooks.
**When to use:** You need non-default lifecycle, panic, or envelope behavior.
**Example:** See snippet below.
**Caveat:** Operationally, custom context/cancel wiring defines how cancellation propagates and how shutdown behaves.

```go
queue := cq.NewQueue(1, 10, 100,
	cq.WithWorkerIdleTick(500*time.Millisecond),
	cq.WithContext(ctx),
	cq.WithPanicHandler(func(err any) {
		log.Printf("panic: %v", err)
	}),
)

// Available options:
// cq.WithWorkerIdleTick(d)           - Interval for idle worker cleanup (default 5s).
// cq.WithContext(ctx)                - Parent context for the queue.
// cq.WithCancelableContext(ctx, fn)  - Parent context with custom cancel function.
// cq.WithPanicHandler(fn)            - Custom handler override for job panics.
// cq.WithEnvelopeStore(store)        - Persist envelope lifecycle and recovery metadata.
```

#### Stopping

**What it does:** Shuts down queue processing with graceful or immediate behavior.
**When to use:** Service shutdown, deploy restart, or controlled termination.
**Example:** See snippet below.
**Caveat:** Operationally, `Terminate()` is abrupt and can interrupt in-flight work.

```go
queue.Stop(true)   // Wait for jobs to finish.
queue.Stop(false)  // Don't wait for queued jobs.
queue.Terminate()  // Immediate shutdown.
```

### Envelope Persistence and Recovery

Use an `EnvelopeStore` to observe queue lifecycle transitions and optionally persist/replay recoverable jobs after restart/outage. It does not have to be durable storage only... you can also use it for event-driven side effects such as DLQ routing, failure notifications, or metrics hooks. Persistence remains useful when you need durability and observability beyond in-memory queue state (process crashes, deploy restarts, audit trails, delayed/released job tracking). Implementations can use Redis, SQL/NoSQL databases, files, object storage, or non-persistent adapters that forward events.

```go
queue := cq.NewQueue(1, 10, 100, cq.WithEnvelopeStore(store))
```

`EnvelopeStore` receives lifecycle callbacks:

- `Enqueue` when accepted by queue.
- `Claim` when a worker starts execution.
- `Ack` on success.
- `Nack` on failure.
- `Reschedule` when execution is intentionally deferred.
- `Recoverable` when loading jobs to replay.

Built-in deferred wrappers automatically call `Reschedule`:

- `WithRelease` -> `cq.EnvelopeRescheduleReasonRelease`
- `WithReleaseSelf` -> `cq.EnvelopeRescheduleReasonReleaseSelf`
- `WithRateLimitRelease` -> `cq.EnvelopeRescheduleReasonRateLimit`

Use `RecoverEnvelopes` to rebuild jobs and enqueue them from persisted envelopes:

```go
registry := cq.NewEnvelopeRegistry()
registry.Register("send_invitation", func(env cq.Envelope) (cq.Job, error) {
	var payload SendInvitationPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return nil, err
	}

	baseJob := func(ctx context.Context) error {
		return sendEmail(ctx, payload)
	}
	return cq.WithRetry(
		cq.WithBackoff(baseJob, cq.ExponentialBackoff),
		3,
	), nil
})

scheduled, err := cq.RecoverEnvelopes(context.Background(), queue, store, registry, time.Now())
if err != nil {
	log.Fatal(err)
}
log.Printf("scheduled %d recovered jobs", scheduled)
```

For larger systems, use `RecoverEnvelopesWithOptions` for batched/lenient recovery and `StartRecoveryLoop` to poll recoverable jobs continuously:

```go
cancelLoop, err := cq.StartRecoveryLoop(
	context.Background(),
	5*time.Second, // Poll interval.
	queue,
	store,
	registry,
	cq.RecoverOptions{
		MaxEnvelopes:    100,
		ContinueOnError: true,
		OnError: func(env cq.Envelope, err error) {
			log.Printf("recover skipped (id=%s type=%s): %v", env.ID, env.Type, err)
		},
	},
	func(err error) {
		log.Printf("recover loop error: %v", err)
	},
)
if err != nil {
	log.Fatal(err)
}
defer cancelLoop()
```

Recovery timing behavior:

- If `Envelope.NextRunAt` is zero or in the past, recovery enqueues immediately.
- If `Envelope.NextRunAt` is in the future, recovery uses `DelayEnqueue` for that remaining duration.

#### Store Implementations

**What it does:** Shows concrete `EnvelopeStore` implementations for different persistence goals.
**When to use:** You need recovery, auditability, DLQ routing, or custom side effects.
**Example:** See the DLQ, file-backed, and DynamoDB snippets below.
**Caveat:** Operationally, use durable storage when restart recovery guarantees are required.

DLQ-only `EnvelopeStore` example:

```go
type deadLetterStore struct {
	db *sql.DB
}

func (s *deadLetterStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Claim(ctx context.Context, id string) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Ack(ctx context.Context, id string) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Recoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	return nil, nil // No replay in this implementation.
}

func (s *deadLetterStore) Nack(ctx context.Context, id string, reason error) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO dead_letter_jobs (job_id, failed_at, error_text) VALUES (?, ?, ?)`,
		id,
		time.Now().UTC(),
		reason.Error(),
	)
	return err
}

queue := cq.NewQueue(1, 10, 100, cq.WithEnvelopeStore(&deadLetterStore{db: db}))
```

Minimal file-backed `EnvelopeStore` example (JSON file):

```go
type fileEnvelopeStore struct {
	mu   sync.Mutex
	path string
}

func (s *fileEnvelopeStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	return s.update(env.ID, func(e *cq.Envelope) {
		*e = env
		e.Status = cq.EnvelopeStatusEnqueued
	})
}

func (s *fileEnvelopeStore) Claim(ctx context.Context, id string) error {
	return s.update(id, func(e *cq.Envelope) {
		e.Status = cq.EnvelopeStatusClaimed
		e.ClaimedAt = time.Now()
	})
}

func (s *fileEnvelopeStore) Ack(ctx context.Context, id string) error {
	return s.update(id, func(e *cq.Envelope) {
		e.Status = cq.EnvelopeStatusAcked
	})
}

func (s *fileEnvelopeStore) Nack(ctx context.Context, id string, reason error) error {
	return s.update(id, func(e *cq.Envelope) {
		e.Status = cq.EnvelopeStatusNacked
		e.LastError = reason.Error()
	})
}

func (s *fileEnvelopeStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return s.update(id, func(e *cq.Envelope) {
		e.Status = cq.EnvelopeStatusEnqueued
		e.NextRunAt = nextRunAt
		e.LastError = reason
	})
}

func (s *fileEnvelopeStore) Recoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := s.readAll()
	if err != nil {
		return nil, err
	}

	out := make([]cq.Envelope, 0, len(records))
	for _, env := range records {
		if env.Status == cq.EnvelopeStatusAcked {
			continue
		}
		if !env.NextRunAt.IsZero() && env.NextRunAt.After(now) {
			continue
		}
		out = append(out, env)
	}
	return out, nil
}

// Helpers to implement:
// - readAll(): loads map[string]cq.Envelope from s.path (json.Unmarshal).
// - writeAll(): persists map[string]cq.Envelope to s.path (json.MarshalIndent).
// - update(id, fn): loads map, applies fn on map[id], then writes map back.
```

DynamoDB-backed `EnvelopeStore` example:

```go
type dynamoEnvelopeStore struct {
	client *dynamodb.Client
	table  string
}

func (s *dynamoEnvelopeStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	env.Status = cq.EnvelopeStatusEnqueued
	return s.putEnvelope(ctx, env)
}

func (s *dynamoEnvelopeStore) Claim(ctx context.Context, id string) error {
	return s.updateStatus(ctx, id, cq.EnvelopeStatusClaimed, time.Now(), nil, "")
}

func (s *dynamoEnvelopeStore) Ack(ctx context.Context, id string) error {
	return s.updateStatus(ctx, id, cq.EnvelopeStatusAcked, time.Time{}, nil, "")
}

func (s *dynamoEnvelopeStore) Nack(ctx context.Context, id string, reason error) error {
	return s.updateStatus(ctx, id, cq.EnvelopeStatusNacked, time.Time{}, nil, reason.Error())
}

func (s *dynamoEnvelopeStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return s.updateStatus(ctx, id, cq.EnvelopeStatusEnqueued, time.Time{}, &nextRunAt, reason)
}

func (s *dynamoEnvelopeStore) Recoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	// Query by status/time using a GSI such as:
	// GSI1PK = status, GSI1SK = next_run_at (unix timestamp).
	//
	// Recoverable rule:
	// - exclude acked
	// - include next_run_at <= now (or missing/zero)
	return s.queryRecoverable(ctx, now)
}

func (s *dynamoEnvelopeStore) putEnvelope(ctx context.Context, env cq.Envelope) error {
	item, err := attributevalue.MarshalMap(env)
	if err != nil {
		return err
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	})
	return err
}

func (s *dynamoEnvelopeStore) updateStatus(
	ctx context.Context,
	id string,
	status string,
	claimedAt time.Time,
	nextRunAt *time.Time,
	lastError string,
) error {
	// Use UpdateItem to set status and optional fields atomically.
	// Example updates:
	// - status
	// - claimed_at (on claim)
	// - next_run_at + last_error (on reschedule/nack)
	// Keep this single-write to avoid partial state transitions.
	return nil
}

func (s *dynamoEnvelopeStore) queryRecoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	// Use Query (preferred with GSI) or Scan (small datasets).
	// Unmarshal each item back into cq.Envelope.
	return nil, nil
}

store := &dynamoEnvelopeStore{
	client: dynamoClient,
	table:  "cq_envelopes",
}
queue := cq.NewQueue(1, 10, 100, cq.WithEnvelopeStore(store))
```

Suggested table configuration:

- Partition key: `id` (string)
- Attributes: `type`, `status`, `payload`, `enqueued_at`, `claimed_at`, `next_run_at`, `last_error`
- Optional GSI for recovery polling: (`status`, `next_run_at`)

### Jobs

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

### Priority Queue

Dispatch jobs based on priority levels using weighted fair queuing. Higher priority jobs get more execution slots per tick, but lower priorities still make progress. Useful when some jobs are more time-sensitive than others.

```go
queue := cq.NewQueue(2, 10, 100)
queue.Start()

pq := cq.NewPriorityQueue(queue, 50)
defer pq.Stop(true)

pq.Enqueue(criticalJob, cq.PriorityHighest)
pq.Enqueue(normalJob, cq.PriorityMedium)
pq.Enqueue(cleanupJob, cq.PriorityLowest)
```

Priority levels: `PriorityHighest`, `PriorityHigh`, `PriorityMedium`, `PriorityLow`, `PriorityLowest`

Default weights (attempts per tick): `5:3:2:1:1`. This means per dispatch cycle, the highest priority queue gets 5 job attempts, then the next gets 3, then 2, then 1, then 1 for the lowest.

#### Custom Weights

**What it does:** Customizes dispatch share across priority levels.
**When to use:** You need workload-specific fairness or latency tuning.
**Example:** See snippet below.
**Caveat:** Operationally, aggressive high-priority weights can starve lower priorities.

```go
// Using raw counts.
pq := cq.NewPriorityQueue(queue, 50,
	cq.WithWeighting(
		cq.NumberWeight(10), // highest: 10 attempts per tick.
		cq.NumberWeight(5),  // high: 5 attempts per tick.
		cq.NumberWeight(3),  // medium: 3 attempts per tick.
		cq.NumberWeight(2),  // low: 2 attempts per tick.
		cq.NumberWeight(1),  // lowest: 1 attempt per tick.
	),
)

// Using percentages (converted to counts from total of 12).
pq := cq.NewPriorityQueue(queue, 50,
	cq.WithWeighting(
		cq.PercentWeight(50), // highest: 6 attempts (50% of 12).
		cq.PercentWeight(25), // high: 3 attempts (25% of 12).
		cq.PercentWeight(15), // medium: 1 attempt (15% of 12, min 1).
		cq.PercentWeight(5),  // low: 1 attempt (min 1).
		cq.PercentWeight(5),  // lowest: 1 attempt (min 1).
	),
)
```

#### Drain Before Stop

**What it does:** Flushes buffered priority jobs into the base queue before shutdown.
**When to use:** Graceful shutdown where queued work must be preserved.
**Example:** See snippet below.
**Caveat:** Operationally, draining trades job safety for longer shutdown time.

```go
drained := pq.Drain()
pq.Stop(true)
```

### Scheduler

Schedule jobs to run at intervals or specific times. Useful for recurring tasks like cleanup, syncs, or reports, and one-time delayed operations.

```go
queue := cq.NewQueue(2, 10, 100)
queue.Start()

scheduler := cq.NewScheduler(context.Background(), queue)
defer scheduler.Stop()

scheduler.Every("cleanup", 10*time.Minute, cleanupJob)
scheduler.At("reminder", time.Now().Add(1*time.Hour), reminderJob)

scheduler.Has("cleanup")
scheduler.Remove("cleanup")
scheduler.List()
scheduler.Count()
```

#### Cron-like Scheduling

**What it does:** Implements cron behavior by recursively scheduling next run times.
**When to use:** You need cron expressions without built-in cron parser support.
**Example:** See snippet below.
**Caveat:** Operationally, unhandled cron parse/reschedule errors can stop future runs silently.

```go
// ScheduleCron uses recursion to simulate cron behavior.
// After each execution, the job re-schedules itself for the next cron tick.
func ScheduleCron(s *cq.Scheduler, id, expr string, job cq.Job) error {
	gron := gronx.New()
	if !gron.IsValid(expr) {
		return fmt.Errorf("SchedulerCron: invalid cron: %s", expr)
	}

	// Calculate the first run time.
	nextRun, _ := gronx.NextTick(expr, true)

	// Create a self-scheduling job wrapper.
	var scheduled cq.Job
	scheduled = func(ctx context.Context) error {
		err := job(ctx) // Execute the actual job.
		// After completion, calculate and schedule the next run.
		if next, e := gronx.NextTick(expr, false); e == nil {
			s.At(id, next, scheduled) // Recursively re-schedule itself.
		}
		return err
	}

	// Schedule the first run.
	return s.At(id, nextRun, scheduled)
}

ScheduleCron(scheduler, "daily", "0 2 * * *", dailyJob)
```

### Custom Locker

The `WithUnique` and `WithoutOverlap` wrappers require a `Locker` implementation. The built-in `MemoryLocker` works for single-instance applications, but distributed systems need a shared lock store like Redis or a database.

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

## Demo

```bash
go run example/web_direct.go
```

```bash
for i in {1..500}; do
  curl -s -X POST localhost:8080/order -d '{"demo":"yes"}' -H "Content-Type: application/json"
done
```

![](example/example.gif)
