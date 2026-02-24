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
- Optional queue lifecycle hooks (enqueue/start/success/failure/reschedule)
- Job tagging and batch tracking
- Overlap prevention and uniqueness constraints
- Tracing hooks for observability
- Zero external dependencies for core functionality
- Extensible envelope system for optional persistence

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
| Observability and outcomes | `WithTracing`, `WithOutcome`, `WithHooks`, `MetaFromContext` | Track attempts, durations, and queue lifecycle transitions |
| Recovery and durability hooks | `WithEnvelopeStore`, `EnvelopeHandler`, `EnqueueEnvelope`, `RegisterEnvelopeHandler`, `RecoverEnvelopes`, `RecoverEnvelopeByID`, `StartRecoveryLoop` | Persist lifecycle events and replay jobs |
| Prioritization and scheduling | `NewPriorityQueue`, `NewScheduler` | Prioritize urgent jobs and run recurring work |

## When to Use

- **Standalone**: Process jobs in-memory without external infrastructure. Great for CLI tools, internal services, or cases where Redis/SQS is unnecessary.
- **With external queues**: Use cq as the execution engine behind SQS, Redis, RabbitMQ, or any broker that feeds jobs.
- **Embedded**: Add background processing to an existing app without introducing new operational infrastructure.

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

## Common Recipes

Use these first when you want practical defaults quickly.

### Reliable API Call (timeout + retry + backoff)

```go
job := cq.WithRetry(
	cq.WithBackoff(
		cq.WithTimeout(fetchFromAPI, 10*time.Second),
		cq.ExponentialBackoff,
	),
	3,
)
queue.Enqueue(job)
```

### Idempotent Work (unique + timeout)

```go
locker := cq.NewUniqueMemoryLocker()
job := cq.WithUnique(
	cq.WithTimeout(processOrder, 30*time.Second),
	"order:123",
	5*time.Minute,
	locker,
)
queue.Enqueue(job)
```

### Recurring Job (scheduler)

```go
scheduler := cq.NewScheduler(context.Background(), queue)
defer scheduler.Stop()

_ = scheduler.Every("sync-products", 10*time.Minute, syncProductsJob)
```

### Replay-Ready Envelope Payloads

Use envelope handlers when you want durability and replay. With `WithEnvelopeStore(...)` configured,
`cq` persists envelope lifecycle snapshots (type, payload, and status transitions) so jobs can be recovered and replayed after process restarts or failures.

Use a first-class `EnvelopeHandler` for enqueue and recovery:

```go
orderCreate := cq.EnvelopeHandler[OrderPayload]{
	Type:  "process_order", // Job type identifier.
	Codec: cq.EnvelopeJSONCodec[OrderPayload](), // Codec for encoding and decoding the payload.
	Handler: func(ctx context.Context, payload OrderPayload) error {
		log.Printf("processing from %s (order_id=%s)", payload.Source, payload.OrderID)
		return nil
	}, // Handler which is job-compatible, accepting the payload.
}

payload := OrderPayload{OrderID: "123", Source: "web"}

// Enqueue with envelope persisted at enqueue-time.
if err := cq.EnqueueEnvelope(queue, orderCreate, payload); err != nil {
	log.Fatal(err)
}

// or ...

// Batch enqueue with one handler and multiple payloads.
if err := cq.EnqueueEnvelopeBatch(queue, orderCreate, []OrderPayload{
	{OrderID: "124", Source: "web"},
	{OrderID: "125", Source: "mobile"},
}); err != nil {
	log.Fatal(err)
}

// or ...

// Non-blocking batch enqueue returns accepted count.
accepted, err := cq.TryEnqueueEnvelopeBatch(queue, orderCreate, []OrderPayload{
	{OrderID: "126", Source: "web"},
	{OrderID: "127", Source: "mobile"},
})
if err != nil {
	log.Fatal(err)
}
log.Printf("accepted %d", accepted)

// Register the same handler for recovery/replay.
cq.RegisterEnvelopeHandler(registry, orderCreate)
```

## Queue

### Creating a Queue

```go
queue := cq.NewQueue(1, 100, 1000)
queue.Start()
defer queue.Stop(true)
```

Parameters: `NewQueue(minWorkers, maxWorkers, capacity)`.

### Enqueue Methods

```go
// For normal jobs.
queue.Enqueue(job)                            // Blocking.
queue.TryEnqueue(job)                         // Non-blocking, returns bool.
queue.DelayEnqueue(job, 2*time.Minute)        // Delayed.
queue.EnqueueBatch(jobs)                      // Multiple jobs.
queue.DelayEnqueueBatch(jobs, 30*time.Second) // Delayed, multiple jobs.

// For envelopes.
cq.EnqueueEnvelope(queue, handler, payload)                          // Blocking.
cq.TryEnqueueEnvelope(queue, handler, payload)                       // Non-blocking, returns (bool, error).
cq.DelayEnqueueEnvelope(queue, handler, payload, 2*time.Minute)      // Delayed.
cq.EnqueueEnvelopeBatch(queue, handler, payloads)                    // Multiple payloads, one handler.
cq.TryEnqueueEnvelopeBatch(queue, handler, payloads)                 // Non-blocking batch, returns (accepted, error).
cq.DelayEnqueueEnvelopeBatch(queue, handler, payloads, 30*time.Second) // Delayed, multiple payloads.
```

### Metrics

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

### Options

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
// cq.WithIDGenerator(fn)             - Override fallback job ID generation.
```

### Stopping

```go
queue.Stop(true)   // Wait for jobs to finish.
queue.Stop(false)  // Don't wait for queued jobs.
queue.Terminate()  // Immediate shutdown.
```

## Documentation

For detailed usage and advanced features, see the following guides:

- **[Job Wrappers](docs/JOB_WRAPPERS.md)** - Complete reference for all job wrappers including retries, timeouts, tracing, rate limiting, circuit breakers, and custom wrappers
- **[Queue Options](docs/QUEUE_OPTIONS.md)** - Queue configuration options including context, panic handling, hooks, envelope persistence, and custom ID generation
- **[Envelope Persistence](docs/ENVELOPE_PERSISTENCE.md)** - Persist and recover jobs using envelope stores with examples for DLQ, file-based, and DynamoDB implementations
- **[Priority Queue](docs/PRIORITY_QUEUE.md)** - Weighted fair queuing with custom priority levels and dispatch strategies
- **[Scheduler](docs/SCHEDULER.md)** - Recurring and one-time job scheduling with cron-like behavior
- **[Custom Locker](docs/CUSTOM_LOCKER.md)** - Distributed lock implementations for `WithUnique` and `WithoutOverlap` with Redis and SQLite examples

## Testing

`make test`

```
ok      github.com/gnikyt/cq            19.013s coverage: 89.5% of statements
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
