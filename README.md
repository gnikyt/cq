# Composable Queue

[![Testing](https://github.com/gnikyt/cq/actions/workflows/cq.yml/badge.svg)](https://github.com/gnikyt/cq/actions/workflows/cq.yml)

An auto-scaling queue that processes functions as jobs. Jobs can be simple functions or composed with job wrappers.

Wrappers support retries, timeouts, deadlines, delays, backoffs, overlap prevention, uniqueness, batch operations, job release/retry, job tagging, priority queues, and custom wrappers. It also includes an optional priority queue on top of the base queue system.

This is inspired from great projects such as Bull, Pond, Ants, and more.

* [Quick Start](#quick-start)
* [Testing](#testing)
  - [Benchmarks](#benchmarks)
* [Building](#building)
* [Examples](#examples)
  - [Queue](#queue)
    + [Tallies](#tallies)
    + [Enqueue](#enqueue)
    + [Idle worker tick](#idle-worker-tick)
  - [Panic handler](#panic-handler)
  - [Context](#context)
  - [Stopping](#stopping)
  - [Jobs](#jobs)
    + [Function](#function)
    + [Retries](#retries)
    + [Backoff](#backoff)
    + [Result catch](#result-catch)
    + [Timeout](#timeout)
    + [Deadline](#deadline)
    + [Overlaps](#overlaps)
    + [Unique](#unique)
    + [Chains](#chains)
    + [Pipeline](#pipeline)
    + [Batch](#batch)
    + [Release](#release)
    + [Recover](#recover)
    + [Tagged](#tagged)
    + [Your own](#your-own)
  - [Priority Queue](#priority-queue)
    + [Basic Usage](#basic-usage)
    + [Priority Levels](#priority-levels)
    + [Weighting](#weighting)
  - [Scheduler](#scheduler)
    + [Basic Usage](#basic-usage-1)
    + [Recurring Jobs](#recurring-jobs)
    + [One-Time Jobs](#one-time-jobs)
    + [Managing Jobs](#managing-jobs)
    + [Job IDs](#job-ids)
    + [Lifecycle](#lifecycle)
    + [Cron-like Scheduling (Optional)](#cron-like-scheduling-optional)
  - [Demo](#demo)

## Quick Start

```go
package main

import (
  "context"
  "fmt"

  "github.com/gnikyt/cq"
)

func main() {
  queue := cq.NewQueue(1, 10, 100)
  queue.Start()
  defer queue.Stop(true) // Wait for queued/running jobs before exit.

  queue.Enqueue(func(ctx context.Context) error {
    fmt.Println("processing job")
    return nil
  })
}
```

## Testing

`make test`

Example result:

    ok      github.com/gnikyt/cq    15.832s coverage: 90.3% of statements
    PASS

Current coverage is around 90% on the core paths. Some tests are time-based and may be improved in the future.

### Benchmarks

`make bench`

Load benchmarks run these request/job mixes: `100x10,000`, `1,000x1,000`, and `10,000x100`.

Example result (Darwin ARM64):

    cpu: Apple M5
    BenchmarkScenarios/100Req--10kJobs-10                             7    192443179 ns/op    107037 B/op      235 allocs/op
    BenchmarkScenarios/1kReq--1kJobs-10                               7    194722393 ns/op    130395 B/op     1180 allocs/op
    BenchmarkScenarios/10kReq--100Jobs-10                             7    352322048 ns/op    542281 B/op    11390 allocs/op
    BenchmarkScenariosSteadyState/100Req--10kJobs-10                  8    221076688 ns/op     18163 B/op      118 allocs/op
    BenchmarkScenariosSteadyState/1kReq--1kJobs-10                    6    354617493 ns/op     85909 B/op     1088 allocs/op
    BenchmarkScenariosSteadyState/10kReq--100Jobs-10                  4    414824562 ns/op    886610 B/op    12358 allocs/op
    BenchmarkSingle-10                                             80572        14407 ns/op     82768 B/op       15 allocs/op
    BenchmarkSingleSteadyState-10                                3063700        393.4 ns/op        32 B/op        2 allocs/op
    PASS

## Building

`make build`

The binary is built at `dist/cq`.

## Examples

### Queue

A queue requires minimum/maximum worker counts and a capacity. Additional options are also available.

```go
// Create a queue with 1 always-running worker.
// 100 maximum workers.
// Capacity of 1000 jobs.
queue = NewQueue(1, 100, 1000)
queue.Start()
// true = wait for jobs to finish before exiting.
defer queue.Stop(true)
```

#### Tallies

You can pull current stats for the queue.

* `RunningWorkers() int` returns the number of running workers.
  - Example: `fmt.Printf("%d running workers", queue.RunningWorkers())`
* `IdleWorkers() int` returns the number of idle workers.
  - Example: `fmt.Printf("%d idle workers", queue.IdleWorkers())`
* `Capacity() int` returns the configured job capacity.
  - Example: `fmt.Printf("%d capacity for jobs", queue.Capacity())`
* `WorkerRange() (int, int)` returns the configured minimum and maximum workers.
  - Example: `fmt.Printf("%d max workers", queue.WorkerRange()[1])`
* `TallyOf(JobState) int` returns the number of jobs for a given state.
  - Example: `fmt.Printf("%d failed jobs", queue.TallyOf(JobStateFailed))`

#### Enqueue

You can push jobs to the queue in a few ways.

* `Enqueue(Job)` submits a job and may block if no worker can be started and the jobs channel is full.
  - Example: `Enqueue(job)`
* `TryEnqueue(Job) bool` attempts to submit a job without blocking and returns whether it was accepted.
  - Example: `ok := TryEnqueue(job)`
* `DelayEnqueue(Job, delay time.Duration)` submits a job after the given delay in a separate goroutine.
  - Example: `DelayEnqueue(job, time.Duration(2 * time.Minute))`
* `EnqueueBatch([]Job)` submits multiple jobs at once.
  - Example: `EnqueueBatch(jobs)`
* `DelayEnqueueBatch([]Job, delay time.Duration)` submits multiple jobs after a delay.
  - Example: `DelayEnqueueBatch(jobs, time.Duration(30 * time.Second))`

#### Idle worker tick

You can configure how often to remove idle workers.

```go
// Check every 500ms for idle workers to remove.
queue := NewQueue(
  2,
  5,
  100,
  WithWorkerIdleTick(time.Duration(500 * time.Millisecond)),
)
```

### Panic handler

Configure a panic handler so the queue does not crash when a panic happens.

A panic can come from job code itself. Use `WithPanicHandler` to centralize panic handling.

```go
queue := NewQueue(
  2,
  5,
  100,
  WithPanicHandler(func(err any) {
    // err can be a string, error, etc.
    // Use type assertions if you need different handling by type.
    log.Errorf("Job or queue failed: %v", err)
  }),
)
```

### Context

Pass a custom context to the queue.

By default, the queue configures a cancelable background context. A common use case is stopping after a certain time period or in response to SIGTERM/SIGINT.

```go
queue := NewQueue(2, 5, 100, WithContext(yourCtx))

// Or...

queue := NewQueue(2, 5, 100, WithCancelableContext(yourCtx, yourCtxCancel))
```

### Stopping

You can stop a queue in a few ways.

* `Stop(true)` gracefully stops the queue and waits for queued/running jobs, then worker goroutines, then performs cleanup.
* `Stop(false)` gracefully stops the queue without waiting for queued jobs to finish first.
* `Terminate()` forces an immediate shutdown and does not wait for jobs or worker goroutines to finish.

See `example/web_direct.go` for an example on how you can configure sigterm/sigint context to stop the queue.

### Jobs

Setup your jobs in any way you please, as long as it matches the signature of `func(ctx context.Context) error`.

You can use basic functions, composed functions, struct methods, and so on.

Built-in wrappers can be composed on top of each other to match your requirements. Any function with the job signature can be used, so you can also build your own wrappers.

The examples below show each built-in wrapper. The job code and names are simplified for demonstration.

#### Function

A basic function.

```go
job := func(ss SomeService) error {
  return func(ctx context.Context) error {
    ss.doWork()
  }
}
queue.Enqueue(job(ss))

// ...

job2 := func(ctx context.Context) error {
  log.Info("Basic function")
  return nil
}
queue.Enqueue(job2)
```

#### Retries

Retry, on error, to a maximum number of retries.

An example use case is retrying an HTTP fetch job X times because the server is possibly down.

```go
retries := 2
job := WithRetry(func (ctx context.Context) error {
  req := fetchSomeEndpoint()
  if err != nil {
    return fmt.Errorf("special job: %w", err)
  }
  return finalize(req)
}, retries)
```

#### Backoff

To be used with `WithRetry`, adds a backoff delay before recalling the job.

An example use case is retrying an HTTP fetch job X times at delayed intervals because the server is possibly down.

```go
retries := 4
backoff := JitterBackoff // ExponentialBackoff is default if `nil` is provided to `WithBackoff`.
job := WithRetry(WithBackoff(func (ctx context.Context) error {
  req := fetchSomeEndpoint()
  if err != nil {
    return fmt.Errorf("special job: %w", err)
  }
  return finalize(req)
}, backoff), retries)
queue.Enqueue(job)
```

There are three built-in backoff implementations, with the ability to write your own given you match the `BackoffFunc` signature.

* `ExponentialBackoff` will exponentially backoff based upon the number of retries.
  - 1 retry = 1s, 2 retries = 1s, 3 retries = 2s, 4 retries = 4s, 5 retries = 8s...
* `FibonacciBackoff` will create a Fibonacci sequence based upon the number of retries.
  - 1 retry = 0s, 2 retries = 1s, 3 retries = 1s, 4 retries = 2s, 5 retries = 3s...
* `JitterBackoff` will randomly generate a backoff based upon the number of retries.
  - 1 retry = 717ms, 2 retries = 903ms, 3 retries = 10s, 4 retries = 4s, 5 retries = 53s...

#### Result catch

Capture job result, completed or failed.

An example use case is sending a Slack message for successful jobs and moving failed jobs to a database table for later reprocessing.

```go
// We will create a job with an ID.
// We will capture its result of success and failure.
// If failed, we will move the job to a table to process later.
job := func(id int) error {
  return WithResultHandler(
    WithRetry(func (ctx context.Context) error {
      req := fetchSomeEndpoint()
      if err != nil {
        return fmt.Errorf("special job: %w", err)
      }
      return finalize(req)
    }, 2),
    // On complete (optional), use nil to disregard.
    func () {
      log.Infof("Special job #%d is done successfully!", id)
    },
    // On failure (optional), use nil to disregard.
    func (err error) {
      moveToFailureTable(id, err)
    },
  )
}
queue.Enqueue(job(id))
```

#### Timeout

Timeout a job after running it for a duration.

An example use case is generating a report from multiple data sources and timing it out if it takes too long.

```go
// Job must complete 5 minutes after running.
job := WithTimeout(func (ctx context.Context) error {
  return someExpensiveLongWork()
}, time.Duration(5 * time.Minute))
queue.Enqueue(job)
```

#### Deadline

Complete a job by a certain datetime.

An example use case is sending orders to a same-day shipping service where labels must be received by a specific datetime.

```go
// Job must complete by today at 16:50.
tn := time.Now()
job := WithDeadline(func (ctx context.Context) error {
  return someExpensiveLongWork()
}, time.Date(tn.Year(), tn.Month(), tn.Day(), 16, 50, 0, 0, time.Local))
queue.Enqueue(job)
```

#### Overlaps

Prevent multiple jobs with the same key from running at the same time.

An example use case is modifying an account balance in sequence to ensure each update is applied correctly.

```go
// Create a new memory-based lock manager which holds mutexes.
locker := NewOverlapMemoryLock() // NewMemoryLock[*sync.Mutex]()
key := strings.Join([]string{"account-amount-", user.ID()}) 
job := WithoutOverlap(func (ctx context.Context) error {
  amount := amountForUser()
  decrement := 4
  if amount < decrement {
    // Cannot remove any more from the amount.
    return nil
  }
  amount -= decrement
}, key, locker)
queue.Enqueue(job)
```

#### Unique

Allow only one job of a key to be run during a window of time or until completed.

An example use case is a search indexing job that should only run once per hour because indexing is expensive. If duplicate jobs arrive during that hour, they are discarded.

```go
// Create a new memory-based lock manager.
locker := NewUniqueMemoryLock()      // NewMemoryLock[struct{}]()
window := time.Duration(1*time.Hour) // No other job of this key can process within an hour.
job := WithUnique(func(ctx context.Context) error {
  return doSomeWork()
}, "job-key-here", window, locker)
queue.Enqueue(job)
```

#### Chains

Chain jobs together, where each job must complete, error-free, before running the next.

An example use case is creating a customer welcome flow with several steps and notifications, where each step must complete before the next starts.

Should you want data to pass from one job to another, you can utilize a buffered channel on your own, or use `WithPipeline`.

```go
job := func (ctx context.Context) error {
  something()
  return sendWelcomeEmail()
}
job2 := func (ctx context.Context) error {
  return findOldSubscriptionsAndClear()
}
job3 := func (ctx context.Context) error {
  coupon := createCoupon()
  return sendUpsellEmail(coupon.code)
}
queue.Enqueue(WithChain(job, job2, job3))
```

#### Pipeline

Chain jobs together, where each job must complete, error-free, before running the next, but additionally, include a buffered channel to pass data from one job to the next.

This is identical to `WithChain`, however it will internally create a buffered channel with a capacity of 1. Each job must be wrapped to accept the channel as a parameter. If this built-in functionality does not meet your needs, you can always create a channel outside on your own and utilize `WithChain` instead.

An example use case is processing a file upload and passing file details between steps.

```go
job := func (file File) func(chan FileInfo) {
  return func (results chan FileInfo) Job {
    return func(ctx context.Context) error {
      file, _ := uploadFile(file.file, file.user)
      results <- file
      return nil
    }
  }
}
job2 := func(results chan FileInfo) Job {
  info := <- results
  return saveToDatabase(info.UserId, info.SourceUrl, info.MimeType)
}

// WithPipeline is a generic function, but Go knows based on the
// job's parameters that the type of buffered channel to create
// is `chan FileInfo`. 
queue.Enqueue(WithChain(job(fileFromUser), job2)) // WithChain[FileInfo](job, job2)
```

#### Batch

Track multiple jobs as a single batch with callbacks for completion and progress. When all jobs finish, the completion callback receives any errors that occurred (empty slice if all succeeded).

An example use case is processing a large dataset and notifying on completion while tracking per-item progress.

```go
jobs := []Job{
  func(ctx context.Context) error {
    return processRecord(1)
  },
  func(ctx context.Context) error {
    return processRecord(2)
  },
  func(ctx context.Context) error {
    return processRecord(3)
  },
}

batchJobs, batchState := WithBatch(
  jobs,
  func(errs []error) {
    if len(errs) == 0 {
      fmt.Println("All jobs completed successfully!")
    } else {
      fmt.Printf("Batch completed with %d errors: %v\n", len(errs), errs)
    }
  },
  func(completed, total int) {
    fmt.Printf("Progress: %d/%d (%.0f%%)\n", completed, total, float64(completed)/float64(total)*100)
  },
)

// Method 1: Enqueue all batch jobs.
for _, job := range batchJobs {
    queue.Enqueue(job)
}
// OR, Method 2: use EnqueueBatch for convenience.
queue.EnqueueBatch(batchJobs)

// Check batch state.
fmt.Printf(
  "Completed: %d, Failed: %d\n", 
  batchState.CompletedJobs.Load(), 
  batchState.FailedJobs.Load(),
)
```

#### Release

Re-enqueue a job after a delay if it returns a specific error. Useful for handling temporary failures like network issues or rate limits.

An example use case is retrying an HTTP request when a service returns 503 (temporarily unavailable) or 429 (rate limited).

```go
var ErrServiceUnavailable = errors.New("service unavailable")
var ErrRateLimited = errors.New("rate limited")

job := WithRelease(
  func(ctx context.Context) error {
    resp, err := http.Get("https://api.example.com/data")
    if err != nil || resp.StatusCode == 503 {
      return ErrServiceUnavailable
    }
    if resp.StatusCode == 429 {
      return ErrRateLimited
    }
    // Process response...
    return nil
  },
  queue,
  30*time.Second,  // Wait 30 seconds before re-enqueueing.
  3,               // Maximum 3 releases (4 total attempts).
  func(err error) bool {
    // Release on these specific errors.
    return errors.Is(err, ErrServiceUnavailable) || errors.Is(err, ErrRateLimited)
  },
)
queue.Enqueue(job)

// For unlimited releases, use 0 as maxReleases.
jobUnlimited := WithRelease(job, queue, 30*time.Second, 0, shouldRelease)
```

#### Recover

Wrap a job to recover from panics and convert them to errors. This allows panics to be handled by `WithResultHandler` or other wrappers.

Without `WithRecover`:
  * Panics are caught by the queue itself and logged to stderr (or sent to the panic handler, if configured).

With `WithRecover`:
  * Panics become normal errors that can be handled in your job composition.

```go
var failedJobs []error

job := WithResultHandler(
  WithRecover(func(ctx context.Context) error {
    panic("Oh no!") // This panic will be converted to an error.
    return nil
  }),
  func() {
    fmt.Println("Job completed")
  },
  func(err error) {
    fmt.Printf("Job failed: %v\n", err) // Job failed: Oh no!
    failedJobs = append(failedJobs, err)
  },
)

queue.Enqueue(job)
```

#### Tagged

Tag jobs for tracking and cancellation. Useful for cancelling groups of related jobs (example, all jobs for a specific user or resource).

An example use case is cancelling all processing jobs when a user deletes their account.

```go
registry := NewJobRegistry()

// Create tagged jobs.
job1 := WithTagged(func(ctx context.Context) error {.
  time.Sleep(5 * time.Second)
  return nil
}, registry, "user:123", "data-export")

job2 := WithTagged(func(ctx context.Context) error {
  // Another job for same user...
  return nil
}, registry, "user:123", "email-campaign")

queue.Enqueue(job1)
queue.Enqueue(job2)

// Later: Cancel all jobs for user:123.
cancelled := registry.CancelForTag("user:123")
fmt.Printf("Cancelled %d jobs\n", cancelled)

// Check how many jobs are tagged.
count := registry.CountForTag("email-campaign")
fmt.Printf("Active email campaigns: %d\n", count)

// Get all active tags.
tags := registry.Tags()
fmt.Printf("Active tags: %v\n", tags)

// Cancel all jobs.
registry.CancelAll()
```

#### Your own

As long as you match the job signature `func(ctx context.Context) error`, you can create any wrapper you need.

```go
func withSmiles(job Job) Job {
  smile := ":)"
  if err := job(context.Background()); err != nil {
    smile = ":("
  }
  log.Print(smile) // Result of job.
  return nil // No error.
}

// ...

job := withSmiles(func (ctx context.Context) error {
  return doSomeWork()
})
queue.Enqueue(job)

// ...

func withSemaphore(job cq.Job, queueName string, ignoreContextErrors bool) cq.Job {
	sem := semaphoreFor(queueName)
	return func(ctx context.Context) error {
		if err := sem.Aquire(ctx); err != nil {
			// Check if we should ignore context errors.
			if ignoreContextErrors && errors.Is(err, context.Canceled) {
				return nil // Context was cancelled, but we ignore it and continue.
			} else if ignoreContextErrors && errors.Is(err, context.DeadlineExceeded) {
				return nil // Context deadline exceeded, but we ignore it and continue.
			} else {
				return err // Return the error (either not a context error, or we don't ignore context errors).
			}
		}
		defer sem.Release()
		return job(ctx)
	}
}

job := withRetry(
  withSemaphore(rateLimitedJob, "product", false),
  2,
)
queue.Enqueue(job)
```

### Priority Queue

`PriorityQueue` wraps a regular `Queue` to add priority-based processing. Higher priority jobs are processed first, using weighted dispatch to prevent starvation.

#### Basic Usage

Create a priority queue by wrapping an existing queue:

```go
// Create base queue.
queue := NewQueue(2, 10, 100)
queue.Start()

// Wrap with priority queue (50 capacity per priority level).
priorityQueue := NewPriorityQueue(queue, 50)
defer priorityQueue.Stop(true) // true = also stop underlying queue.

// Enqueue jobs with different priorities.
priorityQueue.Enqueue(criticalJob, PriorityHighest)
priorityQueue.Enqueue(normalJob, PriorityMedium)
priorityQueue.Enqueue(cleanupJob, PriorityLowest)

// Non-blocking enqueue... returns false if priority channel is full.
if !priorityQueue.TryEnqueue(job, PriorityHigh) {
  log.Warn("High priority queue is full")
}

// Delay enqueue with priority.
priorityQueue.DelayEnqueue(scheduledJob, PriorityMedium, 30*time.Second)

// Monitor pending jobs by priority.
highPending := priorityQueue.CountByPriority(PriorityHigh)
fmt.Printf("High priority jobs pending: %d\n", highPending)

// Get all pending counts.
allPending := priorityQueue.PendingByPriority()
fmt.Printf("Pending: %+v\n", allPending)
```

#### Priority Levels

Five priority levels are available:

* `PriorityHighest` - Critical system operations.
* `PriorityHigh` - Important user-facing requests.
* `PriorityMedium` - Standard background processing.
* `PriorityLow` - Non-urgent tasks.
* `PriorityLowest` - Cleanup and maintenance.

**Default weights (attempts per tick):** 5:3:2:1:1

This means highest priority jobs get 5 attempts per tick, high gets 3, and so on. This keeps high-priority work moving while still giving lower priorities processing time.

#### Weighting

Customize priority weights using `WithWeighting` with either `NumberWeight` (raw counts) or `PercentWeight` (percentages):

```go
// Using NumberWeight.
priorityQueue := NewPriorityQueue(queue, 50,
  WithWeighting(
    NumberWeight(10),  // Highest: 10 attempts per tick.
    NumberWeight(5),   // High: 5 attempts per tick.
    NumberWeight(3),   // Medium: 3 attempts per tick.
    NumberWeight(2),   // Low: 2 attempts per tick.
    NumberWeight(1),   // Lowest: 1 attempt per tick.
  ),
)

// Using PercentWeight (percentages of 12).
priorityQueue := NewPriorityQueue(queue, 50,
  WithWeighting(
    PercentWeight(60),  // Highest: ~7 attempts (60% of 12).
    PercentWeight(25),  // High: 3 attempts (25% of 12).
    PercentWeight(10),  // Medium: 1 attempt (10% of 12).
    PercentWeight(5),   // Low: 1 attempt (5% of 12, min 1).
    PercentWeight(5),   // Lowest: 1 attempt (5% of 12, min 1).
  ),
)

// Adjust tick duration (default 10ms).
priorityQueue := NewPriorityQueue(queue, 50,
  WithPriorityTick(50*time.Millisecond),
)
```

**Stop behavior:**

* `Stop(true)` - Stops both dispatcher and underlying queue.
* `Stop(false)` - Stops only dispatcher, leaves queue running.

### Scheduler

`Scheduler` provides interval-based scheduling for recurring and one-time jobs. Jobs are enqueued into the provided `Queue` when their schedule triggers, allowing you to schedule jobs to run at specific intervals or at specific times.

#### Basic Usage

Create a scheduler and schedule jobs:

```go
// Create queue and scheduler.
queue := NewQueue(2, 10, 100)
queue.Start()

scheduler := NewScheduler(context.Background(), queue)
defer scheduler.Stop()

// Schedule a recurring job every 10 minutes.
err := scheduler.Every("cleanup-task", 10*time.Minute, func(ctx context.Context) error {
    fmt.Println("Running cleanup...")
    return performCleanup()
})
if err != nil {
    log.Fatal(err)
}

// Schedule a one-time job to run at a specific time.
reminderTime := time.Now().Add(1 * time.Hour)
err = scheduler.At("reminder", reminderTime, func(ctx context.Context) error {
    fmt.Println("Time for your meeting!")
    return sendNotification()
})
if err != nil {
    log.Fatal(err)
}

// Check if a job exists.
if scheduler.Has("cleanup-task") {
    fmt.Println("Cleanup task is scheduled")
}

// List all scheduled jobs.
jobs := scheduler.List()
fmt.Printf("Scheduled jobs: %v\n", jobs)

// Remove a scheduled job.
if scheduler.Remove("cleanup-task") {
    fmt.Println("Cleanup task removed")
}
```

#### Recurring Jobs

Use `Every()` to schedule jobs that run at regular intervals:

```go
// Run every 5 minutes.
scheduler.Every("status-check", 5*time.Minute, statusCheckJob)

// Run every 30 seconds.
scheduler.Every("heartbeat", 30*time.Second, heartbeatJob)

// Run every hour.
scheduler.Every("hourly-report", 1*time.Hour, reportJob)
```

Notes:
- Jobs are enqueued at each interval regardless of execution time
- Multiple instances can queue/run if interval < execution time... use `WithoutOverlap` wrapper to prevent concurrent executions

```go
// Prevent overlapping executions.
noOverlapJob := WithoutOverlap(longRunningJob, "long-task", locker)
scheduler.Every("long-task", 1*time.Minute, noOverlapJob)
```

#### One-Time Jobs

Use `At()` to schedule jobs that run once at a specific time:

```go
// Run at a specific time.
runTime := time.Date(2025, 12, 25, 9, 0, 0, 0, time.UTC)
scheduler.At("christmas-greeting", runTime, greetingJob)

// Run after a delay.
futureTime := time.Now().Add(2 * time.Hour)
scheduler.At("delayed-task", futureTime, delayedJob)
```

Notes:
- Jobs are automatically removed after execution
- Returns error if scheduled time is in the past
- Job is enqueued at the specified time

#### Managing Jobs

Scheduler provides methods for managing scheduled jobs:

```go
// Check if a job exists.
exists := scheduler.Has("my-job")

// Get count of scheduled jobs.
count := scheduler.Count()

// List all job IDs.
jobIDs := scheduler.List()

// Remove a specific job.
removed := scheduler.Remove("my-job")

// Stop all scheduled jobs.
scheduler.Stop()
```

#### Job IDs

Job IDs are user-provided strings that identify scheduled jobs:

```go
// Use descriptive IDs.
scheduler.Every("database-backup", 24*time.Hour, backupJob)
scheduler.Every("cache-cleanup", 1*time.Hour, cleanupJob)
scheduler.At("midnight-report", midnight, reportJob)

// IDs must be unique.
err := scheduler.Every("backup", 1*time.Hour, job1)  // OK
err = scheduler.Every("backup", 2*time.Hour, job2)   // Error: duplicate ID
```

#### Lifecycle

Scheduler lifecycle is controlled by context and independent of the Queue:

```go
queue := NewQueue(2, 10, 100)
queue.Start()

// Simple usage with context.Background().
scheduler := NewScheduler(context.Background(), queue)
// Note: No explicit Start() needed, jobs start when added.

// Later, stop scheduler (does not stop queue).
scheduler.Stop()

// Stop queue separately.
queue.Stop(true)
```

You can pass in a context to handle stopping as needed, such as one handling a signal interrupt.

#### Cron-like Scheduling (Optional)

If you want cron expression support (`"*/5 * * * *"`, `"0 2 * * *"`, `@daily`, etc.),
you can layer an external parser on top of `Scheduler.At()`.

Example using [`github.com/adhocore/gronx`](https://github.com/adhocore/gronx):

```go
func ScheduleCron(scheduler *Scheduler, key string, cronExpr string, job Job) error {
	gron := gronx.New()
	if !gron.IsValid(cronExpr) {
		return fmt.Errorf("ScheduleCron: invalid cron expression: %s", cronExpr)
	}

	nextRun, err := gronx.NextTick(cronExpr, true)
	if err != nil {
		return fmt.Errorf("ScheduleCron: failed to calculate next run: %w", err)
	}

	var scheduledJob Job
	scheduledJob = func(ctx context.Context) error {
		err := job(ctx)

		nextRun, scheduleErr := gronx.NextTick(cronExpr, false)
		if scheduleErr != nil {
			log.Printf("[scheduler:%s] failed to calculate next run: %v", key, scheduleErr)
			return err
		}
		if scheduleErr := scheduler.At(key, nextRun, scheduledJob); scheduleErr != nil {
			log.Printf("[scheduler:%s] failed to reschedule: %v", key, scheduleErr)
		}

		return err
	}

	if err := scheduler.At(key, nextRun, scheduledJob); err != nil {
		return fmt.Errorf("ScheduleCron: failed to schedule first run: %w", err)
	}
	return nil
}
```

Usage:

```go
queue := NewQueue(2, 10, 100)
queue.Start()
defer queue.Stop(true)

scheduler := NewScheduler(context.Background(), queue)
defer scheduler.Stop()

err := ScheduleCron(
	scheduler,
	"daily-report",
	"0 2 * * *", // Daily at 2 AM.
	func(ctx context.Context) error {
		return generateDailyReport(ctx)
	},
)
if err != nil {
	log.Fatal(err)
}
```

### Demo

Example of running an HTTP server and queue together. This setup does not rely on an external source (such as Redis); jobs are accepted and processed directly.

The demo simply runs a basic job function which sometimes may have a simulated workload (time delay). It will create a queue with 1 always-running worker, to a maximum of 100 workers, and a capacity of 1000.

`go run example/web_direct.go`

You can then start spamming jobs with cURL or another tool, example:

`for i in {1..500}; do curl --silent -X POST localhost:8080/order --data '{"demo":"yes"}' -H "Content-Type: application/json"; done`

Sample output on metrics and log:

**Metrics**

![](example/example.gif)

**Log**

    INFO: Send JSON data to http://localhost:8080/order
    INFO: View live metrics at http://localhost:8080/metrics
    
    2023/04/28 12:19:10 Queue running
    2023/04/28 12:19:10 Server running
    2023/04/28 12:19:27 [#2079214216039068897] Completed unmarshal
    2023/04/28 12:19:27 [#5730969642294182386] Completed unmarshal
    2023/04/28 12:19:27 [#3927359866642898391] Completed unmarshal
    2023/04/28 12:19:27 [#1454722463295566100] Failed unmarshal: job: decided to fail, just because
    2023/04/28 12:19:27 [#6148396525160814543] Completed unmarshal
    2023/04/28 12:19:27 [#1001127988095436507] Completed unmarshal
    2023/04/28 12:19:27 [#3742680126965064000] Failed unmarshal: job: decided to fail, just because
    2023/04/28 12:19:28 [#8976594173206639746] Completed unmarshal
    2023/04/28 12:19:28 [#8486067339601011429] Completed unmarshal
    2023/04/28 12:19:28 [#3005863832584645464] Completed unmarshal
    # ...
    ^C
    2023/04/28 12:20:33 Stopping queue...
    2023/04/28 12:20:33 Queue stopped
    2023/04/28 12:20:33 Stopping server...
    2023/04/28 12:20:33 Server stopped
