# Composable Queue

[![Testing](https://github.com/gnikyt/cq/actions/workflows/cq.yml/badge.svg)](https://github.com/gnikyt/cq/actions/workflows/cq.yml)

An auto-scalling queue which processes functions as jobs. The jobs can be simple functions or composed of the supporting job wrappers.

Wrapper supports for retries, timeouts, deadlines, delays, backoffs, and potential to write your own.

This is inspired from great projects such as Bull, Pond, Ants, and more.

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
    + [Your own](#your-own)
  - [Demo](#demo)

## Testing

`make test`

Example result:

    ok  	github.com/littlerocketinc/go-webhooks	9.002s	coverage: 91.3% of statements
    PASS

\>90% coverage currently, but tests need to be improved as noted in `queue_test.go`'s TODO.

### Benchmarks

`make bench`

Runs the following benchmarks:

1. 100 requests each pushing 10,000 jobs
2. 1,000 requests each pushing 1,000 jobs
3. 10,000 requests each pushing 100 jobs
4. 1,000,000 requests each pushing 10 jobs

Example result:

    BenchmarkScenarios/100Req--10kJobs-8                   3         543227459 ns/op         8251978 B/op       1935 allocs/op
    BenchmarkScenarios/1kReq--1kJobs-8                     3         637900367 ns/op         8304258 B/op       4801 allocs/op
    BenchmarkScenarios/10kReq--100Jobs-8                   3         874768339 ns/op        14218722 B/op      50486 allocs/op
    BenchmarkScenarios/1mReq--10Jobs-8                     3        16676255114 ns/op       65391498 B/op    1129154 allocs/op
    PASS

## Building

`make build`

Binary will be located in `dist/cq`. You will need to grant execution permissions.

## Examples

### Queue

Queue needs a minimum and maximum amount of workers to run. There are other options you can tap into as well.

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
* `IdleWorkers() int` returns the number of idle workers.
* `Capacity() int` returns the configured job capacity.
* `WorkerRange() (int, int)` returns the configured minimum and maximum workers.
* `TallyOf(cq.JobState) int` returns the number of jobs for a given state, example `TallyOf(cq.JobStateCreated)` will give the number of jobs pushed to the queue so far.

#### Enqueue

You can push jobs to the queue in a few ways.

* `Enqueue(Job)` will push the job to the queue, non-blocking.
  - Example: `Enqueue(job)`
* `TryEnqueue(Job) bool` will try and push the job to the queue, and return if successful or not.
  - Example: `ok := TryEnqueue(job)`
* `DelayEnqueue(Job, delay time.Duration)` will push the job to the queue in a seperate goroutine after the delay.
  - Example: `DelayEnqueue(job, time.Duration(2) * time.Minute)`

#### Idle worker tick

You can configure how often to remove idle workers.

```go
// Check every 500ms for idle workers to remove.
queue := NewQueue(
  2,
  5,
  100,
  WithWorkerIdleTick(time.Duration(500) * time.Millisecond),
)
```

### Panic handler

You can configure a handler for panics so the queue does not die if a panic happens. A panic can happen from running a job itself, or if the queue happens to be maxed out and the job can not push to the queue (using `Enqueue`, not `TryEnqueue`).

```go
queue := NewQueue(
  2,
  5,
  100,
  WithPanicHandler(func (err interface{}) {
    // err can be a string, error, etc.. you can use type assert to check and handle as you need..
    log.Errorf("Job or queue failed: %v", %v)
  }),
)
```

### Context

You can pass a custom context to the queue. By default, the queue will configure a cancelable background context. This option may be useful if you want to terminate the queue after running for a certain period of time, for example.

```go
queue := NewQueue(2, 5, 100, WithContext(yourCtx))
// or...
queue := NewQueue(2, 5, 100, WithCancelableContext(yourCtx, yourCtxCancel))
```

### Stopping

You can stop a queue in a few ways.

* `Stop(jobWait bool)` will flag the queue to stop, optionally wait for jobs to complete in the queue if you pass `true`, wait for workers to complete, and runs some cleanup.
* `Terminate()` will flag the queue to stop, hard stop all workers and jobs regardless of what they are doing.

See `example/web_direct.go` for an example on how you can configure sigterm/sigint context to stop the queue.

### Jobs

You can set your jobs in any way you please, so long as it matches the signature of `func() error`. You can use basic functions, composed functions, struct methods, etc.

Each of the built-in methods can be composed ontop of one-another to build your desired requirements. And since any function that matches the signature will work, you can build your own wrapping functions as well.

Examples below should help in understanding what you can do. Ignore the job functions themselves and method names inside; used simply to demonstrate.

#### Function

A basic function.

```go
job := func(ss SomeService) error {
  return func() error {
    ss.doWork()
  }
}
queue.Enqueue(job(ss))

// ...

job2 := func() error {
  log.Info("Basic function")
  return nil
}
queue.Enqueue(job2)
```

#### Retries

Retry, on error.

```go
// Retry twice before giving up.
job := WithRetry(func () error {
  req := fetchSomeEndpoint()
  if err != nil {
    return fmt.Errorf("special job: %w", err)
  }
  return finalize(req)
}, 2)
```

#### Backoff

Retry, on error, with backoff.

```go
// Retry four times before giving up
//  Backoff exponentially.
// defaultBackoffHandler is used which will exponentially delay the retries.
// 1 retry = 1 second delay.
// 2 retries = 2 second delay.
// 3 retries = 4 second delay.
// 4 retries = 8 second delay... etc
job := WithRetry(WithBackoff(func () error {
  req := fetchSomeEndpoint()
  if err != nil {
    return fmt.Errorf("special job: %w", err)
  }
  return finalize(req)
}, nil), 4)
queue.Enqueue(job)
```

#### Result catch

Capture job result, completed or failed.

```go
// We will create a job with an ID.
// We will capture its result of success and failure.
// If failed, we will move the job to a table to process later.
job := func (id int) error {
  return WithResult(
    WithRetry(func () error {
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
    // On failure (optional), use nil to disfregard.
    func (err error) {
      moveToFailureTable(id, err)
    },
  )
}
queue.Enqueue(job(id))
```

#### Timeout

Timeout a job after running it for a duration.

```go
// Job must complete 5 minutes after running.
job := WithTimeout(func () error {
  return someExpensiveLongWork()
}, time.Duration(5) * time.Minute)
queue.Enqueue(job)
```

#### Deadline

Complete a job by a certain datetime.

```go
// Job must complete by X date.
job := WithDeadline(func () error {
  return someExpensiveLongWork()
}, time.Now().Add(time.Duration(1) * time.Minute))
queue.Enqueue(job)
```

#### Your own

As long as you match the job signature of `func() error`, you can create anything to wrap your job.

```go
func withSmiles(job Job) Job {
  smile := ":)"
  if err := job(); err != nil {
    smile = ":("
  }
  log.Print(smile)
}

// ...

job := withSmiles(func () error {
  return doSomeWork()
})
queue.Enqueue(job)
```

### Demo

An example of running a HTTP server and queue at once, without relying on a source to save or pull the jobs (such as Redis). It will take the job and directly process it.

The demo simply runs a basic job function which sometimes may have a simulated workload (time delay). It will create a queue with 5 running workers, to a maximum of 100 workers, and a capacity of 1000 concurrent jobs.

`go run example/web_direct.go`

You can then start spamming jobs with cURL or another tool:

`for i in {1..500}; do curl --silent -X POST localhost:8080/order --data '{"demo":"yes"}' -H "Content-Type: application/json"; done`

Example of output on metrics and log:

![](example/example.gif)

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
