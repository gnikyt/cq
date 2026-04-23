# Queue Routing

Use `QueueManager` to register multiple named queues and route jobs to them explicitly.
This provides named-queue routing on top of the `Queue` implementation.

```go
mgr := cq.NewQueueManager()

highQ := cq.NewQueue(5, 50, 1000) // High-priority lane.
lowQ := cq.NewQueue(1, 5, 5000)   // Bulk/background lane.

_ = mgr.Register("high", highQ)
_ = mgr.Register("low", lowQ)

mgr.StartAll()
defer mgr.StopAll(true)

_ = mgr.Enqueue("high", criticalJob)
_ = mgr.Enqueue("low", bulkJob)
_ = mgr.DelayEnqueue("low", bulkJob, 30*time.Second)
```

## Why use this?

- Register and access queues by name from one place.
- Route jobs explicitly and testably by queue name.
- Delay jobs through the manager without looking up the queue first.
- Configure separate worker pools per queue (for example, high-priority vs bulk work).
- Manage queue lifecycle from one place (`StartAll`, `StopAll`, `Names`).

## Notes

- `QueueManager` is orchestration-only... each queue still keeps its own options and hooks.
- Unknown queue names return `cq.ErrQueueManagerNotFound`.
- `DelayEnqueue` is a convenience wrapper over the named queue's `DelayEnqueue`.
