# Priority Queue

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
