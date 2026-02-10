package cq

import (
	"context"
	"math/rand"
	"sync"
	"testing"
)

const (
	benchmarkWorkersMin = 1
	benchmarkWorkersMax = 200_000
	benchmarkQueueCap   = 10_000
)

func benchmarkQueue(capacity int) *Queue {
	q := NewQueue(benchmarkWorkersMin, benchmarkWorkersMax, capacity)
	q.Start()
	return q
}

func runScenarioOnce(reqs, jobs int, enqueue func(Job), stop func(bool)) {
	var wg sync.WaitGroup
	wg.Add(reqs * jobs)

	tf := Job(func(ctx context.Context) error {
		rand.Float64()
		wg.Done()
		return nil
	})

	for range reqs {
		go func() {
			for range jobs {
				enqueue(tf)
			}
		}()
	}

	wg.Wait()
	stop(true)
}

// BenchmarkScenarios measures end-to-end performance including queue setup/teardown.
func BenchmarkScenarios(b *testing.B) {
	loads := []struct {
		name string
		reqs int
		jobs int
	}{{
		name: "100Req--10kJobs",
		reqs: 100,
		jobs: 10_000,
	}, {
		name: "1kReq--1kJobs",
		reqs: 1000,
		jobs: 1000,
	}, {
		name: "10kReq--100Jobs",
		reqs: 10_000,
		jobs: 100,
	},
	}

	for _, load := range loads {
		b.Run(load.name, func(b *testing.B) {
			for range b.N {
				q := benchmarkQueue(benchmarkQueueCap)
				runScenarioOnce(load.reqs, load.jobs, q.Enqueue, q.Stop)
			}
		})
	}
}

// BenchmarkScenariosSteadyState measures enqueue/processing performance on a
// long-lived queue to reduce setup-allocation noise.
func BenchmarkScenariosSteadyState(b *testing.B) {
	loads := []struct {
		name string
		reqs int
		jobs int
	}{{
		name: "100Req--10kJobs",
		reqs: 100,
		jobs: 10_000,
	}, {
		name: "1kReq--1kJobs",
		reqs: 1000,
		jobs: 1000,
	}, {
		name: "10kReq--100Jobs",
		reqs: 10_000,
		jobs: 100,
	}}

	for _, load := range loads {
		b.Run(load.name, func(b *testing.B) {
			q := benchmarkQueue(benchmarkQueueCap)
			b.Cleanup(func() {
				q.Stop(true)
			})

			for range b.N {
				var wg sync.WaitGroup
				wg.Add(load.reqs * load.jobs)

				tf := Job(func(ctx context.Context) error {
					rand.Float64()
					wg.Done()
					return nil
				})

				for range load.reqs {
					go func() {
						for range load.jobs {
							q.Enqueue(tf)
						}
					}()
				}

				wg.Wait()
			}
		})
	}
}

// BenchmarkSingle measures end-to-end single-job performance including setup/teardown.
func BenchmarkSingle(b *testing.B) {
	for b.Loop() {
		q := benchmarkQueue(benchmarkQueueCap)
		runScenarioOnce(1, 1, q.Enqueue, q.Stop)
	}
}

// BenchmarkSingleSteadyState measures single-job performance on a long-lived queue.
func BenchmarkSingleSteadyState(b *testing.B) {
	q := benchmarkQueue(benchmarkQueueCap)
	b.Cleanup(func() {
		q.Stop(true)
	})

	for b.Loop() {
		var wg sync.WaitGroup
		wg.Add(1)

		tf := Job(func(ctx context.Context) error {
			rand.Float64()
			wg.Done()
			return nil
		})

		q.Enqueue(tf)
		wg.Wait()
	}
}
