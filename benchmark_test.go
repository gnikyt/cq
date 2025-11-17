package cq

import (
	"context"
	"math/rand"
	"sync"
	"testing"
)

func factory() (func(job Job), func(jobWait bool)) {
	q := NewQueue(
		1,
		200_000,
		1_000_000,
	)
	q.Start()

	return q.Enqueue, q.Stop
}
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
	//{
	// 	name: "1mReq--10Jobs",
	// 	reqs: 1_000_000,
	// 	jobs: 10,
	//}
	}

	for _, load := range loads {
		b.Run(load.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(load.reqs * load.jobs)

				enqueue, stop := factory()
				tf := Job(func(ctx context.Context) error {
					rand.Float64()
					wg.Done()
					return nil
				})
				for i := 0; i < load.reqs; i++ {
					go func() {
						for i := 0; i < load.jobs; i++ {
							enqueue(tf)
						}
					}()
				}
				wg.Wait()
				stop(true)
			}
		})
	}
}

func BenchmarkSingle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(1 * 1)

		enqueue, stop := factory()
		tf := Job(func(ctx context.Context) error {
			rand.Float64()
			wg.Done()
			return nil
		})
		enqueue(tf)
		wg.Wait()
		stop(true)
	}
}
