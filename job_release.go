package cq

import (
	"context"
	"sync/atomic"
	"time"
)

// WithRelease re-enqueues a job after a delay when shouldRelease(err) is true.
// shouldRelease should match transient errors such as timeouts or rate limits.
// maxReleases == 0 means unlimited releases.
// Once maxReleases is exceeded, or shouldRelease returns false, the error is returned.
func WithRelease(job Job, queue *Queue, delay time.Duration, maxReleases int, shouldRelease func(error) bool) Job {
	var releases atomic.Int32
	var wrappedJob Job

	wrappedJob = func(ctx context.Context) error {
		err := job(ctx)
		if err != nil && shouldRelease(err) {
			if maxReleases == 0 {
				queue.DelayEnqueue(wrappedJob, delay)
				return nil
			}

			for {
				current := releases.Load()
				if int(current) >= maxReleases {
					break
				}
				if releases.CompareAndSwap(current, current+1) {
					queue.DelayEnqueue(wrappedJob, delay)
					return nil
				}
			}
		}
		return err
	}
	return wrappedJob
}
