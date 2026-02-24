package cq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// releaseRequest is a request to release a job after a delay.
type releaseRequest struct {
	token uint64        // The token for the current run.
	delay time.Duration // The delay to re-enqueue the job.
}

// releaseRequestStore is a store for release requests.
type releaseRequestStore struct {
	mut      sync.Mutex
	nextTkn  uint64                    // The next token to use for a new run.
	active   map[string]uint64         // The active tokens for the current runs.
	requests map[string]releaseRequest // The requests for the current runs.
}

// newReleaseRequestStore creates a new release request store.
func newReleaseRequestStore() *releaseRequestStore {
	return &releaseRequestStore{
		active:   make(map[string]uint64),
		requests: make(map[string]releaseRequest),
	}
}

// beginRun begins a new run for the given job ID.
// It returns the token for the current run.
// It also deletes any prior requests for the job ID.
func (s *releaseRequestStore) begin(id string) uint64 {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.nextTkn++
	tok := s.nextTkn
	s.active[id] = tok
	delete(s.requests, id) // Defensive cleanup from prior runs.
	return tok
}

// endRun ends the current run for the given job ID and token.
// It deletes the active token and any requests for the job ID.
func (s *releaseRequestStore) end(id string, token uint64) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if activeTkn, ok := s.active[id]; ok && activeTkn == token {
		delete(s.active, id)
	}
	if req, ok := s.requests[id]; ok && req.token == token {
		delete(s.requests, id)
	}
}

// set sets a release request for the given job ID and token.
// It returns true if the request was set.
// It returns false if the request was not set.
func (s *releaseRequestStore) set(id string, token uint64, delay time.Duration) bool {
	if delay < 0 {
		delay = 0
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	activeTkn, ok := s.active[id]
	if !ok || activeTkn != token {
		return false // The token is not active for this job ID.
	}
	s.requests[id] = releaseRequest{
		token: token,
		delay: delay, // Last write wins for this run token.
	}
	return true // The request was set.
}

// pop pops a release request for the given job ID and token.
// It returns the delay and true if a request was found.
// It returns 0 and false if no request was found.
func (s *releaseRequestStore) pop(id string, token uint64) (time.Duration, bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	req, ok := s.requests[id]
	if !ok || req.token != token {
		return 0, false // The request was not found.
	}
	delete(s.requests, id)
	return req.delay, true // The request was found.
}

// WithReleaseSelf allows a running job to request delayed re-enqueue of itself via
// RequestRelease(ctx, delay). maxReleases == 0 means unlimited releases.
//
// Behavior:
//   - If a release is requested during execution and release budget allows, the job
//     is re-enqueued after the requested delay and this run returns nil.
//   - If release budget is exceeded, the wrapper returns the job error (or nil).
//   - If both error and release request occur, release wins while budget allows.
func WithReleaseSelf(job Job, queue *Queue, maxReleases int) Job {
	if maxReleases < 0 {
		maxReleases = 0
	}

	var releases atomic.Int32
	var wrappedJob Job
	store := newReleaseRequestStore()

	// wrappedJob is the actual job that is wrapped with the release self logic.
	// It creates a context with a release requester function that will be called when RequestRelease is called.
	wrappedJob = func(ctx context.Context) error {
		meta := MetaFromContext(ctx)
		if meta.ID == "" {
			return job(ctx) // No release request possible.
		}
		runToken := store.begin(meta.ID)
		defer store.end(meta.ID, runToken)

		// Set a release requester function in the context.
		// This function will be called when RequestRelease is called.
		// It has the current run token to reject stale async requests.
		releaseCtx := contextWithReleaseRequester(ctx, func(delay time.Duration) bool {
			return store.set(meta.ID, runToken, delay)
		})

		err := job(releaseCtx)
		delay, requested := store.pop(meta.ID, runToken)
		if !requested {
			return err // No release request, return the error.
		}

		if maxReleases == 0 {
			// Unlimited releases, just delay and re-enqueue.
			queue.dispatchReschedule(meta, delay, EnvelopeRescheduleReasonReleaseSelf)
			queue.DelayEnqueue(wrappedJob, delay)
			return nil
		}

		for {
			current := releases.Load()
			if int(current) >= maxReleases {
				return err // Release budget exceeded, return the error.
			}

			if releases.CompareAndSwap(current, current+1) {
				queue.dispatchReschedule(meta, delay, EnvelopeRescheduleReasonReleaseSelf)
				queue.DelayEnqueue(wrappedJob, delay)
				return nil // Release budget allows, delay and re-enqueue.
			}
		}
	}

	return wrappedJob
}

// WithRelease re-enqueues a job after a delay when shouldRelease(err) is true.
// shouldRelease should match transient errors such as timeouts or rate limits.
// maxReleases == 0 means unlimited releases.
// Once maxReleases is exceeded, or shouldRelease returns false, the error is returned.
func WithRelease(job Job, queue *Queue, delay time.Duration, maxReleases int, shouldRelease func(error) bool) Job {
	var releases atomic.Int32
	var wrappedJob Job

	wrappedJob = func(ctx context.Context) error {
		meta := MetaFromContext(ctx)
		err := job(ctx)
		if err != nil && shouldRelease(err) {
			if maxReleases == 0 {
				queue.dispatchReschedule(meta, delay, EnvelopeRescheduleReasonRelease)
				queue.DelayEnqueue(wrappedJob, delay)
				return nil
			}

			for {
				current := releases.Load()
				if int(current) >= maxReleases {
					break
				}
				if releases.CompareAndSwap(current, current+1) {
					queue.dispatchReschedule(meta, delay, EnvelopeRescheduleReasonRelease)
					queue.DelayEnqueue(wrappedJob, delay)
					return nil
				}
			}
		}
		return err
	}
	return wrappedJob
}
