package cq

import (
	"context"
	"errors"
)

// Job dispatcher adapter errors.
var (
	ErrJobDispatcherNilQueue                = errors.New("cq: job dispatcher nil queue")
	ErrJobDispatcherNilQueueManager         = errors.New("cq: job dispatcher nil queue manager")
	ErrJobDispatcherNilPriorityQueue        = errors.New("cq: job dispatcher nil priority queue")
	ErrJobDispatcherNilPriorityQueueManager = errors.New("cq: job dispatcher nil priority queue manager")
	ErrJobDispatcherNilQueueRoute           = errors.New("cq: job dispatcher nil queue route")
	ErrJobDispatcherNilPriorityRoute        = errors.New("cq: job dispatcher nil priority route")
	ErrJobDispatcherEmptyQueueName          = errors.New("cq: job dispatcher empty queue name")
)

// QueueDispatchRouteFunc resolves a queue name for a dispatch request.
type QueueDispatchRouteFunc func(req DispatchRequest) (queueName string, err error)

// PriorityDispatchRouteFunc resolves a priority queue name and priority for a dispatch request.
type PriorityDispatchRouteFunc func(req DispatchRequest) (queueName string, priority Priority, err error)

// NewQueueJobDispatcher creates a dispatcher that enqueues into queue.
func NewQueueJobDispatcher(queue *Queue) JobDispatcher {
	return JobDispatcherFunc(func(ctx context.Context, req DispatchRequest) error {
		if queue == nil {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  ErrJobDispatcherNilQueue,
			}
		}
		if err := queue.EnqueueOrError(req.Job); err != nil {
			return &DispatchError{
				Kind: DispatchErrorKindRejected,
				Err:  err,
			}
		}
		return nil
	})
}

// NewQueueManagerJobDispatcher creates a dispatcher that routes to QueueManager queues.
func NewQueueManagerJobDispatcher(mgr *QueueManager, route QueueDispatchRouteFunc) JobDispatcher {
	return JobDispatcherFunc(func(ctx context.Context, req DispatchRequest) error {
		if mgr == nil {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  ErrJobDispatcherNilQueueManager,
			}
		}
		if route == nil {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  ErrJobDispatcherNilQueueRoute,
			}
		}

		name, err := route(req)
		if err != nil {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  err,
			}
		}
		if name == "" {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  ErrJobDispatcherEmptyQueueName,
			}
		}
		if err := mgr.Enqueue(name, req.Job); err != nil {
			return &DispatchError{
				Kind: DispatchErrorKindRejected,
				Err:  err,
			}
		}

		return nil
	})
}

// NewPriorityQueueJobDispatcher creates a dispatcher that enqueues into queue with priority.
func NewPriorityQueueJobDispatcher(queue *PriorityQueue, priority Priority) JobDispatcher {
	return JobDispatcherFunc(func(ctx context.Context, req DispatchRequest) error {
		if queue == nil {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  ErrJobDispatcherNilPriorityQueue,
			}
		}
		if err := queue.EnqueueOrError(req.Job, priority); err != nil {
			return &DispatchError{
				Kind: DispatchErrorKindRejected,
				Err:  err,
			}
		}
		return nil
	})
}

// NewPriorityQueueManagerJobDispatcher creates a dispatcher that routes to PriorityQueueManager queues.
func NewPriorityQueueManagerJobDispatcher(mgr *PriorityQueueManager, route PriorityDispatchRouteFunc) JobDispatcher {
	return JobDispatcherFunc(func(ctx context.Context, req DispatchRequest) error {
		if mgr == nil {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  ErrJobDispatcherNilPriorityQueueManager,
			}
		}
		if route == nil {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  ErrJobDispatcherNilPriorityRoute,
			}
		}

		name, priority, err := route(req)
		if err != nil {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  err,
			}
		}
		if name == "" {
			return &DispatchError{
				Kind: DispatchErrorKindUnavailable,
				Err:  ErrJobDispatcherEmptyQueueName,
			}
		}
		if err := mgr.Enqueue(name, req.Job, priority); err != nil {
			return &DispatchError{
				Kind: DispatchErrorKindRejected,
				Err:  err,
			}
		}

		return nil
	})
}
