package cq

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Priority queue manager errors.
var (
	ErrPriorityQueueManagerInvalidName = errors.New("cq: priority queue manager invalid queue name")
	ErrPriorityQueueManagerNilQueue    = errors.New("cq: priority queue manager nil queue")
	ErrPriorityQueueManagerExists      = errors.New("cq: priority queue manager queue already exists")
	ErrPriorityQueueManagerNotFound    = errors.New("cq: priority queue manager queue not found")
)

// PriorityQueueManager provides named priority queue registration and routing helpers.
type PriorityQueueManager struct {
	mu     sync.RWMutex
	queues map[string]*PriorityQueue
}

// NewPriorityQueueManager creates an empty priority queue manager.
func NewPriorityQueueManager() *PriorityQueueManager {
	return &PriorityQueueManager{
		queues: make(map[string]*PriorityQueue),
	}
}

// Register adds a named priority queue to the manager.
// Names must be non-empty and unique.
func (m *PriorityQueueManager) Register(name string, q *PriorityQueue) error {
	if name == "" {
		return ErrPriorityQueueManagerInvalidName
	}
	if q == nil {
		return ErrPriorityQueueManagerNilQueue
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[name]; exists {
		return ErrPriorityQueueManagerExists
	}
	m.queues[name] = q
	return nil
}

// ByName returns a priority queue by name and whether it exists.
func (m *PriorityQueueManager) ByName(name string) (*PriorityQueue, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[name]
	return q, ok
}

// Names returns registered queue names, sorted alphabetically.
func (m *PriorityQueueManager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.queues))
	for name := range m.queues {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Stop stops one named priority queue.
func (m *PriorityQueueManager) Stop(name string, stopQueue bool) error {
	q, ok := m.ByName(name)
	if !ok {
		return ErrPriorityQueueManagerNotFound
	}
	q.Stop(stopQueue)
	return nil
}

// StopAll stops all registered priority queues.
func (m *PriorityQueueManager) StopAll(stopQueue bool) {
	m.mu.RLock()
	qs := make([]*PriorityQueue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	m.mu.RUnlock()

	for _, q := range qs {
		q.Stop(stopQueue)
	}
}

// Submit routes a job to a named priority queue.
func (m *PriorityQueueManager) Submit(ctx context.Context, name string, job Job, priority Priority, opts ...SubmitOption) (*JobHandle, error) {
	q, ok := m.ByName(name)
	if !ok {
		return nil, ErrPriorityQueueManagerNotFound
	}
	return q.Submit(ctx, job, priority, opts...)
}

// SubmitAfter routes a delayed job to a named priority queue.
func (m *PriorityQueueManager) SubmitAfter(ctx context.Context, name string, job Job, priority Priority, delay time.Duration, opts ...SubmitOption) (*JobHandle, error) {
	q, ok := m.ByName(name)
	if !ok {
		return nil, ErrPriorityQueueManagerNotFound
	}
	return q.SubmitAfter(ctx, job, priority, delay, opts...)
}
