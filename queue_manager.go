package cq

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// Queue manager errors.
var (
	ErrQueueManagerInvalidName = errors.New("cq: queue manager invalid queue name")
	ErrQueueManagerNilQueue    = errors.New("cq: queue manager nil queue")
	ErrQueueManagerExists      = errors.New("cq: queue manager queue already exists")
	ErrQueueManagerNotFound    = errors.New("cq: queue manager queue not found")
)

// QueueManager provides named queue registration and routing helpers.
type QueueManager struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

// NewQueueManager creates an empty queue manager.
func NewQueueManager() *QueueManager {
	return &QueueManager{
		queues: make(map[string]*Queue),
	}
}

// Register adds a named queue to the manager.
// Names must be non-empty and unique.
func (m *QueueManager) Register(name string, q *Queue) error {
	if name == "" {
		return ErrQueueManagerInvalidName
	}
	if q == nil {
		return ErrQueueManagerNilQueue
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[name]; exists {
		return ErrQueueManagerExists
	}
	m.queues[name] = q
	return nil
}

// ByName returns a queue by name and whether it exists.
func (m *QueueManager) ByName(name string) (*Queue, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[name]
	return q, ok
}

// Names returns registered queue names, sorted alphabetically.
func (m *QueueManager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.queues))
	for name := range m.queues {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Start starts one named queue.
func (m *QueueManager) Start(name string) error {
	q, ok := m.ByName(name)
	if !ok {
		return ErrQueueManagerNotFound
	}
	q.Start()
	return nil
}

// StartAll starts all registered queues.
func (m *QueueManager) StartAll() {
	m.mu.RLock()
	qs := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	m.mu.RUnlock()

	for _, q := range qs {
		q.Start()
	}
}

// Stop stops one named queue.
func (m *QueueManager) Stop(name string, wait bool) error {
	q, ok := m.ByName(name)
	if !ok {
		return ErrQueueManagerNotFound
	}
	q.Stop(wait)
	return nil
}

// StopAll stops all registered queues.
func (m *QueueManager) StopAll(wait bool) {
	m.mu.RLock()
	qs := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	m.mu.RUnlock()

	for _, q := range qs {
		q.Stop(wait)
	}
}

// Enqueue routes a job to a named queue.
func (m *QueueManager) Enqueue(name string, job Job) error {
	q, ok := m.ByName(name)
	if !ok {
		return ErrQueueManagerNotFound
	}
	return q.EnqueueOrError(job)
}

// TryEnqueue routes a job to a named queue without blocking.
func (m *QueueManager) TryEnqueue(name string, job Job) (bool, error) {
	q, ok := m.ByName(name)
	if !ok {
		return false, ErrQueueManagerNotFound
	}
	return q.TryEnqueueOrError(job)
}

// DelayEnqueue routes a delayed job to a named queue.
func (m *QueueManager) DelayEnqueue(name string, job Job, delay time.Duration) error {
	q, ok := m.ByName(name)
	if !ok {
		return ErrQueueManagerNotFound
	}
	q.DelayEnqueue(job, delay)
	return nil
}
