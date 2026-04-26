package cq

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

func TestQueueMiddleware_Order(t *testing.T) {
	var (
		mu  sync.Mutex
		seq []string
	)
	push := func(v string) {
		mu.Lock()
		seq = append(seq, v)
		mu.Unlock()
	}

	m1 := func(next Job) Job {
		return func(ctx context.Context) error {
			push("m1-pre")
			err := next(ctx)
			push("m1-post")
			return err
		}
	}
	m2 := func(next Job) Job {
		return func(ctx context.Context) error {
			push("m2-pre")
			err := next(ctx)
			push("m2-post")
			return err
		}
	}

	q := NewQueue(1, 1, 10, WithMiddleware(m1, m2))
	q.Start()
	q.Enqueue(func(ctx context.Context) error {
		push("job")
		return nil
	})
	q.Stop(true)

	got := seq
	want := []string{"m1-pre", "m2-pre", "job", "m2-post", "m1-post"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("middleware order mismatch: got %v, want %v", got, want)
	}
}

func TestQueueMiddleware_AppendsAcrossOptions(t *testing.T) {
	var (
		mu  sync.Mutex
		seq []string
	)
	push := func(v string) {
		mu.Lock()
		seq = append(seq, v)
		mu.Unlock()
	}

	m1 := func(next Job) Job {
		return func(ctx context.Context) error {
			push("m1")
			return next(ctx)
		}
	}
	m2 := func(next Job) Job {
		return func(ctx context.Context) error {
			push("m2")
			return next(ctx)
		}
	}

	q := NewQueue(1, 1, 10, WithMiddleware(m1), WithMiddleware(nil, m2))
	q.Start()
	q.Enqueue(func(ctx context.Context) error {
		push("job")
		return nil
	})
	q.Stop(true)

	got := seq
	want := []string{"m1", "m2", "job"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("middleware append mismatch: got %v, want %v", got, want)
	}
}
