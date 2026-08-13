package cq

import (
	"context"
	"testing"
	"time"
)

func TestDescribeReportsEveryKind(t *testing.T) {
	q := NewQueue(1, 1, 8)
	q.Start()
	defer q.Stop(false)

	s := NewScheduler(context.Background(), q)
	defer s.Stop()

	job := func(ctx context.Context) error { return nil }
	if _, err := s.Every("recurring", time.Hour, job); err != nil {
		t.Fatalf("Every(): %v", err)
	}
	if _, err := s.At("once", time.Now().Add(time.Hour), job); err != nil {
		t.Fatalf("At(): %v", err)
	}
	schedule, err := ParseCron("0 0 * * *")
	if err != nil {
		t.Fatalf("ParseCron(): %v", err)
	}
	if _, err := s.On("cron", schedule, job); err != nil {
		t.Fatalf("On(): %v", err)
	}

	infos := s.Describe()
	if len(infos) != 3 {
		t.Fatalf("Describe(): got %d schedules, want 3", len(infos))
	}

	// Sorted by ID: cron, once, recurring.
	for i, want := range []string{"cron", "once", "recurring"} {
		if infos[i].ID != want {
			t.Errorf("Describe()[%d].ID: got %q, want %q", i, infos[i].ID, want)
		}
	}

	byID := map[string]ScheduleInfo{}
	for _, info := range infos {
		byID[info.ID] = info
	}

	if got := byID["recurring"].Kind; got != ScheduleKindInterval {
		t.Errorf("Describe() recurring kind: got %q, want %q", got, ScheduleKindInterval)
	}
	if got := byID["recurring"].Interval; got != time.Hour {
		t.Errorf("Describe() recurring interval: got %v, want 1h", got)
	}
	if got := byID["once"].Kind; got != ScheduleKindOnce {
		t.Errorf("Describe() once kind: got %q, want %q", got, ScheduleKindOnce)
	}
	if byID["once"].RunAt.IsZero() {
		t.Error("Describe() once RunAt: got zero, want the scheduled time")
	}
	if got := byID["cron"].Kind; got != ScheduleKindSchedule {
		t.Errorf("Describe() cron kind: got %q, want %q", got, ScheduleKindSchedule)
	}
}

func TestDescribeReportsNextFireTime(t *testing.T) {
	q := NewQueue(1, 1, 8)
	q.Start()
	defer q.Stop(false)

	s := NewScheduler(context.Background(), q)
	defer s.Stop()

	before := time.Now()
	if _, err := s.Every("recurring", time.Hour, func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("Every(): %v", err)
	}

	// The goroutine records its next fire as soon as it starts waiting.
	var info ScheduleInfo
	for range 100 {
		described := s.Describe()
		if len(described) == 1 && !described[0].NextFireAt.IsZero() {
			info = described[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if info.NextFireAt.IsZero() {
		t.Fatal("Describe().NextFireAt: got zero, want the next fire time")
	}
	if !info.NextFireAt.After(before) {
		t.Errorf("Describe().NextFireAt: got %v, want a time after %v", info.NextFireAt, before)
	}
	if delta := info.NextFireAt.Sub(before); delta > 2*time.Hour {
		t.Errorf("Describe().NextFireAt: got %v away, want roughly 1h", delta)
	}
}

func TestDescribeReportsSubmissionsAndError(t *testing.T) {
	q := NewQueue(1, 1, 8)
	q.Start()

	s := NewScheduler(context.Background(), q)
	defer s.Stop()

	fired := make(chan struct{}, 4)
	if _, err := s.Every("ticker", 10*time.Millisecond, func(ctx context.Context) error {
		select {
		case fired <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("Every(): %v", err)
	}

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("schedule never fired")
	}

	var info ScheduleInfo
	for range 100 {
		described := s.Describe()
		if len(described) == 1 && described[0].Submissions > 0 {
			info = described[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if info.Submissions == 0 {
		t.Fatal("Describe().Submissions: got 0, want at least 1")
	}
	if info.LastErr != nil {
		t.Errorf("Describe().LastErr: got %v, want nil while the queue accepts", info.LastErr)
	}

	// A stopped queue rejects, and the rejection must surface.
	q.Stop(false)
	deadline := time.After(2 * time.Second)
	for {
		described := s.Describe()
		if len(described) == 1 && described[0].LastErr != nil {
			return
		}
		select {
		case <-deadline:
			t.Fatal("Describe().LastErr: got nil, want the rejection from a stopped queue")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestDescribeEmptyScheduler(t *testing.T) {
	q := NewQueue(1, 1, 8)
	q.Start()
	defer q.Stop(false)

	s := NewScheduler(context.Background(), q)
	defer s.Stop()

	if infos := s.Describe(); len(infos) != 0 {
		t.Errorf("Describe(): got %d, want 0", len(infos))
	}
}
