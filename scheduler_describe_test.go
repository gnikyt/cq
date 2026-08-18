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
	waitFor(t, 500*time.Millisecond, func() bool {
		d := s.Describe()
		return len(d) == 1 && !d[0].NextFireAt.IsZero()
	})
	info := s.Describe()[0]
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

	recvOrFail(t, fired, 2*time.Second, "schedule never fired")

	waitFor(t, 500*time.Millisecond, func() bool {
		d := s.Describe()
		return len(d) == 1 && d[0].Submissions > 0
	})
	info := s.Describe()[0]
	if info.LastErr != nil {
		t.Errorf("Describe().LastErr: got %v, want nil while the queue accepts", info.LastErr)
	}

	// A stopped queue rejects, and the rejection must surface.
	q.Stop(false)
	waitFor(t, 2*time.Second, func() bool {
		d := s.Describe()
		return len(d) == 1 && d[0].LastErr != nil
	})
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
