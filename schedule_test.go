package cq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestParseCronErrors(t *testing.T) {
	cases := []string{
		"",               // No fields.
		"* * * *",        // Too few fields.
		"* * * * * *",    // Too many fields.
		"60 * * * *",     // Minute out of range.
		"* 24 * * *",     // Hour out of range.
		"* * 0 * *",      // Day-of-month out of range.
		"* * * 13 *",     // Month out of range.
		"* * * * 8",      // Day-of-week out of range.
		"*/0 * * * *",    // Zero step.
		"*/x * * * *",    // Invalid step.
		"a * * * *",      // Invalid value.
		"10-5 * * * *",   // Inverted range.
		"1,,2 * * * *",   // Empty list term.
		"@fortnightly",   // Unknown descriptor.
		"* * * xyz *",    // Unknown month name.
		"* * * * monday", // Full day names unsupported.
	}
	for _, expr := range cases {
		if _, err := ParseCron(expr); !errors.Is(err, ErrCronExprInvalid) {
			t.Errorf("ParseCron(%q): got %v, want ErrCronExprInvalid", expr, err)
		}
	}
}

func TestCronScheduleNext(t *testing.T) {
	// 2026-07-10 is a Friday.
	base := time.Date(2026, time.July, 10, 10, 7, 30, 0, time.UTC)
	cases := []struct {
		expr string
		want time.Time
	}{
		{"*/15 * * * *", time.Date(2026, time.July, 10, 10, 15, 0, 0, time.UTC)},
		{"* * * * *", time.Date(2026, time.July, 10, 10, 8, 0, 0, time.UTC)},
		{"0 9 * * mon-fri", time.Date(2026, time.July, 13, 9, 0, 0, 0, time.UTC)},
		{"30 4 1 1 *", time.Date(2027, time.January, 1, 4, 30, 0, 0, time.UTC)},
		{"@daily", time.Date(2026, time.July, 11, 0, 0, 0, 0, time.UTC)},
		{"@hourly", time.Date(2026, time.July, 10, 11, 0, 0, 0, time.UTC)},
		{"0 0 29 2 *", time.Date(2028, time.February, 29, 0, 0, 0, 0, time.UTC)},
		{"0 0 * * 7", time.Date(2026, time.July, 12, 0, 0, 0, 0, time.UTC)}, // 7 folds to Sunday.
		{"0 0-23/6 * * *", time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)},
		{"5,35 8 * * *", time.Date(2026, time.July, 11, 8, 5, 0, 0, time.UTC)},
		// Vixie OR semantics: fires on the 13th or on Fridays, whichever is sooner.
		{"0 0 13 * fri", time.Date(2026, time.July, 13, 0, 0, 0, 0, time.UTC)},
		// Restricted day-of-month with unrestricted day-of-week: day-of-month only.
		{"0 0 13 * *", time.Date(2026, time.July, 13, 0, 0, 0, 0, time.UTC)},
		// Stepped star restricts day-of-month: fires on odd days OR Mondays.
		// From Fri the 10th, day 11 (odd) beats Mon the 13th.
		{"0 0 */2 * mon", time.Date(2026, time.July, 11, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		cs, err := ParseCron(tc.expr)
		if err != nil {
			t.Fatalf("ParseCron(%q): unexpected error: %v", tc.expr, err)
		}
		if got := cs.Next(base); !got.Equal(tc.want) {
			t.Errorf("Next(%q) from %v: got %v, want %v", tc.expr, base, got, tc.want)
		}
	}
}

func TestCronScheduleNextAdvancesStrictly(t *testing.T) {
	cs := MustParseCron("*/5 * * * *")
	// Starting exactly on a fire time must return the following fire time.
	at := time.Date(2026, time.July, 10, 10, 5, 0, 0, time.UTC)
	want := time.Date(2026, time.July, 10, 10, 10, 0, 0, time.UTC)
	if got := cs.Next(at); !got.Equal(want) {
		t.Fatalf("Next(): got %v, want %v", got, want)
	}
}

func TestCronScheduleNextImpossible(t *testing.T) {
	cs := MustParseCron("0 0 31 2 *")
	if got := cs.Next(time.Date(2026, time.July, 10, 0, 0, 0, 0, time.UTC)); !got.IsZero() {
		t.Fatalf("Next(): got %v, want zero time for impossible expression", got)
	}
}

// fakeSchedule fires at a fixed list of times.
type fakeSchedule struct {
	mu    sync.Mutex
	times []time.Time
}

func (f *fakeSchedule) Next(after time.Time) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, at := range f.times {
		if at.After(after) {
			return at
		}
	}
	return time.Time{}
}

func TestJitterSchedule(t *testing.T) {
	base := MustParseCron("0 * * * *")
	max := 5 * time.Minute
	js := JitterSchedule(base, max)

	after := time.Date(2026, time.July, 10, 10, 7, 30, 0, time.UTC)
	want := base.Next(after)
	for range 50 {
		got := js.Next(after)
		if got.Before(want) || !got.Before(want.Add(max)) {
			t.Fatalf("Next(): got %v, want within [%v, %v)", got, want, want.Add(max))
		}
	}
}

func TestJitterSchedulePassthrough(t *testing.T) {
	base := MustParseCron("0 0 31 2 *") // Impossible... Next returns zero.
	js := JitterSchedule(base, time.Minute)
	if got := js.Next(time.Date(2026, time.July, 10, 0, 0, 0, 0, time.UTC)); !got.IsZero() {
		t.Fatalf("Next(): got %v, want zero time passthrough", got)
	}

	if got := JitterSchedule(base, 0); got != Schedule(base) {
		t.Fatalf("JitterSchedule(): got %v, want base schedule for non-positive max", got)
	}
	if got := JitterSchedule(nil, time.Minute); got != nil {
		t.Fatalf("JitterSchedule(): got %v, want nil for nil schedule", got)
	}
}

func TestSchedulerOnValidation(t *testing.T) {
	queue := NewQueue(1, 2, 16)
	queue.Start()
	defer queue.Stop(true)
	s := NewScheduler(context.Background(), queue)
	defer s.Stop()

	if _, err := s.On("bad", &fakeSchedule{}, nil); !errors.Is(err, ErrScheduleJobRequired) {
		t.Errorf("On(): got %v, want ErrScheduleJobRequired", err)
	}
	if _, err := s.On("bad", nil, func(ctx context.Context) error { return nil }); !errors.Is(err, ErrScheduleRequired) {
		t.Errorf("On(): got %v, want ErrScheduleRequired", err)
	}
}

func TestSchedulerOnRunsAndCompletes(t *testing.T) {
	queue := NewQueue(1, 2, 16)
	queue.Start()
	defer queue.Stop(true)
	s := NewScheduler(context.Background(), queue)
	defer s.Stop()

	now := time.Now()
	sched := &fakeSchedule{times: []time.Time{
		now.Add(30 * time.Millisecond),
		now.Add(60 * time.Millisecond),
	}}

	var mu sync.Mutex
	runs := 0
	handle, err := s.On("fake", sched, func(ctx context.Context) error {
		mu.Lock()
		runs++
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("On(): %v", err)
	}

	select {
	case <-handle.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("schedule did not complete")
	}

	if got := handle.SubmissionAttempts(); got != 2 {
		t.Errorf("SubmissionAttempts(): got %d, want 2", got)
	}
	if s.Has("fake") {
		t.Error("completed schedule should be removed")
	}

	queue.Stop(true)
	mu.Lock()
	defer mu.Unlock()
	if runs != 2 {
		t.Errorf("got runs=%d, want 2", runs)
	}
}

func TestSchedulerHooks(t *testing.T) {
	queue := NewQueue(1, 2, 16)
	queue.Start()
	defer queue.Stop(true)

	fires := make(chan string, 4)
	completes := make(chan string, 4)
	s := NewScheduler(context.Background(), queue, WithSchedulerHooks(SchedulerHooks{
		OnFire: func(id string, handle *JobHandle, err error) {
			if handle == nil || err != nil {
				t.Errorf("OnFire(%q): got (%v, %v), want accepted handle", id, handle, err)
			}
			fires <- id
		},
		OnComplete: func(id string) {
			completes <- id
		},
	}))
	defer s.Stop()

	sched := &fakeSchedule{times: []time.Time{time.Now().Add(30 * time.Millisecond)}}
	handle, err := s.On("hooked", sched, func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatalf("On(): %v", err)
	}

	select {
	case id := <-fires:
		if id != "hooked" {
			t.Errorf("OnFire id: got %q, want %q", id, "hooked")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnFire hook never ran")
	}

	<-handle.Done()
	select {
	case id := <-completes:
		if id != "hooked" {
			t.Errorf("OnComplete id: got %q, want %q", id, "hooked")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnComplete hook never ran")
	}

	// Completion is once-only... no duplicate notifications.
	select {
	case id := <-completes:
		t.Fatalf("OnComplete ran twice: %q", id)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSchedulerOnCancel(t *testing.T) {
	queue := NewQueue(1, 2, 16)
	queue.Start()
	defer queue.Stop(true)
	s := NewScheduler(context.Background(), queue)
	defer s.Stop()

	sched := &fakeSchedule{times: []time.Time{time.Now().Add(time.Hour)}}
	handle, err := s.On("cancel-me", sched, func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatalf("On(): %v", err)
	}
	if !handle.Cancel() {
		t.Fatal("expected Cancel to succeed")
	}

	select {
	case <-handle.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled schedule did not finish")
	}
	if handle.SubmissionAttempts() != 0 {
		t.Error("cancelled schedule should not have submitted")
	}
}

func TestSchedulerOnNonAdvancingScheduleCompletes(t *testing.T) {
	queue := NewQueue(1, 2, 16)
	queue.Start()
	defer queue.Stop(true)
	s := NewScheduler(context.Background(), queue)
	defer s.Stop()

	// A schedule that never advances must terminate, not spin.
	handle, err := s.On("stuck", scheduleFunc(func(after time.Time) time.Time { return after }), func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatalf("On(): %v", err)
	}
	select {
	case <-handle.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("non-advancing schedule did not complete")
	}
}

// scheduleFunc adapts a function to the Schedule interface.
type scheduleFunc func(time.Time) time.Time

func (f scheduleFunc) Next(after time.Time) time.Time { return f(after) }
