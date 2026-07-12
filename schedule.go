package cq

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule errors.
var (
	// ErrCronExprInvalid is returned (or wrapped) when a cron expression cannot be parsed.
	ErrCronExprInvalid = errors.New("cq: cron expression invalid")
)

// cronMaxLookahead bounds Next's search so impossible expressions
// (example: "0 0 31 2 *") terminate instead of scanning forever.
const cronMaxLookahead = 5 * 366 * 24 * time.Hour

// cronDescriptors maps @-descriptors to equivalent five-field expressions.
var cronDescriptors = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// cronMonthNames maps three-letter month names to month numbers.
var cronMonthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

// cronDayNames maps three-letter day names to weekday numbers.
var cronDayNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// Schedule computes fire times for Scheduler.On.
// Implementations must be safe for concurrent use.
type Schedule interface {
	// Next returns the next fire time strictly after `after`.
	// Returning the zero time (or a time not after `after`) ends the schedule.
	Next(after time.Time) time.Time
}

// CronSchedule is a Schedule parsed from a standard five-field cron expression.
// Fire times are computed in the location of the time passed to Next.
type CronSchedule struct {
	expr   string // Original expression for observability.
	minute uint64 // Bitmask of matching minutes (0-59).
	hour   uint64 // Bitmask of matching hours (0-23).
	dom    uint64 // Bitmask of matching days of month (1-31).
	month  uint64 // Bitmask of matching months (1-12).
	dow    uint64 // Bitmask of matching days of week (0-6, Sunday=0).

	domStar bool // Whether day-of-month is unrestricted ("*").
	dowStar bool // Whether day-of-week is unrestricted ("*").
}

// MustParseCron is ParseCron that panics on error, for package-level schedules.
func MustParseCron(expr string) *CronSchedule {
	cs, err := ParseCron(expr)
	if err != nil {
		panic(err)
	}
	return cs
}

// ParseCron parses a standard five-field cron expression into a CronSchedule.
//
// Fields are: minute (0-59), hour (0-23), day-of-month (1-31), month (1-12 or
// jan-dec), day-of-week (0-7 or sun-sat, both 0 and 7 mean Sunday). Each field
// supports "*", values, ranges (a-b), steps (*/n, a-b/n, a/n), and lists (a,b-c).
// The @yearly, @annually, @monthly, @weekly, @daily, @midnight, and @hourly
// descriptors are also supported.
//
// Following Vixie cron, when both day-of-month and day-of-week are restricted
// (neither is "*"), a day matches when either field matches.
func ParseCron(expr string) (*CronSchedule, error) {
	original := strings.TrimSpace(expr)
	resolved := original
	if strings.HasPrefix(resolved, "@") {
		descriptor, ok := cronDescriptors[strings.ToLower(resolved)]
		if !ok {
			return nil, fmt.Errorf("%w: unknown descriptor %q", ErrCronExprInvalid, original)
		}
		resolved = descriptor
	}

	fields := strings.Fields(resolved)
	if len(fields) != 5 {
		return nil, fmt.Errorf("%w: expected 5 fields, got %d in %q", ErrCronExprInvalid, len(fields), original)
	}

	minute, _, err := parseCronField(fields[0], 0, 59, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: minute field %q: %w", ErrCronExprInvalid, fields[0], err)
	}

	hour, _, err := parseCronField(fields[1], 0, 23, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: hour field %q: %w", ErrCronExprInvalid, fields[1], err)
	}

	dom, domStar, err := parseCronField(fields[2], 1, 31, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: day-of-month field %q: %w", ErrCronExprInvalid, fields[2], err)
	}

	month, _, err := parseCronField(fields[3], 1, 12, cronMonthNames)
	if err != nil {
		return nil, fmt.Errorf("%w: month field %q: %w", ErrCronExprInvalid, fields[3], err)
	}

	dow, dowStar, err := parseCronField(fields[4], 0, 7, cronDayNames)
	if err != nil {
		return nil, fmt.Errorf("%w: day-of-week field %q: %w", ErrCronExprInvalid, fields[4], err)
	}
	if dow&(1<<7) != 0 {
		// Fold day-of-week 7 (Sunday) into 0.
		dow = (dow &^ (1 << 7)) | 1
	}

	return &CronSchedule{
		expr:    original,
		minute:  minute,
		hour:    hour,
		dom:     dom,
		month:   month,
		dow:     dow,
		domStar: domStar,
		dowStar: dowStar,
	}, nil
}

// String returns the original cron expression.
func (c *CronSchedule) String() string {
	return c.expr
}

// Next returns the next fire time strictly after `after`, computed in
// after's location. It returns the zero time when no fire time exists
// within a five-year lookahead.
func (c *CronSchedule) Next(after time.Time) time.Time {
	loc := after.Location()
	// Start at the next whole minute.
	t := time.Date(after.Year(), after.Month(), after.Day(), after.Hour(), after.Minute(), 0, 0, loc).Add(time.Minute)
	limit := after.Add(cronMaxLookahead)

	for {
		if t.After(limit) {
			return time.Time{} // No matching time within lookahead... impossible expression.
		}

		if c.month&(1<<uint(t.Month())) == 0 {
			// Month mismatch... advance to the first of the next month.
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc).AddDate(0, 1, 0)
			continue
		}

		if !c.dayMatches(t) {
			// Day mismatch... advance to the next day at midnight.
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, 1)
			continue
		}

		if c.hour&(1<<uint(t.Hour())) == 0 {
			// Hour mismatch... advance to the next hour.
			// Add on absolute time so DST transitions converge.
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc).Add(time.Hour)
			continue
		}

		if c.minute&(1<<uint(t.Minute())) == 0 {
			t = t.Add(time.Minute)
			continue
		}

		return t
	}
}

// dayMatches reports whether t's day satisfies the day-of-month and
// day-of-week fields under Vixie cron OR semantics.
func (c *CronSchedule) dayMatches(t time.Time) bool {
	domMatch := c.dom&(1<<uint(t.Day())) != 0
	dowMatch := c.dow&(1<<uint(t.Weekday())) != 0

	switch {
	case c.domStar && c.dowStar:
		return true
	case c.domStar:
		return dowMatch
	case c.dowStar:
		return domMatch
	default:
		return domMatch || dowMatch
	}
}

// parseCronField parses one cron field into a bitmask of matching values.
// `star` reports whether any term left the field unrestricted ("*" or "?").
func parseCronField(field string, min int, max int, names map[string]int) (bits uint64, star bool, err error) {
	for _, term := range strings.Split(field, ",") {
		if term == "" {
			return 0, false, fmt.Errorf("empty term")
		}

		rangePart := term
		step := 1
		if base, stepPart, found := strings.Cut(term, "/"); found {
			rangePart = base
			step, err = strconv.Atoi(stepPart)
			if err != nil || step <= 0 {
				return 0, false, fmt.Errorf("invalid step %q", stepPart)
			}
		}

		lo, hi := min, max
		switch {
		case rangePart == "*" || rangePart == "?":
			// A stepped star ("*/n") restricts the field, so it does not
			// count as unrestricted for the day-matching OR rule.
			if step == 1 {
				star = true
			}
		default:
			loPart, hiPart, isRange := strings.Cut(rangePart, "-")
			lo, err = parseCronValue(loPart, names)
			if err != nil {
				return 0, false, err
			}
			switch {
			case isRange:
				hi, err = parseCronValue(hiPart, names)
				if err != nil {
					return 0, false, err
				}
			case strings.Contains(term, "/"):
				// Single value with step ("a/n") means a through max.
				hi = max
			default:
				hi = lo
			}
		}

		if lo < min || hi > max || lo > hi {
			return 0, false, fmt.Errorf("value out of range [%d,%d] in %q", min, max, term)
		}
		for v := lo; v <= hi; v += step {
			bits |= 1 << uint(v)
		}
	}
	return bits, star, nil
}

// parseCronValue parses one cron field value, resolving names when provided.
func parseCronValue(raw string, names map[string]int) (int, error) {
	if names != nil {
		if v, ok := names[strings.ToLower(raw)]; ok {
			return v, nil
		}
	}

	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", raw)
	}
	return v, nil
}
