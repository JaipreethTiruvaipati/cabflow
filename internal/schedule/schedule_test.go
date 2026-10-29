package schedule

import (
	"testing"
	"time"
)

func zone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// No 02:30 on spring-forward day, so that day gets skipped.
func TestSpringForwardSkipsMissingWallTime(t *testing.T) {
	c := NewCalculator()
	ny := zone(t, "America/New_York")
	next, err := c.Next(time.Date(2026, 3, 8, 0, 0, 0, 0, ny), "30 2 * * *", "America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	if got := next.In(ny); got.Day() != 9 || got.Hour() != 2 || got.Minute() != 30 {
		t.Fatalf("want 02:30 on March 9, got %s", got)
	}
}

// 01:30 happens twice on fall-back day; we want one run, the first one.
// Checked east and west of UTC since Go picks a different fold for each.
func TestFallBackRunsOnceAtFirstOccurrence(t *testing.T) {
	cases := []struct {
		zone, cron, earlier string
		after               time.Time
	}{
		{"America/New_York", "30 1 * * *", "EDT", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)},
		{"Europe/Berlin", "30 2 * * *", "CEST", time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)},
		{"Australia/Sydney", "30 2 * * *", "AEDT", time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC)},
	}
	c := NewCalculator()
	for _, tc := range cases {
		loc := zone(t, tc.zone)
		first, err := c.Next(tc.after, tc.cron, tc.zone)
		if err != nil {
			t.Fatal(err)
		}
		second, err := c.Next(first, tc.cron, tc.zone)
		if err != nil {
			t.Fatal(err)
		}
		if name, _ := first.In(loc).Zone(); name != tc.earlier {
			t.Errorf("%s: first run in %s, want the earlier copy (%s)", tc.zone, name, tc.earlier)
		}
		if second.Sub(first) < 23*time.Hour {
			t.Errorf("%s: ran twice on fall-back day: %s and %s", tc.zone, first.In(loc), second.In(loc))
		}
	}
}

func TestCronRunsOnTenantWallClock(t *testing.T) {
	c := NewCalculator()
	next, err := c.Next(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), "0 22 * * *", "Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 1, 1, 16, 30, 0, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("22:00 IST should be %s UTC, got %s", want, next)
	}
}

func BenchmarkNext(b *testing.B) {
	c := NewCalculator()
	after := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < b.N; i++ {
		if _, err := c.Next(after, "30 21 * * *", "Asia/Kolkata"); err != nil {
			b.Fatal(err)
		}
	}
}

