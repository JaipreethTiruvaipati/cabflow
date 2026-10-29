package schedule

import (
	"testing"
	"time"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
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

func TestMisfirePolicies(t *testing.T) {
	c := NewCalculator()
	scheduled := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	late := scheduled.Add(45 * time.Minute) // missed 08:00 through 08:40
	job := func(policy domain.MisfirePolicy) domain.Job {
		return domain.Job{Cron: "*/10 * * * *", Timezone: "UTC", MisfirePolicy: policy}
	}
	cases := []struct {
		name     string
		policy   domain.MisfirePolicy
		now      time.Time
		wantRun  bool
		wantNext time.Time
	}{
		{"on time", domain.Skip, scheduled.Add(2 * time.Second), true, scheduled.Add(10 * time.Minute)},
		{"fire now replays each missed run", domain.FireNow, late, true, scheduled.Add(10 * time.Minute)},
		{"coalesce runs once then jumps ahead", domain.Coalesce, late, true, scheduled.Add(50 * time.Minute)},
		{"skip drops the run and jumps ahead", domain.Skip, late, false, scheduled.Add(50 * time.Minute)},
	}
	for _, tc := range cases {
		got, err := Decide(job(tc.policy), scheduled, tc.now, c)
		if err != nil {
			t.Fatal(err)
		}
		if got.Run != tc.wantRun || !got.Next.Equal(tc.wantNext) {
			t.Errorf("%s: got run=%v next=%s, want run=%v next=%s", tc.name, got.Run, got.Next, tc.wantRun, tc.wantNext)
		}
	}
}

func TestOneOffJobHasNoNextRun(t *testing.T) {
	got, err := Decide(domain.Job{Timezone: "UTC", MisfirePolicy: domain.Skip}, time.Unix(0, 0), time.Unix(3600, 0), NewCalculator())
	if err != nil {
		t.Fatal(err)
	}
	if got.Run || !got.Next.IsZero() {
		t.Fatalf("late one-off SKIP job: want no run and no next, got %+v", got)
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
