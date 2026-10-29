package schedule

import (
	"time"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
)

// MisfireThreshold is how late a run can start before we call it missed.
// Normal queueing is well under a second, so this only kicks in after real
// downtime or a long tenant pause.
const MisfireThreshold = time.Minute

// Decision: run this occurrence or not, and when the next one is (zero for
// one-off jobs).
type Decision struct {
	Run  bool
	Next time.Time
}

// Decide applies the misfire policy. On time, or FIRE_NOW, the next run just
// follows this one on the schedule; for FIRE_NOW that means the missed runs
// get replayed one by one. COALESCE runs this one and jumps ahead to the
// first slot after now. SKIP jumps ahead without running.
func Decide(job domain.Job, scheduledAt, now time.Time, calc *Calculator) (Decision, error) {
	late := now.Sub(scheduledAt) > MisfireThreshold
	d := Decision{Run: !(late && job.MisfirePolicy == domain.Skip)}
	if !job.Recurring() {
		return d, nil
	}
	from := scheduledAt
	if late && job.MisfirePolicy != domain.FireNow {
		from = now
	}
	var err error
	d.Next, err = calc.Next(from, job.Cron, job.Timezone)
	return d, err
}

func Upcoming(job domain.Job, first time.Time, limit int, calc *Calculator) ([]time.Time, error) {
	times := []time.Time{first.UTC()}
	for job.Recurring() && len(times) < limit {
		next, err := calc.Next(times[len(times)-1], job.Cron, job.Timezone)
		if err != nil {
			return nil, err
		}
		times = append(times, next)
	}
	return times, nil
}
