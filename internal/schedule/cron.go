// Package schedule is the cron + DST + misfire logic. No database in here on
// purpose, so it's easy to test.
package schedule

import (
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
)

// Parsed zones and cron specs never change, so the cache can't go stale. We
// just wipe it if it somehow grows past this.
const cacheLimit = 10_000

// Calculator computes fire times. Parsing is cached because every claim of
// a recurring run calls Next.
type Calculator struct {
	parser    cron.Parser
	mu        sync.Mutex
	locations map[string]*time.Location
	specs     map[string]cron.Schedule
}

func NewCalculator() *Calculator {
	return &Calculator{
		parser:    cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		locations: make(map[string]*time.Location),
		specs:     make(map[string]cron.Schedule),
	}
}

// Validate checks the zone, and the cron expression if there is one.
func (c *Calculator) Validate(expression, timezone string) error {
	if _, err := c.location(timezone); err != nil {
		return err
	}
	if expression == "" {
		return nil
	}
	_, err := c.spec(expression)
	return err
}

// Next returns the first fire time after `after`, on the tenant's wall clock.
//
// DST: if the time doesn't exist that day (02:30 when clocks jump 02:00 to
// 03:00) we skip that day. If it happens twice (01:30 on fall-back day) we
// fire once, on the first one.
func (c *Calculator) Next(after time.Time, expression, timezone string) (time.Time, error) {
	loc, err := c.location(timezone)
	if err != nil {
		return time.Time{}, err
	}
	spec, err := c.spec(expression)
	if err != nil {
		return time.Time{}, err
	}
	next := spec.Next(after.In(loc))
	if isRepeatedWallTime(next) {
		next = spec.Next(next.Add(time.Second))
	}
	return next.UTC(), nil
}

// isRepeatedWallTime is true when t is the second 01:30 of a fall-back
// day, i.e. the same wall time already happened a little earlier. Real DST
// shifts are 30m, 1h or 2h, so checking those is enough.
func isRepeatedWallTime(t time.Time) bool {
	const wall = "2006-01-02 15:04:05"
	for _, shift := range []time.Duration{30 * time.Minute, time.Hour, 2 * time.Hour} {
		if t.Add(-shift).Format(wall) == t.Format(wall) {
			return true
		}
	}
	return false
}

func (c *Calculator) location(name string) (*time.Location, error) {
	c.mu.Lock()
	loc, ok := c.locations[name]
	c.mu.Unlock()
	if ok {
		return loc, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil || name == "" {
		return nil, domain.Invalid("invalid timezone %q", name)
	}
	c.mu.Lock()
	if len(c.locations) >= cacheLimit {
		clear(c.locations)
	}
	c.locations[name] = loc
	c.mu.Unlock()
	return loc, nil
}

func (c *Calculator) spec(expression string) (cron.Schedule, error) {
	c.mu.Lock()
	spec, ok := c.specs[expression]
	c.mu.Unlock()
	if ok {
		return spec, nil
	}
	spec, err := c.parser.Parse(expression)
	if err != nil {
		return nil, domain.Invalid("invalid cron expression %q", expression)
	}
	c.mu.Lock()
	if len(c.specs) >= cacheLimit {
		clear(c.specs)
	}
	c.specs[expression] = spec
	c.mu.Unlock()
	return spec, nil
}
