package retry

import "time"

type Retry struct {
	Interval    time.Duration
	Multiplier  float64
	MaxInterval time.Duration
	StopAfter   time.Duration
}

// NextRun returns the time when the jobs must be retried again.
func (r *Retry) NextRun(sched, now time.Time) time.Time {
	passed := now.Sub(sched)
	periods := float64(passed) / float64(r.Interval)
	delay := (r.Multiplier-1)*periods + 1
	interval := time.Duration(delay * float64(r.Interval))
	if interval > r.MaxInterval {
		interval = r.MaxInterval
	}
	nextRun := now.Add(interval)
	if r.StopAfter > 0 && nextRun.Sub(sched) > r.StopAfter {
		return time.Time{}
	}
	return nextRun
}
