package retry

import "time"

type Retry struct {
	Interval    time.Duration
	Multiplier  float64
	MaxInterval time.Duration
}

// NextRun returns the time when the jobs must be retried again.
func (r *Retry) NextRun(sched, now time.Time) time.Time {
	passed := now.Sub(sched)
	periods := float64(passed) / float64(r.Interval)
	delay := float64(r.Multiplier-1)*periods + 1
	interval := time.Duration(delay * float64(r.Interval))
	if interval > r.MaxInterval {
		interval = r.MaxInterval
	}
	return now.Add(interval)
}
