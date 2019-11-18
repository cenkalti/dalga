package jobmanager

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/cenkalti/dalga/internal/scheduler"
	"github.com/cenkalti/dalga/internal/table"
)

var debugging bool

func EnableDebug() {
	debugging = true
}

func debug(args ...interface{}) {
	if debugging {
		log.Println(args...)
	}
}

type JobManager struct {
	table     *table.Table
	scheduler *scheduler.Scheduler
}

type ScheduleOptions struct {
	OneOff    bool
	Immediate bool
	FirstRun  time.Time
	Interval  time.Duration
}

var ErrInvalidArgs = errors.New("invalid arguments")

func New(t *table.Table, s *scheduler.Scheduler) *JobManager {
	return &JobManager{
		table:     t,
		scheduler: s,
	}
}

func (m *JobManager) Get(ctx context.Context, path, body string) (*table.Job, error) {
	return m.table.Get(ctx, path, body)
}

// Schedule inserts a new job to the table or replaces existing one.
// Returns the created or replaced job.
func (m *JobManager) Schedule(ctx context.Context, path, body string, opt ScheduleOptions) (*table.Job, error) {
	key := table.Key{
		Path: path,
		Body: body,
	}

	var interval time.Duration
	var delay time.Duration
	var nextRun time.Time
	switch {
	case opt.OneOff && opt.Immediate: // one-off and immediate
		// both first-run and interval params must be zero
		if !opt.FirstRun.IsZero() || opt.Interval != 0 {
			return nil, ErrInvalidArgs
		}
	case opt.OneOff && !opt.Immediate: // one-off but later
		// only one of from first-run and interval params must be set
		if (!opt.FirstRun.IsZero() && opt.Interval != 0) || (opt.FirstRun.IsZero() && opt.Interval == 0) {
			return nil, ErrInvalidArgs
		}
		if opt.Interval != 0 {
			delay = opt.Interval
		} else {
			nextRun = opt.FirstRun
		}
	case !opt.OneOff && opt.Immediate: // periodic and immediate
		if opt.Interval == 0 || !opt.FirstRun.IsZero() {
			return nil, ErrInvalidArgs
		}
		interval = opt.Interval
	default: // periodic
		if opt.Interval == 0 {
			return nil, ErrInvalidArgs
		}
		interval = opt.Interval
		if !opt.FirstRun.IsZero() {
			nextRun = opt.FirstRun
		} else {
			delay = opt.Interval
		}
	}
	debug("job is scheduled:", key)
	return m.table.AddJob(ctx, key, interval, delay, nextRun)
}

// Cancel deletes the job with path and body.
func (m *JobManager) Cancel(ctx context.Context, path, body string) error {
	debug("job is cancelled")
	return m.table.DeleteJob(ctx, table.Key{Path: path, Body: body})
}

// Running returns the number of running jobs currently.
func (m *JobManager) Running() int {
	return m.scheduler.Running()
}

// Total returns the count of all jobs in jobs table.
func (m *JobManager) Total(ctx context.Context) (int64, error) {
	return m.table.Count(ctx)
}
