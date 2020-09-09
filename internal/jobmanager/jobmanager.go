package jobmanager

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/dalga/v3/internal/log"
	"github.com/cenkalti/dalga/v3/internal/scheduler"
	"github.com/cenkalti/dalga/v3/internal/table"
	"github.com/senseyeio/duration"
)

type JobManager struct {
	table     *table.Table
	scheduler *scheduler.Scheduler
}

type ScheduleOptions struct {
	OneOff    bool
	Immediate bool
	FirstRun  time.Time
	Location  *time.Location
	Interval  duration.Duration
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

	var interval duration.Duration
	var delay duration.Duration
	var nextRun time.Time
	switch {
	case opt.OneOff && opt.Immediate: // one-off and immediate
		// both first-run and interval params must be zero
		if !opt.FirstRun.IsZero() || !opt.Interval.IsZero() {
			return nil, ErrInvalidArgs
		}
	case opt.OneOff && !opt.Immediate: // one-off but later
		// only one of from first-run and interval params must be set
		if (!opt.FirstRun.IsZero() && !opt.Interval.IsZero()) || (opt.FirstRun.IsZero() && opt.Interval.IsZero()) {
			return nil, ErrInvalidArgs
		}
		if !opt.Interval.IsZero() {
			delay = opt.Interval
		} else {
			nextRun = opt.FirstRun
		}
	case !opt.OneOff && opt.Immediate: // periodic and immediate
		if opt.Interval.IsZero() || !opt.FirstRun.IsZero() {
			return nil, ErrInvalidArgs
		}
		interval = opt.Interval
	default: // periodic
		if opt.Interval.IsZero() {
			return nil, ErrInvalidArgs
		}
		interval = opt.Interval
		if !opt.FirstRun.IsZero() {
			nextRun = opt.FirstRun
		} else {
			delay = opt.Interval
		}
	}
	log.Debugln("job is scheduled:", key.Path, key.Body)
	return m.table.AddJob(ctx, key, interval, delay, opt.Location, nextRun)
}

// Disable prevents a job from from running until re-enabled.
// Disabling an I/P job does not cancel its current run.
func (m *JobManager) Disable(ctx context.Context, path, body string) (*table.Job, error) {
	log.Debugln("job is disabled:", path, body)
	return m.table.DisableJob(ctx, table.Key{Path: path, Body: body})
}

// Enable reschedules a disabled job so that it will run again.
func (m *JobManager) Enable(ctx context.Context, path, body string) (*table.Job, error) {
	log.Debugln("job is enabled:", path, body)
	return m.table.EnableJob(ctx, table.Key{Path: path, Body: body})
}

// Cancel deletes the job with path and body.
func (m *JobManager) Cancel(ctx context.Context, path, body string) error {
	log.Debugln("job is cancelled:", path, body)
	return m.table.DeleteJob(ctx, table.Key{Path: path, Body: body})
}

// Running returns the number of running jobs currently.
func (m *JobManager) Running() int {
	return m.scheduler.Running()
}
