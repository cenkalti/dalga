package jobmanager

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/cenkalti/dalga/v4/internal/log"
	"github.com/cenkalti/dalga/v4/internal/scheduler"
	"github.com/cenkalti/dalga/v4/internal/table"
	"github.com/senseyeio/duration"
)

const (
	orderInsert    = "insert-order"
	orderNextSched = "next-sched"
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

func (m *JobManager) List(ctx context.Context, path, sortBy string, reverse bool, limit int64) (jobs []table.Job, cursor string, err error) {
	var orderByColumn string
	switch sortBy {
	case orderInsert:
		orderByColumn = "id"
	case orderNextSched:
		orderByColumn = "next_sched, id"
	default:
		return nil, "", errors.New("invalid sort-by param")
	}
	jobsWithID, err := m.table.List(ctx, path, orderByColumn, reverse, "", "", limit)
	if err != nil {
		return nil, "", err
	}
	jobs = make([]table.Job, 0, len(jobsWithID))
	for _, j := range jobsWithID {
		jobs = append(jobs, j.Job)
	}
	if len(jobsWithID) > 0 {
		lastItem := jobsWithID[len(jobsWithID)-1]
		cursor = generateCursor(lastItem, path, sortBy, reverse, limit)
	}
	return
}

func (m *JobManager) ListContinue(ctx context.Context, cursor string) (jobs []table.Job, nextCursor string, err error) {
	var c Cursor
	err = c.Decode(cursor)
	if err != nil {
		return nil, "", errors.New("invalid cursor")
	}
	var orderByColumn, greaterThan, lessThan string
	switch c.SortBy {
	case orderInsert:
		orderByColumn = "id"
		if c.Reverse {
			lessThan = c.LastValue
		} else {
			greaterThan = c.LastValue
		}
	case orderNextSched:
		orderByColumn = "next_sched, id"
		if c.Reverse {
			lessThan = "'" + c.LastValue + "'"
		} else {
			greaterThan = "'" + c.LastValue + "'"
		}
	default:
		return nil, "", errors.New("invalid sort-by param")
	}
	jobsWithID, err := m.table.List(ctx, c.Path, orderByColumn, c.Reverse, greaterThan, lessThan, c.Limit)
	if err != nil {
		return nil, "", err
	}
	jobs = make([]table.Job, 0, len(jobsWithID))
	for _, j := range jobsWithID {
		jobs = append(jobs, j.Job)
	}
	if len(jobsWithID) > 0 {
		lastItem := jobsWithID[len(jobsWithID)-1]
		nextCursor = generateCursor(lastItem, c.Path, c.SortBy, c.Reverse, c.Limit)
	}
	return
}

func generateCursor(job table.JobWithID, path, sortBy string, reverse bool, limit int64) string {
	c := Cursor{
		Path:    path,
		SortBy:  sortBy,
		Reverse: reverse,
		Limit:   limit,
	}
	switch sortBy {
	case orderInsert:
		c.LastValue = strconv.FormatInt(job.ID, 10)
	case orderNextSched:
		c.LastValue = job.NextSched.Format(time.RFC3339)
	}
	return c.Encode()
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
