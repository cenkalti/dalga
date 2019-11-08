package dalga

import (
	"errors"
	"time"
)

type JobManager struct {
	table     *table
	scheduler *scheduler
}

type ScheduleOptions struct {
	OneOff    bool
	Immediate bool
	FirstRun  time.Time
	Interval  time.Duration
}

var errInvalidArgs = errors.New("invalid arguments")

func newJobManager(t *table, s *scheduler) *JobManager {
	return &JobManager{
		table:     t,
		scheduler: s,
	}
}

// Get returns the job with path and body.
func (m *JobManager) Get(path, body string) (*Job, error) {
	return m.table.Get(path, body)
}

// Schedule inserts a new job to the table or replaces existing one.
// Returns the created or replaced job.
func (m *JobManager) Schedule(path, body string, opt ScheduleOptions) (*Job, error) {
	job := &Job{JobKey: JobKey{
		Path: path,
		Body: body,
	}}

	switch {
	case opt.OneOff && opt.Immediate: // one-off and immediate
		// both first-run and interval params must be zero
		if !opt.FirstRun.IsZero() || opt.Interval != 0 {
			return nil, errInvalidArgs
		}
		job.NextRun = time.Now().UTC()
	case opt.OneOff && !opt.Immediate: // one-off but later
		// only one of from first-run and interval params must be set
		if (!opt.FirstRun.IsZero() && opt.Interval != 0) || (opt.FirstRun.IsZero() && opt.Interval == 0) {
			return nil, errInvalidArgs
		}
		if opt.Interval != 0 {
			job.NextRun = time.Now().UTC().Add(opt.Interval)
		} else {
			job.NextRun = opt.FirstRun
		}
	case !opt.OneOff && opt.Immediate: // periodic and immediate
		if opt.Interval == 0 || !opt.FirstRun.IsZero() {
			return nil, errInvalidArgs
		}
		job.Interval = opt.Interval
		job.NextRun = time.Now().UTC()
	default: // periodic
		if opt.Interval == 0 {
			return nil, errInvalidArgs
		}
		job.Interval = opt.Interval
		if !opt.FirstRun.IsZero() {
			job.NextRun = opt.FirstRun
		} else {
			job.NextRun = time.Now().UTC().Add(opt.Interval)
		}
	}

	err := m.table.Insert(job)
	if err != nil {
		return nil, err
	}
	m.scheduler.WakeUp("new job")
	debug("job is scheduled:", job)
	return job, nil
}

// Trigger runs the job immediately and resets it's next run time.
func (m *JobManager) Trigger(path, body string) (*Job, error) {
	job, err := m.table.Get(path, body)
	if err != nil {
		return nil, err
	}
	job.NextRun = time.Now().UTC()
	if err := m.table.Insert(job); err != nil {
		return nil, err
	}
	m.scheduler.WakeUp("job is triggered")
	debug("job is triggered:", job)
	return job, nil
}

// Cancel deletes the job with path and body.
func (m *JobManager) Cancel(path, body string) error {
	err := m.table.Delete(path, body)
	if err != nil {
		return err
	}
	m.scheduler.WakeUp("job cancelled")
	debug("job is cancelled")
	return nil
}

// Running returns the number of running jobs currently.
func (m *JobManager) Running() int {
	return m.scheduler.Running()
}

// Total returns the count of all jobs in jobs table.
func (m *JobManager) Total() (int64, error) {
	return m.table.Count()
}
