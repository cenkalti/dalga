package dalga

import (
	"errors"
	"time"
)

type JobManager struct {
	table     *table
	scheduler *scheduler
}

var invalidArgs = errors.New("invalid arguments")

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
func (m *JobManager) Schedule(path, body string, oneOff, immediate bool, firstRun *time.Time, interval *time.Duration) (*Job, error) {
	job := &Job{JobKey: JobKey{
		Path: path,
		Body: body,
	}}

	if oneOff && immediate {
		if firstRun != nil || interval != nil {
			return nil, invalidArgs
		}
		job.NextRun = time.Now().UTC()
	} else if oneOff && !immediate {
		if firstRun != nil {
			job.NextRun = *firstRun
		} else if interval != nil {
			job.NextRun = time.Now().UTC().Add(*interval)
		} else {
			return nil, invalidArgs
		}
	} else if !oneOff && immediate { // periodic and immediate
		if interval == nil {
			return nil, invalidArgs
		}
		if firstRun != nil {
			return nil, invalidArgs
		}
		job.Interval = *interval
		job.NextRun = time.Now().UTC()
	} else { // periodic
		if interval == nil {
			return nil, invalidArgs
		}
		job.Interval = *interval
		if firstRun != nil {
			job.NextRun = *firstRun
		} else {
			job.NextRun = time.Now().UTC().Add(*interval)
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
