package dalga

import (
	"encoding/json"
	"fmt"
	"time"
)

// Job is the record stored in jobs table.
// Primary key for the table is JobKey.
type Job struct {
	JobKey
	// Interval is the duration between each POST to the endpoint.
	// Interval is 0 for one-off jobs.
	Interval time.Duration
	// NextRun is the next run time of the job, stored in UTC.
	NextRun time.Time
}

type JobKey struct {
	// Path is where the job is going to be POSTed when it's time came.
	Path string
	// Body of POST request.
	Body string
}

func newJob(path, body string, interval time.Duration, oneOff bool) *Job {
	j := Job{
		JobKey: JobKey{
			Path: path,
			Body: body,
		},
		NextRun: time.Now().UTC().Add(interval),
	}
	if !oneOff {
		j.Interval = interval
	}
	return &j
}

// String returns the job in human-readable form.
func (j *Job) String() string {
	return fmt.Sprintf("Job{%q, %q, %s, %s}", j.Body, j.Path, j.Interval, j.NextRun.String()[:23])
}

// OneOff returns true for one-off jobs. One-off jobs are stored with 0 interval on jobs table.
func (j *Job) OneOff() bool {
	return j.Interval == 0
}

// Remaining returns the remaining time to the job's next run time.
func (j *Job) Remaining() time.Duration {
	return j.NextRun.Sub(time.Now().UTC())
}

func (j *Job) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Path     string        `json:"routing_key"`
		Body     string        `json:"body"`
		Interval time.Duration `json:"interval"`
		NextRun  string        `json:"next_run"`
	}{
		Path:     j.Path,
		Body:     j.Body,
		Interval: j.Interval / time.Second,
		NextRun:  j.NextRun.Format(time.RFC3339),
	})
}
