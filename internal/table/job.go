package table

import (
	"encoding/json"
	"fmt"
	"time"
)

// Job is the record stored in jobs table.
// Primary key for the table is Key.
type Job struct {
	Key
	// Interval is the duration between each POST to the endpoint.
	// Interval is 0 for one-off jobs.
	Interval time.Duration
	// NextRun is the next run time of the job, stored in UTC.
	NextRun time.Time
	// Job is running if not nil.
	InstanceID *uint32
}

type Key struct {
	// Path is where the job is going to be POSTed when it's time came.
	Path string
	// Body of POST request.
	Body string
}

// String returns the job in human-readable form.
func (j *Job) String() string {
	return fmt.Sprintf("Job{%q, %q, %s, %s}", j.Body, j.Path, j.Interval, j.NextRun.String()[:23])
}

// OneOff returns true for one-off jobs. One-off jobs are stored with 0 interval on jobs table.
func (j *Job) OneOff() bool {
	return j.Interval == 0
}

func (j *Job) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Path       string        `json:"path"`
		Body       string        `json:"body"`
		Interval   time.Duration `json:"interval"`
		NextRun    string        `json:"next_run"`
		InstanceID *uint32       `json:"instance_id"`
	}{
		Path:       j.Path,
		Body:       j.Body,
		Interval:   j.Interval / time.Second,
		NextRun:    j.NextRun.Format(time.RFC3339),
		InstanceID: j.InstanceID,
	})
}
