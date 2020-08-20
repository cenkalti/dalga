package table

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/senseyeio/duration"
)

// Job is the record stored in jobs table.
// Primary key for the table is Key.
type Job struct {
	Key
	// Interval is the duration between each POST to the endpoint.
	// Interval is "" for one-off jobs.
	Interval duration.Duration
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
	var id uint32
	if j.InstanceID != nil {
		id = *j.InstanceID
	}
	format := "2006-01-02T15:04:05"
	return fmt.Sprintf("Job<%q, %q, %s, %s, %d>", j.Path, j.Body, j.Interval.String(), j.NextRun.Format(format), id)
}

// OneOff returns true for one-off jobs. One-off jobs are stored with empty interval on jobs table.
func (j *Job) OneOff() bool {
	return j.Interval.IsZero()
}

func (j *Job) MarshalJSON() ([]byte, error) {
	return json.Marshal(JobJSON{
		Path:       j.Path,
		Body:       j.Body,
		Interval:   j.Interval.String(),
		NextRun:    j.NextRun.Format(time.RFC3339),
		InstanceID: j.InstanceID,
	})
}

type JobJSON struct {
	Path       string  `json:"path"`
	Body       string  `json:"body"`
	Interval   string  `json:"interval"`
	NextRun    string  `json:"next_run"`
	InstanceID *uint32 `json:"instance_id"`
}
