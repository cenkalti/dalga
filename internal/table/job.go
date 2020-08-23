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
	// Interval is relative to the Location.
	// Format is the tz database name, such as America/Los_Angeles.
	Location *time.Location
	// NextRun is the next run time of the job, including retries.
	NextRun time.Time
	// NextSched is the next time the job is scheduled to run, regardless of retries.
	NextSched time.Time
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
	return fmt.Sprintf("Job<%q, %q, %s, %s, %s, %s, %d>", j.Path, j.Body, j.Interval.String(), j.Location.String(), j.NextRun.Format(time.RFC3339), j.NextSched.Format(time.RFC3339), id)
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
		Location:   j.Location.String(),
		NextRun:    j.NextRun.Format(time.RFC3339),
		NextSched:  j.NextSched.Format(time.RFC3339),
		InstanceID: j.InstanceID,
	})
}

type JobJSON struct {
	Path       string  `json:"path"`
	Body       string  `json:"body"`
	Interval   string  `json:"interval"`
	Location   string  `json:"location"`
	NextRun    string  `json:"next_run"`
	NextSched  string  `json:"next_sched"`
	InstanceID *uint32 `json:"instance_id"`
}
