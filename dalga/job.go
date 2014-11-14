package dalga

import (
	"encoding/json"
	"fmt"
	"time"
)

// Job is the record stored in jobs table.
// Primary key is (Path, Body).
type Job struct {
	// Path is where the job is going to be POSTed when it's time came.
	Path string
	// Body of POST request.
	Body string
	// Interval is the duration between each publish to the endpoint.
	// If interval is 0 job will be deleted after first publish.
	Interval time.Duration
	// NextRun is the next run time of the job, stored in UTC.
	NextRun time.Time
}

func newJob(description, routingKey string, interval uint32, oneOff bool) *Job {
	j := Job{
		Body:    description,
		Path:    routingKey,
		NextRun: time.Now().UTC().Add(time.Duration(interval) * time.Second),
	}
	if oneOff {
		j.Interval = 0
	}
	return &j
}

// String implements Stringer interface. Returns the job in human-readable form.
func (j *Job) String() string {
	return fmt.Sprintf("Job{%q, %q, %d, %s}", j.Body, j.Path, j.Interval/time.Second, j.NextRun.String()[:23])
}

// Remaining returns the duration left to the job's next run time.
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
