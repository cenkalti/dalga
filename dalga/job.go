package dalga

import (
	"bytes"
	"fmt"
	"time"
)

type Job struct {
	RoutingKey string
	Body       []byte
	Interval   time.Duration
	NextRun    time.Time
}

func NewJob(routingKey string, body []byte, interval uint32) *Job {
	job := Job{
		RoutingKey: routingKey,
		Body:       body,
		Interval:   time.Duration(interval) * time.Second,
	}
	job.SetNewNextRun()
	return &job
}

// String implements Stringer interface. Returns the job in human-readable form.
func (j *Job) String() string {
	return fmt.Sprintf("Job{%q, %q, %d, %s}", j.RoutingKey, string(j.Body), j.Interval/time.Second, j.NextRun.String()[:23])
}

// Remaining returns the duration until the job's next scheduled time.
func (j *Job) Remaining() time.Duration {
	return -time.Since(j.NextRun)
}

// SetNewNextRun calculates the new run time according to current time and sets it on the job.
func (j *Job) SetNewNextRun() {
	j.NextRun = time.Now().UTC().Add(j.Interval)
}

func (j *Job) Equals(k *Job) bool {
	return (j.RoutingKey == k.RoutingKey) && (bytes.Compare(j.Body, k.Body) == 0)
}
