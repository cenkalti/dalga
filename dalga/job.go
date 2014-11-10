package dalga

import (
	"fmt"
	"time"
)

type Job struct {
	primaryKey
	Interval time.Duration
	NextRun  time.Time
}

type primaryKey struct {
	ID         string
	RoutingKey string
}

func NewJob(id, routingKey string, interval uint32) *Job {
	job := Job{
		primaryKey: primaryKey{id, routingKey},
		Interval:   time.Duration(interval) * time.Second,
	}
	job.SetNewNextRun()
	return &job
}

// String implements Stringer interface. Returns the job in human-readable form.
func (j *Job) String() string {
	return fmt.Sprintf("Job{%q, %q, %d, %s}", j.ID, j.RoutingKey, j.Interval/time.Second, j.NextRun.String()[:23])
}

// Remaining returns the duration until the job's next scheduled time.
func (j *Job) Remaining() time.Duration {
	return j.NextRun.Sub(time.Now().UTC())
}

// SetNewNextRun calculates the new run time according to current time and sets it on the job.
func (j *Job) SetNewNextRun() {
	j.NextRun = time.Now().UTC().Add(j.Interval)
}

func (j *Job) Equal(k *Job) bool {
	return j.primaryKey == k.primaryKey
}
