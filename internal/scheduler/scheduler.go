package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cenkalti/dalga/v2/internal/log"
	"github.com/cenkalti/dalga/v2/internal/table"
	"github.com/go-sql-driver/mysql"
)

type Scheduler struct {
	table               *table.Table
	instanceID          uint32
	client              http.Client
	baseURL             string
	randomizationFactor float64
	retryInterval       time.Duration
	runningJobs         int32
	done                chan struct{}
}

func New(t *table.Table, instanceID uint32, baseURL string, clientTimeout, retryInterval time.Duration, randomizationFactor float64) *Scheduler {
	return &Scheduler{
		table:               t,
		instanceID:          instanceID,
		baseURL:             baseURL,
		randomizationFactor: randomizationFactor,
		retryInterval:       retryInterval,
		done:                make(chan struct{}),
		client: http.Client{
			Timeout: clientTimeout,
		},
	}
}

func (s *Scheduler) NotifyDone() <-chan struct{} {
	return s.done
}

func (s *Scheduler) Running() int {
	return int(atomic.LoadInt32(&s.runningJobs))
}

// Run runs a loop that reads the next Job from the queue and executees it in it's own goroutine.
func (s *Scheduler) Run(ctx context.Context) {
	defer close(s.done)

	for {
		log.Debugln("---")

		job, err := s.table.Front(ctx, s.instanceID)
		if err == context.Canceled {
			return
		}
		if err == sql.ErrNoRows {
			log.Debugln("no scheduled jobs in the table")
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		if myErr, ok := err.(*mysql.MySQLError); ok && myErr.Number == 1146 {
			// Table doesn't exist
			log.Fatal(myErr)
		}
		if err != nil {
			log.Println("error while getting next job:", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}

		go func(job *table.Job) {
			atomic.AddInt32(&s.runningJobs, 1)
			if err := s.execute(ctx, job); err != nil {
				log.Printf("error on execution of %s: %s", job.String(), err)
			}
			atomic.AddInt32(&s.runningJobs, -1)
		}(job)
	}
}

func randomize(d time.Duration, f float64) time.Duration {
	delta := time.Duration(f * float64(d))
	return d - delta + time.Duration(float64(2*delta)*rand.Float64())
}

// execute makes a POST request to the endpoint and updates the Job's next run time.
func (s *Scheduler) execute(ctx context.Context, j *table.Job) error {
	log.Debugln("executing:", j.String())
	code, err := s.postJob(ctx, j)
	if err != nil {
		log.Printf("error while doing http post for %s: %s", j.String(), err)
		return s.table.UpdateNextRun(ctx, j.Key, s.retryInterval)
	}
	if j.OneOff() {
		log.Debugln("deleting one-off job")
		return s.table.DeleteJob(ctx, j.Key)
	}
	if code == 204 {
		log.Debugln("deleting not found job")
		return s.table.DeleteJob(ctx, j.Key)
	}
	add := j.Interval
	if s.randomizationFactor > 0 {
		// Add some randomization to periodic tasks.
		add = randomize(add, s.randomizationFactor)
	}
	return s.table.UpdateNextRun(ctx, j.Key, add)
}

func (s *Scheduler) postJob(ctx context.Context, j *table.Job) (code int, err error) {
	url := s.baseURL + j.Path
	log.Debugln("doing http post to ", url)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(j.Body))
	if err != nil {
		return
	}
	req.Header.Set("content-type", "text/plain")
	resp, err := s.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200, 204:
		code = resp.StatusCode
	default:
		err = fmt.Errorf("endpoint error: %d", resp.StatusCode)
	}
	return
}
