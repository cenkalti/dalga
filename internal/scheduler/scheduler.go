package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/dalga/v3/internal/log"
	"github.com/cenkalti/dalga/v3/internal/retry"
	"github.com/cenkalti/dalga/v3/internal/table"
	"github.com/go-sql-driver/mysql"
)

type Scheduler struct {
	table               *table.Table
	instanceID          uint32
	client              http.Client
	baseURL             string
	randomizationFactor float64
	retryParams         *retry.Retry
	runningJobs         int32
	scanFrequency       time.Duration
	done                chan struct{}
	wg                  sync.WaitGroup
	maxRunning          chan struct{}

	// Flags to disable some functionality for running benchmarks.
	skipOneOffDelete bool
	skipPost         bool
}

func New(t *table.Table, instanceID uint32, baseURL string, clientTimeout time.Duration, retryParams *retry.Retry, randomizationFactor float64, scanFrequency time.Duration, maxRunning int) *Scheduler {
	s := &Scheduler{
		table:               t,
		instanceID:          instanceID,
		baseURL:             baseURL,
		randomizationFactor: randomizationFactor,
		retryParams:         retryParams,
		scanFrequency:       scanFrequency,
		done:                make(chan struct{}),
		client: http.Client{
			Timeout: clientTimeout,
		},
	}
	// Create a semaphore channel to limit max running jobs.
	if maxRunning > 0 {
		s.maxRunning = make(chan struct{}, maxRunning)
	}
	return s
}

func (s *Scheduler) NotifyDone() <-chan struct{} {
	return s.done
}

func (s *Scheduler) Running() int {
	return int(atomic.LoadInt32(&s.runningJobs))
}

// Run runs a loop that reads the next Job from the queue and executees it in it's own goroutine.
func (s *Scheduler) Run(ctx context.Context) {
	defer func() {
		s.wg.Wait()
		close(s.done)
	}()

	for {
		log.Debugln("---")
		ok := s.runOnce(ctx)
		if !ok {
			return
		}
	}
}

func (s *Scheduler) runOnce(ctx context.Context) bool {
	job, err := s.table.Front(ctx, s.instanceID)
	if err == context.Canceled {
		return false
	}
	if err == sql.ErrNoRows {
		log.Debugln("no scheduled jobs in the table")
		select {
		case <-time.After(s.scanFrequency):
		case <-ctx.Done():
			return false
		}
		return true
	}
	if myErr, ok := err.(*mysql.MySQLError); ok && myErr.Number == 1146 {
		// Table doesn't exist
		log.Fatal(myErr)
	}
	if err != nil {
		log.Println("error while getting next job:", err)
		select {
		case <-time.After(s.scanFrequency):
		case <-ctx.Done():
			return false
		}
		return true
	}
	if s.maxRunning != nil {
		// Wait for semaphore
		select {
		case s.maxRunning <- struct{}{}:
		case <-ctx.Done():
			return false
		default:
			log.Printf("max running jobs (%d) has been reached", cap(s.maxRunning))
			select {
			case s.maxRunning <- struct{}{}:
			case <-ctx.Done():
				return false
			}
		}
	}
	s.wg.Add(1)
	go s.runJob(job)
	return true
}

func (s *Scheduler) runJob(job *table.Job) {
	atomic.AddInt32(&s.runningJobs, 1)
	if err := s.execute(job); err != nil {
		log.Printf("error on execution of %s: %s", job.String(), err)
	}
	if s.maxRunning != nil {
		<-s.maxRunning
	}
	atomic.AddInt32(&s.runningJobs, -1)
	s.wg.Done()
}

// execute makes a POST request to the endpoint and updates the Job's next run time.
func (s *Scheduler) execute(j *table.Job) error {
	log.Debugln("executing:", j.String())

	// Stopping the instance should not cancel running execution.
	// We rely on http.Client timeout and MySQL driver timeouts here.
	ctx := context.Background()

	code, err := s.postJob(ctx, j)
	if err != nil {
		log.Printf("error while doing http post for %s: %s", j.String(), err)
		return s.table.UpdateNextRun(ctx, j.Key, 0.0, s.retryParams)
	}
	if !s.skipOneOffDelete && j.OneOff() {
		log.Debugln("deleting one-off job")
		return s.table.DeleteJob(ctx, j.Key)
	}
	if code == 204 {
		log.Debugln("deleting not found job")
		return s.table.DeleteJob(ctx, j.Key)
	}
	return s.table.UpdateNextRun(ctx, j.Key, s.randomizationFactor, nil)
}

func (s *Scheduler) postJob(ctx context.Context, j *table.Job) (code int, err error) {
	if s.skipPost {
		return 200, nil
	}
	url := s.baseURL + j.Path
	log.Debugln("doing http post to", url)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(j.Body))
	if err != nil {
		return
	}
	req.Header.Set("content-type", "text/plain")
	req.Header.Set("dalga-sched", j.NextSched.Format(time.RFC3339))
	req.Header.Set("dalga-instance", fmt.Sprintf("%d", s.instanceID))
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
