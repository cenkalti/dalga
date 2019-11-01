package dalga

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/go-sql-driver/mysql"
)

type scheduler struct {
	table               *table
	client              http.Client
	baseURL             string
	randomizationFactor float64
	// to stop scheduler goroutine
	stop chan struct{}
	// will be closed when scheduler goroutine is stopped
	stopped chan struct{}
	// to wake up scheduler when a new job is scheduled or cancelled
	wakeUp      chan struct{}
	runningJobs map[JobKey]struct{}
	m           sync.Mutex
	wg          sync.WaitGroup
}

func newScheduler(t *table, baseURL string, clientTimeout time.Duration, randomizationFactor float64) *scheduler {
	s := &scheduler{
		table:               t,
		baseURL:             baseURL,
		randomizationFactor: randomizationFactor,
		stop:                make(chan struct{}),
		stopped:             make(chan struct{}),
		wakeUp:              make(chan struct{}, 1),
		runningJobs:         make(map[JobKey]struct{}),
	}
	s.client.Timeout = clientTimeout
	return s
}

func (s *scheduler) WakeUp(debugMessage string) {
	select {
	case s.wakeUp <- struct{}{}:
		debug("notifying scheduler:", debugMessage)
	default:
	}
}

func (s *scheduler) NotifyDone() <-chan struct{} {
	return s.stopped
}

func (s *scheduler) Stop() {
	close(s.stop)
}

func (s *scheduler) Running() int {
	s.m.Lock()
	defer s.m.Unlock()
	return len(s.runningJobs)
}

// Run runs a loop that reads the next Job from the queue and executees it in it's own goroutine.
func (s *scheduler) Run() {
	defer close(s.stopped)

	for {
		debug("---")

		var after <-chan time.Time

		job, err := s.table.Front()
		if err != nil {
			if err == sql.ErrNoRows {
				debug("no scheduled jobs in the table")
			} else if myErr, ok := err.(*mysql.MySQLError); ok && myErr.Number == 1146 {
				// Table doesn't exist
				log.Fatal(myErr)
			} else {
				log.Print(err)
				time.Sleep(time.Second)
				continue
			}
		} else {
			remaining := job.Remaining()
			after = time.After(remaining)
			debug("next job:", job, "remaining:", remaining)
		}

		// Sleep until the next job's run time or the webserver's wakes us up.
		select {
		case <-after:
			debug("job sleep time finished")
			if err = s.execute(job); err != nil {
				log.Print(err)
				time.Sleep(time.Second)
			}
		case <-s.wakeUp:
			debug("woken up from sleep by notification")
			continue
		case <-s.stop:
			debug("came quit message")
			s.wg.Wait()
			return
		}
	}
}

func randomize(d time.Duration, f float64) time.Duration {
	delta := time.Duration(f * float64(d))
	return d - delta + time.Duration(float64(2*delta)*rand.Float64())
}

// execute makes a POST request to the endpoint and updates the Job's next run time.
func (s *scheduler) execute(j *Job) error {
	debug("execute", *j)

	var add time.Duration
	if j.OneOff() {
		add = s.client.Timeout
	} else {
		add = j.Interval
		if s.randomizationFactor > 0 {
			// Add some randomization to periodic tasks.
			add = randomize(add, s.randomizationFactor)
		}
	}

	j.NextRun = time.Now().UTC().Add(add)

	if err := s.table.UpdateNextRun(j); err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Do not do multiple POSTs for the same job at the same time.
		s.m.Lock()
		if _, ok := s.runningJobs[j.JobKey]; ok {
			debug("job is already running", j.JobKey)
			s.m.Unlock()
			return
		}
		s.runningJobs[j.JobKey] = struct{}{}
		s.m.Unlock()

		defer func() {
			s.m.Lock()
			delete(s.runningJobs, j.JobKey)
			s.m.Unlock()
		}()

		code := s.retryPostJob(j)
		if code == 0 {
			return
		}

		if j.OneOff() {
			debug("deleting one-off job")
			s.retryDeleteJob(j)
			s.WakeUp("deleted one-off job")
			return
		}

		if code == 204 {
			debug("deleting not found job")
			if err := s.deleteJob(j); err != nil {
				log.Print(err)
				return
			}
			s.WakeUp("deleted not found job")
			return
		}
	}()

	return nil
}

func (s *scheduler) postJob(j *Job) (code int, err error) {
	url := s.baseURL + j.Path
	debug("POSTing to ", url)
	resp, err := s.client.Post(url, "text/plain", strings.NewReader(j.Body))
	if err != nil {
		return
	}
	switch resp.StatusCode {
	case 200, 204:
		code = resp.StatusCode
	default:
		err = fmt.Errorf("endpoint error: %d", resp.StatusCode)
	}
	return
}

func (s *scheduler) retryPostJob(j *Job) (code int) {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0 // retry forever
	if j.Interval > 0 {
		b.MaxInterval = j.Interval
	}
	f := func() (interface{}, error) { return s.postJob(j) }
	code, _ = retry(b, f, s.stop).(int)
	return
}

func (s *scheduler) retryDeleteJob(j *Job) {
	b := backoff.NewConstantBackOff(time.Second)
	f := func() (interface{}, error) { return nil, s.deleteJob(j) }
	retry(b, f, nil)
}

func (s *scheduler) deleteJob(j *Job) error {
	err := s.table.Delete(j.Path, j.Body)
	if err == ErrNotExist {
		return nil
	}
	return err
}

func retry(b backoff.BackOff, f func() (result interface{}, err error), stop chan struct{}) (result interface{}) {
	ticker := backoff.NewTicker(b)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var err error
			if result, err = f(); err != nil {
				log.Print(err)
				continue
			}
			return
		case <-stop:
			return
		}
	}
}
