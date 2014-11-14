package dalga

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/dalga/vendor/github.com/cenkalti/backoff"
	"github.com/cenkalti/dalga/vendor/github.com/go-sql-driver/mysql"
)

var debugging = flag.Bool("debug", false, "turn on debug messages")

func debug(args ...interface{}) {
	if *debugging {
		log.Println(args...)
	}
}

type Dalga struct {
	config      Config
	db          *sql.DB
	table       *table
	listener    net.Listener
	client      http.Client
	runningJobs map[JobKey]struct{}
	m           sync.Mutex
	wg          sync.WaitGroup
	// to wake up scheduler when a new job is scheduled or cancelled
	notify chan struct{}
	// will be closed when dalga is ready to accept requests
	ready chan struct{}
	// will be closed by Shutdown method
	shutdown chan struct{}
	// to stop scheduler goroutine
	stopScheduler chan struct{}
	// will be closed when scheduler goroutine is stopped
	schedulerStopped chan struct{}
}

func New(config Config) *Dalga {
	d := &Dalga{
		config:           config,
		runningJobs:      make(map[JobKey]struct{}),
		notify:           make(chan struct{}, 1),
		ready:            make(chan struct{}),
		shutdown:         make(chan struct{}),
		stopScheduler:    make(chan struct{}),
		schedulerStopped: make(chan struct{}),
	}
	d.client.Timeout = time.Duration(config.Endpoint.Timeout) * time.Second
	return d
}

// Run Dalga. This function is blocking. Returns nil if Shutdown is called.
func (d *Dalga) Run() error {
	if err := d.connectDB(); err != nil {
		return err
	}
	defer d.db.Close()

	var err error
	d.listener, err = net.Listen("tcp", d.config.Listen.Addr())
	if err != nil {
		return err
	}
	log.Println("Listening", d.listener.Addr())

	close(d.ready)

	go d.scheduler()
	defer func() {
		close(d.stopScheduler)
		<-d.schedulerStopped
	}()

	if err = d.serveHTTP(); err != nil {
		select {
		case _, ok := <-d.shutdown:
			if !ok {
				// shutdown in progress, do not return error
				return nil
			}
		default:
		}
	}
	return err
}

// Shutdown running Dalga.
func (d *Dalga) Shutdown() error {
	close(d.shutdown)
	return d.listener.Close()
}

// NotifyReady returns a channel that will be closed when Dalga is running.
func (d *Dalga) NotifyReady() <-chan struct{} {
	return d.ready
}

func (d *Dalga) connectDB() error {
	var err error
	d.db, err = sql.Open("mysql", d.config.MySQL.DSN())
	if err != nil {
		return err
	}
	if err = d.db.Ping(); err != nil {
		return err
	}
	log.Print("Connected to MySQL")
	d.table = &table{d.db, d.config.MySQL.Table}
	return nil
}

// CreateTable creates the table for storing jobs.
func (d *Dalga) CreateTable() error {
	db, err := sql.Open("mysql", d.config.MySQL.DSN())
	if err != nil {
		return err
	}
	defer db.Close()
	t := &table{db, d.config.MySQL.Table}
	return t.Create()
}

// GetJob returns the job with path and body.
func (d *Dalga) GetJob(path, body string) (*Job, error) {
	return d.table.Get(path, body)
}

// ScheduleJob inserts a new job to the table or replaces existing one.
// Returns the created or replaced job.
func (d *Dalga) ScheduleJob(path, body string, interval uint32, oneOff bool) (*Job, error) {
	job := newJob(path, body, time.Duration(interval)*time.Second, oneOff)
	err := d.table.Insert(job)
	if err != nil {
		return nil, err
	}
	d.notifyScheduler("new job")
	debug("Job is scheduled:", job)
	return job, nil
}

// TriggerJob sends the job to the HTTP endpoint immediately and resets the next run time of the job.
func (d *Dalga) TriggerJob(path, body string) (*Job, error) {
	job, err := d.GetJob(path, body)
	if err != nil {
		return nil, err
	}
	job.NextRun = time.Now().UTC()
	if err := d.table.Insert(job); err != nil {
		return nil, err
	}
	d.notifyScheduler("job is triggered")
	debug("Job is triggered:", job)
	return job, nil
}

// CancelJob deletes the job with path and body.
func (d *Dalga) CancelJob(path, body string) error {
	err := d.table.Delete(path, body)
	if err != nil {
		return err
	}
	d.notifyScheduler("job cancelled")
	debug("Job is cancelled")
	return nil
}

func (d *Dalga) notifyScheduler(debugMessage string) {
	select {
	case d.notify <- struct{}{}:
		debug("notifying scheduler:", debugMessage)
	default:
	}
}

// scheduler runs a loop that reads the next Job from the queue and executees it.
func (d *Dalga) scheduler() {
	defer close(d.schedulerStopped)

	for {
		debug("---")

		var after <-chan time.Time

		job, err := d.table.Front()
		if err != nil {
			if err == sql.ErrNoRows {
				debug("No scheduled jobs in the table")
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
			debug("Next job:", job, "Remaining:", remaining)
		}

		// Sleep until the next job's run time or the webserver's wakes us up.
		select {
		case <-after:
			debug("Job sleep time finished")
			if err = d.execute(job); err != nil {
				log.Print(err)
				time.Sleep(time.Second)
			}
		case <-d.notify:
			debug("Woken up from sleep by notification")
			continue
		case <-d.stopScheduler:
			debug("Came quit message")
			d.wg.Wait()
			return
		}
	}
}

// execute makes a POST request to the endpoint and updates the Job's next run time.
func (d *Dalga) execute(j *Job) error {
	debug("execute", *j)

	var add time.Duration
	if j.OneOff() {
		add = d.client.Timeout
	} else {
		add = j.Interval
	}

	j.NextRun = time.Now().UTC().Add(add)

	if err := d.table.UpdateNextRun(j); err != nil {
		return err
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		// Do not do multiple POSTs for the same job at the same time.
		d.m.Lock()
		if _, ok := d.runningJobs[j.JobKey]; ok {
			debug("job is already running", j.JobKey)
			d.m.Unlock()
			return
		}
		d.runningJobs[j.JobKey] = struct{}{}
		d.m.Unlock()

		defer func() {
			d.m.Lock()
			delete(d.runningJobs, j.JobKey)
			d.m.Unlock()
		}()

		code := d.retryPostJob(j)
		if code == 0 {
			return
		}

		if j.OneOff() {
			debug("deleting one-off job")
			d.retryDeleteJob(j)
			d.notifyScheduler("deleted one-off job")
			return
		}

		if code == 204 {
			debug("deleting not found job")
			if err := d.deleteJob(j); err != nil {
				log.Print(err)
				return
			}
			d.notifyScheduler("deleted not found job")
			return
		}
	}()

	return nil
}

func (d *Dalga) postJob(j *Job) (code int, err error) {
	url := d.config.Endpoint.BaseURL + j.Path
	debug("POSTing to ", url)
	resp, err := d.client.Post(url, "text/plain", strings.NewReader(j.Body))
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

func (d *Dalga) retryPostJob(j *Job) interface{} {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0 // retry forever
	if j.Interval > 0 {
		b.MaxInterval = j.Interval
	}
	f := func() (interface{}, error) { return d.postJob(j) }
	return retry(b, f, d.stopScheduler)
}

func (d *Dalga) retryDeleteJob(j *Job) {
	b := backoff.NewConstantBackOff(time.Second)
	f := func() (interface{}, error) { return nil, d.deleteJob(j) }
	retry(b, f, nil)
}

func (d *Dalga) deleteJob(j *Job) error {
	err := d.table.Delete(j.Path, j.Body)
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
