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
	config     Config
	db         *sql.DB
	table      *table
	listener   net.Listener
	client     http.Client
	activeJobs map[string]struct{}
	m          sync.Mutex
	wg         sync.WaitGroup
	// to wake up publisher when a new job is scheduled or cancelled
	notify chan struct{}
	// will be closed when dalga is ready to accept requests
	ready chan struct{}
	// will be closed by Shutdown method
	shutdown chan struct{}
	// to stop publisher goroutine
	stopPublisher chan struct{}
	// will be closed when publisher goroutine is stopped
	publisherStopped chan struct{}
}

func New(config Config) *Dalga {
	d := &Dalga{
		config:           config,
		activeJobs:       make(map[string]struct{}),
		notify:           make(chan struct{}, 1),
		ready:            make(chan struct{}),
		shutdown:         make(chan struct{}),
		stopPublisher:    make(chan struct{}),
		publisherStopped: make(chan struct{}),
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

	go d.publisher()
	defer func() {
		close(d.stopPublisher)
		<-d.publisherStopped
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

// GetJob returns the job with description routing key.
func (d *Dalga) GetJob(description, routingKey string) (*Job, error) {
	return d.table.Get(description, routingKey)
}

// ScheduleJob inserts a new job to the table or replaces existing one.
// Returns the created or replaced job.
func (d *Dalga) ScheduleJob(description, routingKey string, interval uint32, oneOff bool) (*Job, error) {
	job := newJob(description, routingKey, interval, oneOff)
	err := d.table.Insert(job)
	if err != nil {
		return nil, err
	}
	d.notifyPublisher("new job")
	debug("Job is scheduled:", job)
	return job, nil
}

// TriggerJob publishes the job to RabbitMQ immediately and resets the next run time of the job.
func (d *Dalga) TriggerJob(description, routingKey string) (*Job, error) {
	job, err := d.GetJob(description, routingKey)
	if err != nil {
		return nil, err
	}
	job.NextRun = time.Now().UTC()
	if err := d.table.Insert(job); err != nil {
		return nil, err
	}
	d.notifyPublisher("job is triggered")
	debug("Job is triggered:", job)
	return job, nil
}

// CancelJob deletes the job with description and routing key.
func (d *Dalga) CancelJob(description, routingKey string) error {
	err := d.table.Delete(description, routingKey)
	if err != nil {
		return err
	}
	d.notifyPublisher("job cancelled")
	debug("Job is cancelled")
	return nil
}

func (d *Dalga) notifyPublisher(debugMessage string) {
	select {
	case d.notify <- struct{}{}:
		debug("notifying publisher:", debugMessage)
	default:
	}
}

// publisher runs a loop that reads the next Job from the queue and publishes it.
func (d *Dalga) publisher() {
	defer close(d.publisherStopped)

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
			if err = d.publish(job); err != nil {
				log.Print(err)
				time.Sleep(time.Second)
			}
		case <-d.notify:
			debug("Woken up from sleep by notification")
			continue
		case <-d.stopPublisher:
			debug("Came quit message")
			d.wg.Wait()
			return
		}
	}
}

// publish makes a POST request to the endpoint and updates the Job's next run time.
func (d *Dalga) publish(j *Job) error {
	debug("publish", *j)

	var add time.Duration
	if j.Interval == 0 {
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
		// Do not do multiple POSTs for the same job at the same time.
		key := j.Path + j.Body
		d.m.Lock()
		if _, ok := d.activeJobs[key]; ok {
			d.m.Unlock()
			return
		}
		d.activeJobs[key] = struct{}{}
		d.m.Unlock()

		d.retryPostJob(j)
		if j.Interval == 0 {
			debug("deleting one-off job")
			d.retryDeleteJob(j)
		}

		d.m.Lock()
		delete(d.activeJobs, key)
		d.m.Unlock()

		d.wg.Done()
	}()

	return nil
}

func (d *Dalga) postJob(j *Job) error {
	var resp *http.Response
	var err error
	resp, err = d.client.Post(d.config.Endpoint.BaseURL+j.Path, "text/plain", strings.NewReader(j.Body))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("endpoint error: %d", resp.StatusCode)
	}
	return nil
}

func (d *Dalga) retryPostJob(j *Job) {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0 // retry forever
	if j.Interval > 0 {
		b.MaxInterval = j.Interval
	}
	f := func() error { return d.postJob(j) }
	retry(b, f, d.stopPublisher)
}

func (d *Dalga) retryDeleteJob(j *Job) {
	b := backoff.NewConstantBackOff(time.Second)
	f := func() error { return d.table.Delete(j.Path, j.Body) }
	retry(b, f, nil)
}

func retry(b backoff.BackOff, f func() error, stop chan struct{}) {
	ticker := backoff.NewTicker(b)
	for {
		select {
		case <-ticker.C:
			if err := f(); err != nil {
				log.Print(err)
				continue
			}
			return
		case <-stop:
			return
		}
	}
}
