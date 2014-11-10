package dalga

// TODO refactor publisher wait/notify
// TODO update readme

import (
	"database/sql"
	"flag"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/cenkalti/dalga/vendor/github.com/go-sql-driver/mysql"
	"github.com/cenkalti/dalga/vendor/github.com/streadway/amqp"
)

var debugging = flag.Bool("debug", false, "turn on debug messages")

func debug(args ...interface{}) {
	if *debugging {
		log.Println(args...)
	}
}

type Dalga struct {
	Config            Config
	table             *table
	channel           *amqp.Channel
	listener          net.Listener
	newJobs           chan *Job
	canceledJobs      chan *Job
	quitPublisher     chan bool
	publisherFinished chan bool
}

func New(config Config) *Dalga {
	return &Dalga{
		Config:            config,
		newJobs:           make(chan *Job),
		canceledJobs:      make(chan *Job),
		quitPublisher:     make(chan bool),
		publisherFinished: make(chan bool),
	}
}

// Start starts the publisher and http server goroutines.
func (d *Dalga) Start() error {
	if err := d.connectDB(); err != nil {
		return err
	}

	if err := d.connectMQ(); err != nil {
		return err
	}

	server, err := d.makeServer()
	if err != nil {
		return err
	}

	go d.publisher()
	go server()

	return nil
}

// Run starts the dalga and waits until Shutdown() is called.
func (d *Dalga) Run() error {
	err := d.Start()
	if err != nil {
		return err
	}

	debug("Waiting a message from publisherFinished channel")
	<-d.publisherFinished
	debug("Received message from publisherFinished channel")

	return nil
}

func (d *Dalga) Shutdown() error {
	// TODO stop goroutines
	return d.listener.Close()
}

func (d *Dalga) connectDB() error {
	db, err := sql.Open("mysql", d.Config.MySQL.DSN())
	if err != nil {
		return err
	}
	if err = db.Ping(); err != nil {
		return err
	}
	log.Println("Connected to MySQL")
	d.table = &table{db, d.Config.MySQL.Table}
	return nil
}

func (d *Dalga) connectMQ() error {
	conn, err := amqp.Dial(d.Config.RabbitMQ.URL())
	if err != nil {
		return err
	}
	log.Println("Connected to RabbitMQ")

	// Exit program when AMQP connection is closed.
	connClosed := make(chan *amqp.Error)
	conn.NotifyClose(connClosed)
	go func() {
		if err, ok := <-connClosed; ok {
			log.Fatal(err)
		}
	}()

	d.channel, err = conn.Channel()
	if err != nil {
		return err
	}

	// Log undelivered messages.
	returns := make(chan amqp.Return)
	d.channel.NotifyReturn(returns)
	go func() {
		for r := range returns {
			log.Printf("%d: %s exchange=%q routing-key=%q", r.ReplyCode, r.ReplyText, r.Exchange, r.RoutingKey)
		}
	}()

	return nil
}

func (d *Dalga) CreateTable() error {
	db, err := sql.Open("mysql", d.Config.MySQL.DSN())
	if err != nil {
		return err
	}
	defer db.Close()
	t := &table{db, d.Config.MySQL.Table}
	return t.Create()
}

func (d *Dalga) Schedule(id, routingKey string, interval uint32) error {
	job := NewJob(id, routingKey, interval)

	if err := d.table.Insert(job); err != nil {
		return err
	}

	// Wake up the publisher.
	//
	// publisher() may be sleeping for the next job on the queue
	// at the time we schedule a new Job. Let it wake up so it can
	// re-fetch the new Job from the front of the queue.
	//
	// The code below is an idiom for non-blocking send to a channel.
	select {
	case d.newJobs <- job:
		debug("Sent new job signal")
	default:
		debug("Did not send new job signal")
	}

	debug("Job is scheduled:", job)
	return nil
}

func (d *Dalga) Cancel(id, routingKey string) error {
	if err := d.table.Delete(id, routingKey); err != nil {
		return err
	}

	job := Job{primaryKey: primaryKey{id, routingKey}}

	select {
	case d.canceledJobs <- &job:
		debug("Sent cancel signal")
	default:
		debug("Did not send cancel signal")
	}

	debug("Job is cancelled:", job)
	return nil
}

// publish sends a message to exchange defined in the config and
// updates the Job's next run time on the database.
func (d *Dalga) publish(j *Job) error {
	debug("publish", *j)

	// Send a message to RabbitMQ
	err := d.channel.Publish(d.Config.RabbitMQ.Exchange, j.RoutingKey, true, false, amqp.Publishing{
		Body:         []byte(j.ID),
		DeliveryMode: amqp.Persistent,
		Expiration:   strconv.FormatFloat(j.Interval.Seconds(), 'f', 0, 64) + "000",
	})
	if err != nil {
		return err
	}

	return d.table.UpdateNextRun(j)
}

// publisher runs a loop that reads the next Job from the queue and publishes it.
func (d *Dalga) publisher() {
	publish := func(j *Job) {
		err := d.publish(j)
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
		}
	}

	for {
		debug("---")

		select {
		case <-d.quitPublisher:
			debug("Came message from channel 1: quitPublisher")
			goto end
		default:
		}

		job, err := d.table.Front()
		if err != nil {
			if err != sql.ErrNoRows {
				log.Println(err)
				time.Sleep(time.Second)
				continue
			} else if myErr, ok := err.(*mysql.MySQLError); ok && myErr.Number == 1146 { // Table doesn't exist
				log.Fatal(myErr)
			}

			debug("No waiting jobs in the queue")
			debug("Waiting for new job signal")
			select {
			case job = <-d.newJobs:
				debug("Got new job signal")
			case <-d.quitPublisher:
				debug("Came message from channel 2: quitPublisher")
				goto end
			}
		}

	checkNextRun:
		remaining := job.Remaining()
		debug("Next job:", job, "Remaining:", remaining)

		now := time.Now().UTC()
		if !job.NextRun.After(now) {
			publish(job)
			continue
		}

		// Wait until the next Job time or
		// the webserver's /schedule handler wakes us up
		debug("Sleeping for job:", remaining)
		select {
		case <-time.After(remaining):
			debug("Job sleep time finished")
			publish(job)
		case newJob := <-d.newJobs:
			debug("A new job has been scheduled")
			if newJob.NextRun.Before(job.NextRun) {
				debug("The new job comes before out current job")
				job = newJob // Process the new job next
			}
			// Continue processing the current job without fetching from database
			goto checkNextRun
		case canceledJob := <-d.canceledJobs:
			debug("A job has been cancelled")
			if job.Equal(canceledJob) {
				// The job we are waiting for has been canceled.
				// We need to fetch the next job in the queue.
				debug("The cancelled job is our current job")
				continue
			}
			// Continue to process our current job
			goto checkNextRun
		case <-d.quitPublisher:
			debug("Came message from channel 3: quitPublisher")
			goto end
		}
	}
end:
	d.publisherFinished <- true
}
