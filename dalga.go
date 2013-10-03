package dalga

// TODO list
// write basic integration tests
// handle mysql disconnect
// handle rabbitmq disconnect

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

const createTableSQL = "" +
	"CREATE TABLE `%s` (" +
	"  `routing_key` VARCHAR(255)    NOT NULL," +
	"  `body`        BLOB(767)       NOT NULL," +
	"  `interval`    INT UNSIGNED    NOT NULL," +
	"  `next_run`    DATETIME        NOT NULL," +
	"" +
	"  PRIMARY KEY (`routing_key`, `body`(767))," +
	"  KEY `idx_next_run` (`next_run`)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8"

var debugging = flag.Bool("d", false, "turn on debug messages")

func debug(args ...interface{}) {
	if *debugging {
		log.Println(args...)
	}
}

type Dalga struct {
	C            *Config
	db           *sql.DB
	rabbit       *amqp.Connection
	channel      *amqp.Channel
	listener     net.Listener
	newJobs      chan *Job
	canceledJobs chan *Job
	quit         chan bool
}

func NewDalga(config *Config) *Dalga {
	return &Dalga{
		C:            config,
		newJobs:      make(chan *Job),
		canceledJobs: make(chan *Job),
		quit:         make(chan bool, 1),
	}
}

// Start starts the publisher and http server goroutines.
func (d *Dalga) Start() (chan bool, error) {
	err := d.connectDB()
	if err != nil {
		return nil, err
	}

	err = d.connectMQ()
	if err != nil {
		return nil, err
	}

	server, err := d.makeServer()
	if err != nil {
		return nil, err
	}

	go d.publisher()
	go server()

	ch := make(chan bool, 1)
	ch <- true

	return ch, nil
}

// Run starts the dalga and waits until Shutdown() is called.
func (d *Dalga) Run() error {
	_, err := d.Start()
	if err != nil {
		return err
	}

	debug("Waiting a message from quit channel")
	<-d.quit
	debug("Got quit message")
	return nil
}

func (d *Dalga) Shutdown() error {
	return d.listener.Close()
}

func (d *Dalga) connectDB() error {
	var err error
	d.db, err = d.newMySQLConnection()
	return err
}

func (d *Dalga) newMySQLConnection() (*sql.DB, error) {
	my := d.C.MySQL
	dsn := my.User + ":" + my.Password + "@" + "tcp(" + my.Host + ":" + my.Port + ")/" + my.Db + "?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	fmt.Println("Connected to MySQL")
	return db, nil
}

func (d *Dalga) connectMQ() error {
	var err error
	rabbit := d.C.RabbitMQ
	uri := "amqp://" + rabbit.User + ":" + rabbit.Password + "@" + rabbit.Host + ":" + rabbit.Port + rabbit.VHost
	d.rabbit, err = amqp.Dial(uri)
	if err != nil {
		return err
	}
	d.channel, err = d.rabbit.Channel()
	fmt.Println("Connected to RabbitMQ")
	return err
}

func (d *Dalga) CreateTable() error {
	db, err := d.newMySQLConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	sql := fmt.Sprintf(createTableSQL, d.C.MySQL.Table)
	_, err = db.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (d *Dalga) Schedule(routingKey string, body []byte, interval uint32) error {
	job := NewJob(routingKey, body, interval)

	err := d.enter(job)
	if err != nil {
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

	return nil
}

func (d *Dalga) Cancel(routingKey string, body []byte) error {
	err := d.cancel(routingKey, body)
	if err != nil {
		return err
	}

	select {
	case d.canceledJobs <- &Job{RoutingKey: routingKey, Body: body}:
		debug("Sent cancel signal")
	default:
		debug("Did not send cancel signal")
	}

	return nil
}

// front returns the first job to be run in the queue.
func (d *Dalga) front() (*Job, error) {
	var interval uint
	j := Job{}
	row := d.db.QueryRow("SELECT routing_key, body, `interval`, next_run " +
		"FROM " + d.C.MySQL.Table + " " +
		"ORDER BY next_run ASC LIMIT 1")
	err := row.Scan(&j.RoutingKey, &j.Body, &interval, &j.NextRun)
	if err != nil {
		return nil, err
	}
	j.Interval = time.Duration(interval) * time.Second
	return &j, nil
}

// publish sends a message to exchange defined in the config and
// updates the Job's next run time on the database.
func (d *Dalga) publish(j *Job) error {
	debug("publish", *j)

	// Update next run time
	_, err := d.db.Exec("UPDATE "+d.C.MySQL.Table+" "+
		"SET next_run=? "+
		"WHERE routing_key=? AND body=?",
		time.Now().UTC().Add(j.Interval), j.RoutingKey, j.Body)
	if err != nil {
		return err
	}

	// Send a message to RabbitMQ
	err = d.channel.Publish(d.C.RabbitMQ.Exchange, j.RoutingKey, false, false, amqp.Publishing{
		Headers: amqp.Table{
			"interval":     j.Interval.Seconds(),
			"published_at": time.Now().UTC().String(),
		},
		ContentType:  "application/octet-stream",
		Body:         j.Body,
		DeliveryMode: amqp.Persistent,
		Priority:     0,
		Expiration:   strconv.FormatUint(uint64(j.Interval.Seconds()), 10) + "000",
	})
	if err != nil {
		return err
	}

	return nil
}

// enter puts the job to the waiting queue.
func (d *Dalga) enter(j *Job) error {
	interval := j.Interval.Seconds()
	_, err := d.db.Exec("INSERT INTO "+d.C.MySQL.Table+" "+
		"(routing_key, body, `interval`, next_run) "+
		"VALUES(?, ?, ?, ?) "+
		"ON DUPLICATE KEY UPDATE "+
		"next_run=DATE_ADD(next_run, INTERVAL (? - `interval`) SECOND), "+
		"`interval`=?",
		j.RoutingKey, j.Body, interval, j.NextRun, interval, interval)
	return err
}

// cancel removes the job from the waiting queue.
func (d *Dalga) cancel(routingKey string, body []byte) error {
	_, err := d.db.Exec("DELETE FROM "+d.C.MySQL.Table+" "+
		"WHERE routing_key=? AND body=?", routingKey, body)
	return err
}

// publisher runs a loop that reads the next Job from the queue and publishes it.
func (d *Dalga) publisher() {
	publish := func(j *Job) {
		err := d.publish(j)
		if err != nil {
			fmt.Println(err)
		}
	}

	for {
		debug("")

		job, err := d.front()
		if err != nil {
			if strings.Contains(err.Error(), "no rows in result set") {
				debug("No waiting jobs in the queue")
				debug("Waiting wakeup signal")
				job = <-d.newJobs
				debug("Got wakeup signal")
			} else {
				fmt.Println(err)
				continue
			}
		}

	CheckNextRun:
		remaining := job.Remaining()
		debug("Next job:", job, "Remaining:", remaining)

		now := time.Now().UTC()
		if job.NextRun.After(now) {
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
				goto CheckNextRun
			case canceledJob := <-d.canceledJobs:
				debug("A job has been cancelled")
				if job.Equals(canceledJob) {
					// The job we are waiting for has been canceled.
					// We need to fetch the next job in the queue.
					debug("The cancelled job is our current job")
					continue
				}
				// Continue to process our current job
				goto CheckNextRun
			}
		} else {
			publish(job)
		}
	}
}
