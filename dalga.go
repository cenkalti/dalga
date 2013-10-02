package main

import (
	"code.google.com/p/gcfg"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	debugging = flag.Bool("d", false, "turn on debug messages")
	cfg       struct {
		DB struct {
			Driver string
			Dsn    string
			Table  string
		}
		RabbitMQ struct {
			Uri      string
			Exchange string
		}
		HTTP struct {
			Host string
			Port string
		}
	}

	db      *sql.DB
	broker  *amqp.Connection
	channel *amqp.Channel
	wakeUp  = make(chan int, 1)
)

type Job struct {
	routingKey string
	body       string
	interval   time.Duration
	nextRun    time.Time
	state      string
}

func debug(args ...interface{}) {
	if *debugging {
		log.Println(args...)
	}
}

// hadleSchedule is the web server endpoint for path: /schedule
func handleSchedule(w http.ResponseWriter, r *http.Request) {
	routingKey, body, interval_s :=
		r.FormValue("routing_key"), r.FormValue("body"), r.FormValue("interval")
	debug("/schedule", routingKey, body)

	interval, err := strconv.ParseInt(interval_s, 10, 64)
	if err != nil {
		panic(err)
	}

	next_run := time.Now().UTC().Add(time.Duration(interval) * time.Second)
	_, err = db.Exec("INSERT INTO "+cfg.DB.Table+" "+
		"(routing_key, body, `interval`, next_run, state) "+
		"VALUES(?, ?, ?, ?, 'WAITING') "+
		"ON DUPLICATE KEY UPDATE `interval`=?",
		routingKey, body, interval, next_run, interval)
	if err != nil {
		panic(err)
	}

	// Wake up the publisher.
	//
	// publisher() may be sleeping for the next job on the queue
	// at the time we schedule a new Job. Let it wake up so it can
	// re-fetch the new Job from the front of the queue.
	//
	// The code below is an idiom for non-blocking send to a channel.
	select {
	case wakeUp <- 1:
		debug("Sent wakeup signal")
	default:
		debug("Skipped wakeup signal")
	}
}

// handleCancel is the web server endpoint for path: /cancel
func handleCancel(w http.ResponseWriter, r *http.Request) {
	routingKey, body := r.FormValue("routing_key"), r.FormValue("body")
	debug("/cancel", routingKey, body)

	_, err := db.Exec("DELETE FROM "+cfg.DB.Table+" "+
		"WHERE routing_key=? AND body=?", routingKey, body)
	if err != nil {
		panic(err)
	}
}

// front returns the first job to be run in the queue.
func front() (*Job, error) {
	var interval uint
	j := Job{}
	row := db.QueryRow("SELECT routing_key, body, `interval`, next_run, state " +
		"FROM " + cfg.DB.Table + " " +
		"WHERE state='WAITING' " +
		"ORDER BY next_run ASC")
	err := row.Scan(&j.routingKey, &j.body, &interval, &j.nextRun, &j.state)
	if err != nil {
		return nil, err
	}
	j.interval = time.Duration(interval) * time.Second
	return &j, nil
}

// Publish sends a message to exchange defined in the config and
// updates the Job's state to RUNNING on the database.
func (j *Job) Publish() error {
	debug("publish", *j)

	// Send a message to the broker
	err := channel.Publish(cfg.RabbitMQ.Exchange, j.routingKey, false, false, amqp.Publishing{
		Headers: amqp.Table{
			"interval":     j.interval,
			"published_at": time.Now().UTC(),
		},
		ContentType:     "application/octet-stream",
		ContentEncoding: "",
		Body:            []byte(j.body),
		DeliveryMode:    amqp.Persistent,
		Priority:        0,
	})
	if err != nil {
		return err
	}

	// Update state from database
	_, err = db.Exec("UPDATE "+cfg.DB.Table+" "+
		"SET state='RUNNING', next_run=? "+
		"WHERE routing_key=? AND body=?",
		j.CalculateNextRun(), j.routingKey, j.body)
	if err != nil {
		return err
	}
	return nil
}

// Remaining returns the duration until the job's next scheduled time.
func (j *Job) Remaining() time.Duration {
	return -time.Since(j.nextRun)
}

func (j *Job) CalculateNextRun() time.Time {
	return j.nextRun.Add(j.interval)
}

// publisher runs a loop that reads the next Job from the queue and publishes it.
func publisher() {
	publish := func(j *Job) {
		err := j.Publish()
		if err != nil {
			fmt.Println(err)
		}
	}

	for {
		job, err := front()
		if err != nil {
			if strings.Contains(err.Error(), "no rows in result set") {
				debug("No waiting jobs the queue")
				debug("Waiting wakeup signal")
				<-wakeUp
				debug("Got wakeup signal")
			} else {
				fmt.Println(err)
			}
			continue
		}
		remaining := job.Remaining()
		debug("Next job:", job, "Remaining:", remaining)

		now := time.Now().UTC()
		if job.nextRun.After(now) {
			// Wait until the next Job time or
			// the webserver's /schedule handler wakes us up
			debug("Sleeping for job:", remaining)
			select {
			case <-time.After(remaining):
				debug("Job sleep time finished")
				publish(job)
			case <-wakeUp:
				debug("Woke up by webserver")
				continue
			}
		} else {
			publish(job)
		}

	}
}

func main() {
	flag.Parse()

	// Read config
	err := gcfg.ReadFileInto(&cfg, "dalga.ini")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Read config: ", cfg)

	// Connect to database
	db, err = sql.Open(cfg.DB.Driver, cfg.DB.Dsn)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to DB")

	// Connect to RabbitMQ
	broker, err = amqp.Dial(cfg.RabbitMQ.Uri)
	if err != nil {
		log.Fatal(err)
	}
	channel, err = broker.Channel()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to RabbitMQ")

	// Run publisher
	go publisher()

	// Start HTTP server
	addr := cfg.HTTP.Host + ":" + cfg.HTTP.Port
	http.HandleFunc("/schedule", handleSchedule)
	http.HandleFunc("/cancel", handleCancel)
	http.ListenAndServe(addr, nil)
}
