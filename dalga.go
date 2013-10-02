package main

import (
	"code.google.com/p/gcfg"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"strconv"
	"time"
)

var (
	cfg struct {
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

	db     *sql.DB
	broker *amqp.Connection
	wakeUp = make(chan int, 1)
)

type Job struct {
	routingKey string
	body       string
	interval   uint
	nextRun    time.Time
	state      string
}

func handleSchedule(w http.ResponseWriter, r *http.Request) {
	routingKey, body, interval_s :=
		r.FormValue("routing_key"), r.FormValue("body"), r.FormValue("interval")
	log.Println("/schedule", routingKey, body)

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

	// Wake up the publisher
	//
	// publisher() may be sleeping for the next job on the queue
	// at the time we schedule a new Job. Let it wake up so it can
	// re-fetch the new Job from the front of the queue.
	//
	// The code below is an idiom for non-blocking send to a channel
	select {
	case wakeUp <- 1:
		fmt.Println("Sent wakeup signal")
	default:
		fmt.Println("Skipped wakeup signal")
	}
}

func handleCancel(w http.ResponseWriter, r *http.Request) {
	routingKey, body := r.FormValue("routing_key"), r.FormValue("body")
	fmt.Println("/cancel", routingKey, body)

	_, err := db.Exec("DELETE FROM "+cfg.DB.Table+" "+
		"WHERE routing_key=? AND body=?", routingKey, body)
	if err != nil {
		panic(err)
	}
}

func front() (*Job, error) {
	j := Job{}
	row := db.QueryRow("SELECT routing_key, body, `interval`, next_run, state " +
		"FROM " + cfg.DB.Table + " " +
		"WHERE next_run = (" +
		"SELECT MIN(next_run) FROM " + cfg.DB.Table + ")")
	err := row.Scan(&j.routingKey, &j.body, &j.interval, &j.nextRun, &j.state)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (j *Job) Delete() error {
	_, err := db.Exec("DELETE FROM "+cfg.DB.Table+" "+
		"WHERE routing_key=? AND body=?",
		j.routingKey, j.body)
	if err != nil {
		return err
	}
	return nil
}

func (j *Job) Publish() error {
	fmt.Println("publish", *j)
	return nil
}

func (j *Job) Remaining() time.Duration {
	return -time.Since(j.nextRun)
}

func publisher() {
	for {
		job, err := front()
		if err != nil {
			fmt.Println(err)
			fmt.Println("Waiting wakeup signal")
			<-wakeUp
			fmt.Println("Got wakeup signal")
			continue
		}
		log.Println("Next job:", job, "Remaining:", job.Remaining())

		now := time.Now().UTC()
		if job.nextRun.After(now) {
			select {
			case <-time.After(job.Remaining()):
			case <-wakeUp:
				continue
			}
		} else {
			job.Publish()
			err = job.Delete()
			if err != nil {
				log.Println(err)
			}
		}

	}
}

func main() {
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
	_, err = amqp.Dial(cfg.RabbitMQ.Uri)
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
