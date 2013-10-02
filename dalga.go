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
)

type State uint

const (
	WAITING = 0
	RUNNING = 1
)

type Job struct {
	routingKey string
	body       string
	interval   uint
	nextRun    time.Time
	state      State
}

func handleSchedule(w http.ResponseWriter, r *http.Request) {
	routingKey, body, interval_s :=
		r.FormValue("routing_key"), r.FormValue("body"), r.FormValue("interval")
	log.Println("/schedule", routingKey, body)

	interval, err := strconv.ParseInt(interval_s, 10, 64)
	if err != nil {
		panic(err)
	}

	next_run := time.Now().Add(time.Duration(interval) * time.Second)
	_, err = db.Exec("INSERT IGNORE INTO "+cfg.DB.Table+" "+
		"(routing_key, body, `interval`, next_run, state) "+
		"VALUES(?, ?, ?, ?, 'WAITING')",
		routingKey, body, interval, next_run)
	if err != nil {
		panic(err)
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
	row := db.QueryRow("SELECT routing_key, body, interval, next_run, state " +
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
	return nil
}

func (j *Job) Sleep() error {
	return nil
}

func publisher() {
	for {
		job, err := front()
		if err != nil {
			fmt.Println(err)
			continue
		}

		now := time.Now()
		if job.nextRun.After(now) {
			job.Sleep()
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
