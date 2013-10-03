package main

// TODO list
// Optimize wake up
// Idempotent schedule call
// seperate files
// make dalga a type
// make http server closeable
// write basic integration tests
// handle mysql disconnect
// handle rabbitmq disconnect

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
	debugging  = flag.Bool("d", false, "turn on debug messages")
	configPath = flag.String("c", "", "config file path")
	cfg        *Config
	db         *sql.DB
	rabbit     *amqp.Connection
	channel    *amqp.Channel
	wakeUp     = make(chan int, 1)
)

type Config struct {
	MySQL struct {
		User     string
		Password string
		Host     string
		Port     string
		Db       string
		Table    string
	}
	RabbitMQ struct {
		User     string
		Password string
		Host     string
		Port     string
		VHost    string
		Exchange string
	}
	HTTP struct {
		Host string
		Port string
	}
}

// NewConfig returns a pointer to a newly created Config initialized with default parameters.
func NewConfig() *Config {
	c := &Config{}
	c.MySQL.User = "root"
	c.MySQL.Host = "localhost"
	c.MySQL.Port = "3306"
	c.MySQL.Db = "test"
	c.MySQL.Table = "dalga"
	c.RabbitMQ.User = "guest"
	c.RabbitMQ.Password = "guest"
	c.RabbitMQ.Host = "localhost"
	c.RabbitMQ.Port = "5672"
	c.RabbitMQ.VHost = "/"
	c.HTTP.Host = "0.0.0.0"
	c.HTTP.Port = "17500"
	return c
}

type Job struct {
	routingKey string
	body       string
	interval   time.Duration
	nextRun    time.Time
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
	_, err = db.Exec("INSERT INTO "+cfg.MySQL.Table+" "+
		"(routing_key, body, `interval`, next_run) "+
		"VALUES(?, ?, ?, ?) "+
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

	_, err := db.Exec("DELETE FROM "+cfg.MySQL.Table+" "+
		"WHERE routing_key=? AND body=?", routingKey, body)
	if err != nil {
		panic(err)
	}
}

// front returns the first job to be run in the queue.
func front() (*Job, error) {
	var interval uint
	j := Job{}
	row := db.QueryRow("SELECT routing_key, body, `interval`, next_run " +
		"FROM " + cfg.MySQL.Table + " " +
		"ORDER BY next_run ASC")
	err := row.Scan(&j.routingKey, &j.body, &interval, &j.nextRun)
	if err != nil {
		return nil, err
	}
	j.interval = time.Duration(interval) * time.Second
	return &j, nil
}

// Publish sends a message to exchange defined in the config and
// updates the Job's next run time on the database.
func (j *Job) Publish() error {
	debug("publish", *j)

	// Update next run time
	_, err := db.Exec("UPDATE "+cfg.MySQL.Table+" "+
		"SET next_run=? "+
		"WHERE routing_key=? AND body=?",
		time.Now().UTC().Add(j.interval), j.routingKey, j.body)
	if err != nil {
		return err
	}

	// Send a message to RabbitMQ
	err = channel.Publish(cfg.RabbitMQ.Exchange, j.routingKey, false, false, amqp.Publishing{
		Headers: amqp.Table{
			"interval":     j.interval.Seconds(),
			"published_at": time.Now().UTC().String(),
		},
		ContentType:     "text/plain",
		ContentEncoding: "UTF-8",
		Body:            []byte(j.body),
		DeliveryMode:    amqp.Persistent,
		Priority:        0,
		Expiration:      strconv.FormatUint(uint64(j.interval.Seconds()), 10) + "000",
	})
	if err != nil {
		return err
	}

	return nil
}

// Remaining returns the duration until the job's next scheduled time.
func (j *Job) Remaining() time.Duration {
	return -time.Since(j.nextRun)
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
	var err error
	flag.Parse()

	// Read config
	cfg = NewConfig()
	if *configPath != "" {
		err = gcfg.ReadFileInto(cfg, *configPath)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println("Read config: ", cfg)
	}

	// Connect to database
	dsn := cfg.MySQL.User + ":" + cfg.MySQL.Password + "@" + "tcp(" + cfg.MySQL.Host + ":" + cfg.MySQL.Port + ")/" + cfg.MySQL.Db + "?parseTime=true"
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MySQL")

	// Connect to RabbitMQ
	uri := "amqp://" + cfg.RabbitMQ.User + ":" + cfg.RabbitMQ.Password + "@" + cfg.RabbitMQ.Host + ":" + cfg.RabbitMQ.Port + cfg.RabbitMQ.VHost
	rabbit, err = amqp.Dial(uri)
	if err != nil {
		log.Fatal(err)
	}
	channel, err = rabbit.Channel()
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
