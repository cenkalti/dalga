package dalga

// TODO shutdown dalga gracefully
// TODO stop all goroutines
// TODO rename id to job and change limit to 65535
// TODO put a lock on table while working on it
// TODO update readme
// TODO implement raft consensus

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
	notify            chan struct{}
	quitPublisher     chan bool
	publisherFinished chan bool
}

func New(config Config) *Dalga {
	return &Dalga{
		Config:            config,
		notify:            make(chan struct{}, 1),
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
	log.Print("Connected to MySQL")
	d.table = &table{db, d.Config.MySQL.Table}
	return nil
}

func (d *Dalga) connectMQ() error {
	conn, err := amqp.Dial(d.Config.RabbitMQ.URL())
	if err != nil {
		return err
	}
	log.Print("Connected to RabbitMQ")

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
	select {
	case d.notify <- struct{}{}:
		debug("Sent new job signal")
	default:
	}

	debug("Job is scheduled:", job)
	return nil
}

func (d *Dalga) Cancel(id, routingKey string) error {
	if err := d.table.Delete(id, routingKey); err != nil {
		return err
	}

	select {
	case d.notify <- struct{}{}:
		debug("Sent cancel signal")
	default:
	}

	debug("Job is cancelled:", Job{primaryKey: primaryKey{id, routingKey}})
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
	defer func() { d.publisherFinished <- true }()

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
		case <-d.quitPublisher:
			debug("Came quit message")
			return
		}
	}
}
