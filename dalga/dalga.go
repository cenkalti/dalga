package dalga

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
	config     Config
	db         *sql.DB
	table      *table
	connection *amqp.Connection
	channel    *amqp.Channel
	listener   net.Listener
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
	return &Dalga{
		config:           config,
		notify:           make(chan struct{}, 1),
		ready:            make(chan struct{}),
		shutdown:         make(chan struct{}),
		stopPublisher:    make(chan struct{}),
		publisherStopped: make(chan struct{}),
	}
}

// Run the dalga and waits until Shutdown() is called.
func (d *Dalga) Run() error {
	if err := d.connectDB(); err != nil {
		return err
	}
	defer d.db.Close()

	if err := d.connectMQ(); err != nil {
		return err
	}
	defer d.channel.Close()
	defer d.connection.Close()

	var err error
	d.listener, err = net.Listen("tcp", d.config.HTTP.Addr())
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

func (d *Dalga) Shutdown() error {
	close(d.shutdown)
	return d.listener.Close()
}

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

func (d *Dalga) connectMQ() error {
	var err error
	d.connection, err = amqp.Dial(d.config.RabbitMQ.URL())
	if err != nil {
		return err
	}
	log.Print("Connected to RabbitMQ")

	// Exit program when AMQP connection is closed.
	connClosed := make(chan *amqp.Error)
	d.connection.NotifyClose(connClosed)
	go func() {
		if err, ok := <-connClosed; ok {
			log.Fatal(err)
		}
	}()

	d.channel, err = d.connection.Channel()
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
	db, err := sql.Open("mysql", d.config.MySQL.DSN())
	if err != nil {
		return err
	}
	defer db.Close()
	t := &table{db, d.config.MySQL.Table}
	return t.Create()
}

func (d *Dalga) Get(description, routingKey string) (*Job, error) {
	return d.table.Get(description, routingKey)
}

func (d *Dalga) Schedule(description, routingKey string, interval uint32, oneOff bool) (*Job, error) {
	job := NewJob(description, routingKey, interval, oneOff)
	err := d.table.Insert(job)
	if err != nil {
		return nil, err
	}
	d.notifyPublisher("new job")
	debug("Job is scheduled:", job)
	return job, nil
}

func (d *Dalga) Kick(description, routingKey string) (*Job, error) {
	job, err := d.Get(description, routingKey)
	if err != nil {
		return nil, err
	}
	job.NextRun = time.Now().UTC()
	if err := d.table.Insert(job); err != nil {
		return nil, err
	}
	d.notifyPublisher("job is kicked")
	debug("Job is kicked:", job)
	return job, nil
}

func (d *Dalga) Cancel(description, routingKey string) error {
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

// publish sends a message to exchange defined in the config and
// updates the Job's next run time on the database.
func (d *Dalga) publish(j *Job) error {
	debug("publish", *j)

	err := d.channel.Publish(d.config.RabbitMQ.Exchange, j.RoutingKey, true, false, amqp.Publishing{
		Body:         []byte(j.Description),
		DeliveryMode: amqp.Persistent,
		Expiration:   strconv.FormatFloat(j.Interval.Seconds(), 'f', 0, 64) + "000",
	})
	if err != nil {
		return err
	}

	if j.Interval == 0 {
		debug("deleting one-off job")
		return d.table.Delete(j.Description, j.RoutingKey)
	}

	j.SetNewNextRun()
	return d.table.UpdateNextRun(j)
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
			return
		}
	}
}
