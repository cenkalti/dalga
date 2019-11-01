package dalga

import (
	"database/sql"
	"errors"
	"flag"
	"log"
	"net"
	"sync"
	"time"
)

const Version = "2.0.0"

var debugging = flag.Bool("debug", false, "turn on debug messages")

func debug(args ...interface{}) {
	if *debugging {
		log.Println(args...)
	}
}

type Dalga struct {
	config    Config
	db        *sql.DB
	table     *table
	listener  net.Listener
	Jobs      *JobManager
	scheduler *scheduler
	// will be closed when dalga is ready to accept requests
	ready chan struct{}
	// will be closed by Shutdown method
	shutdown     chan struct{}
	onceShutdown sync.Once
}

func New(config Config) (*Dalga, error) {
	if config.Jobs.RandomizationFactor < 0 || config.Jobs.RandomizationFactor > 1 {
		return nil, errors.New("randomization factor must be between 0 and 1")
	}
	db, err := sql.Open("mysql", config.MySQL.DSN())
	if err != nil {
		return nil, err
	}
	t := &table{db: db, name: config.MySQL.Table}
	s := newScheduler(t, config.Endpoint.BaseURL, time.Duration(config.Endpoint.Timeout)*time.Second, config.Jobs.RandomizationFactor)
	m := newJobManager(t, s)
	return &Dalga{
		config:    config,
		db:        db,
		table:     t,
		Jobs:      m,
		scheduler: s,
		ready:     make(chan struct{}),
		shutdown:  make(chan struct{}),
	}, nil
}

// Run Dalga. This function is blocking. Returns nil after Shutdown is called.
func (d *Dalga) Run() error {
	var err error
	d.listener, err = net.Listen("tcp", d.config.Listen.Addr())
	if err != nil {
		return err
	}
	log.Println("listening", d.listener.Addr())

	d.db, err = sql.Open("mysql", d.config.MySQL.DSN())
	if err != nil {
		return err
	}
	defer d.db.Close()

	if err = d.db.Ping(); err != nil {
		return err
	}
	log.Print("connected to mysql")

	if err = d.table.Prepare(); err != nil {
		return err
	}

	log.Print("dalga is ready")
	close(d.ready)

	go d.scheduler.Run()
	defer func() {
		d.scheduler.Stop()
		<-d.scheduler.NotifyDone()
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

// Shutdown running Dalga gracefully.
func (d *Dalga) Shutdown() {
	d.onceShutdown.Do(func() {
		close(d.shutdown)
		if err := d.listener.Close(); err != nil {
			log.Print(err)
		}
	})
}

// NotifyReady returns a channel that will be closed when Dalga is ready to accept HTTP requests.
func (d *Dalga) NotifyReady() <-chan struct{} {
	return d.ready
}

// CreateTable creates the table for storing jobs on database.
func (d *Dalga) CreateTable() error {
	db, err := sql.Open("mysql", d.config.MySQL.DSN())
	if err != nil {
		return err
	}
	defer db.Close()
	t := &table{db: db, name: d.config.MySQL.Table}
	return t.Create()
}
