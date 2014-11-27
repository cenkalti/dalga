package dalga

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/cenkalti/dalga/dalga/Godeps/_workspace/src/github.com/fzzy/radix/redis"
)

var debugging = flag.Bool("debug", false, "turn on debug messages")

const (
	redisLockKey        = "dalga-lock"
	redisLockExpiry     = 30 * time.Second
	redisLockRenewAfter = 20 * time.Second
)

func debug(args ...interface{}) {
	if *debugging {
		log.Println(args...)
	}
}

type Dalga struct {
	config    Config
	redis     *redis.Client
	db        *sql.DB
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
	db, err := sql.Open("mysql", config.MySQL.DSN())
	if err != nil {
		return nil, err
	}
	t := &table{db, config.MySQL.Table}
	s := newScheduler(t, config.Endpoint.BaseURL, time.Duration(config.Endpoint.Timeout)*time.Second)
	m := newJobManager(t, s)
	return &Dalga{
		config:    config,
		db:        db,
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
	log.Println("Listening", d.listener.Addr())

	if !d.config.Redis.Zero() {
		d.redis, err = redis.Dial("tcp", d.config.Redis.Addr())
		if err != nil {
			return err
		}
		log.Print("Connected to Redis")
		if err = d.holdRedisLock(); err != nil {
			return err
		}
	}

	d.db, err = sql.Open("mysql", d.config.MySQL.DSN())
	if err != nil {
		return err
	}
	defer d.db.Close()

	if err = d.db.Ping(); err != nil {
		return err
	}
	log.Print("Connected to MySQL")

	log.Print("Dalga is ready")
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

func (d *Dalga) holdRedisLock() error {
	log.Print("Acquiring Redis lock")
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	value := fmt.Sprintf("%s:%d", hostname, d.listener.Addr().(*net.TCPAddr).Port)
	reply := d.redis.Cmd("SET", redisLockKey, value, "NX", "PX", int(redisLockExpiry/time.Millisecond))
	if reply.Err != nil {
		return reply.Err
	}
	status, err := reply.Str()
	if status != "OK" {
		return errors.New("Cannot acquire Redis lock")
	}
	log.Print("Acquired Redis lock")
	go d.renewRedisLock(value)
	return nil
}

func (d *Dalga) renewRedisLock(value string) {
	for {
		select {
		case <-time.After(redisLockRenewAfter):
			debug("renewing Redis lock")
			reply := d.redis.Cmd("EVAL", `
				if redis.call("GET", KEYS[1]) == ARGV[1] then
					return redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
				else
					return 0
				end
				`, 1, redisLockKey, value, int(redisLockExpiry/time.Millisecond))
			if reply.Err != nil {
				log.Print(reply.Err)
				d.Shutdown()
				return
			}
			debug("lock renewed")
		case <-d.scheduler.NotifyDone():
			d.redis.Cmd("DEL", redisLockKey)
			return
		}
	}
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
	t := &table{db, d.config.MySQL.Table}
	return t.Create()
}
