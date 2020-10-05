package dalga

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net"
	"time"

	"github.com/cenkalti/dalga/v4/internal/clock"
	"github.com/cenkalti/dalga/v4/internal/instance"
	"github.com/cenkalti/dalga/v4/internal/jobmanager"
	"github.com/cenkalti/dalga/v4/internal/retry"
	"github.com/cenkalti/dalga/v4/internal/scheduler"
	"github.com/cenkalti/dalga/v4/internal/server"
	"github.com/cenkalti/dalga/v4/internal/table"
)

// Dalga is a job scheduler.
type Dalga struct {
	config    Config
	db        *sql.DB
	listener  net.Listener
	table     *table.Table
	instance  *instance.Instance
	Jobs      *jobmanager.JobManager
	scheduler *scheduler.Scheduler
	server    *server.Server
	done      chan struct{}
}

// New returns a new Dalga instance. Close must be called when disposing the object.
func New(config Config) (*Dalga, error) {
	if config.Jobs.RandomizationFactor < 0 || config.Jobs.RandomizationFactor > 1 {
		return nil, errors.New("randomization factor must be between 0 and 1")
	}

	db, err := sql.Open("mysql", config.MySQL.DSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(config.MySQL.MaxOpenConns)

	lis, err := net.Listen("tcp", config.Listen.Addr())
	if err != nil {
		db.Close()
		return nil, err
	}
	log.Println("listening", lis.Addr())

	t := table.New(db, config.MySQL.Table)
	t.SkipLocked = config.MySQL.SkipLocked
	t.FixedIntervals = config.Jobs.FixedIntervals
	i := instance.New(t)
	r := &retry.Retry{
		Interval:    config.Jobs.RetryInterval,
		MaxInterval: config.Jobs.RetryMaxInterval,
		Multiplier:  config.Jobs.RetryMultiplier,
		StopAfter:   config.Jobs.RetryStopAfter,
	}
	s := scheduler.New(t, i.ID(), config.Endpoint.BaseURL, config.Endpoint.Timeout, r, config.Jobs.RandomizationFactor, config.Jobs.ScanFrequency, config.Jobs.MaxRunning)
	j := jobmanager.New(t, s)
	srv := server.New(j, t, i.ID(), lis, config.Listen.ShutdownTimeout)
	return &Dalga{
		config:    config,
		db:        db,
		listener:  lis,
		table:     t,
		instance:  i,
		scheduler: s,
		Jobs:      j,
		server:    srv,
		done:      make(chan struct{}),
	}, nil
}

// Close database connections and HTTP listener.
func (d *Dalga) Close() {
	d.listener.Close()
	d.db.Close()
}

// NotifyDone returns a channel that will be closed when Run method returns.
func (d *Dalga) NotifyDone() chan struct{} {
	return d.done
}

// Run Dalga. This function is blocking.
func (d *Dalga) Run(ctx context.Context) {
	defer close(d.done)

	go d.server.Run(ctx)
	go d.instance.Run(ctx)
	go d.scheduler.Run(ctx)

	<-ctx.Done()

	<-d.server.NotifyDone()
	<-d.instance.NotifyDone()
	<-d.scheduler.NotifyDone()
}

// CreateTable creates the table for storing jobs on database.
func (d *Dalga) CreateTable() error {
	return d.table.Create(context.Background())
}

// UseClock overrides Dalga's datetime to help test schedules, retry behavior, etc.
// Use the returned "clock" to manually advance time and trigger jobs as desired.
func (d *Dalga) UseClock(now time.Time) *clock.Clock {
	d.table.Clk = clock.New(now)
	return d.table.Clk
}
