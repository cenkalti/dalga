package scheduler // nolint:testpackage

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/dalga/v4/internal/instance"
	"github.com/cenkalti/dalga/v4/internal/log"
	"github.com/cenkalti/dalga/v4/internal/retry"
	"github.com/cenkalti/dalga/v4/internal/table"
	"github.com/senseyeio/duration"
)

const tableName = "sched"

var dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&transaction_isolation=%%27READ-COMMITTED%%27", "root", "", "127.0.0.1", 3306, "test")

// Run with command: go test -bench=Bench -run=x ./internal/scheduler
// To see the output of a single test, add -v flag and comment out desired tests below.

func BenchmarkScheduler(b *testing.B) {
	if testing.Verbose() {
		log.EnableDebug()
	} else {
		log.Disable()
	}

	const numJobs = 1000
	cleanup := prepareTable(b, dsn, numJobs)
	defer cleanup()

	testCases := []struct {
		numInstances int
		maxRunning   int
		skipLocked   bool
	}{
		{1, 1, false},
		{1, 0, false},
		{10, 1, false},
		{10, 0, false},
		{1, 1, true},
		{1, 0, true},
		{10, 1, true},
		{10, 0, true},
	}

	for _, tc := range testCases {
		tc := tc
		b.Run(fmt.Sprintf("numInstances:%d maxRunning:%d skipLocked:%v", tc.numInstances, tc.maxRunning, tc.skipLocked), func(b *testing.B) {
			benchmarkScheduler(b, tc.numInstances, tc.maxRunning, tc.skipLocked)
		})
	}
}

func benchmarkScheduler(b *testing.B, numInstances, maxRunning int, skipLocked bool) {
	var wg sync.WaitGroup

	// Craeate scheduler instances.
	schedulers := make([]*Scheduler, 0, numInstances)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < numInstances; i++ {
		s, cleanup := prepareInstance(ctx, b, dsn, skipLocked, maxRunning)
		defer cleanup()
		schedulers = append(schedulers, s)
	}

	// Run all schedulers but one in infinite loop to see how it affects performance.
	// Note that benchmark output shows only single instance throughput.
	// To find out the throughput of whole cluster, the result need to be multiplied by the number of instances.
	for i := 1; i < numInstances; i++ {
		wg.Add(1)
		go func(s *Scheduler) {
			for {
				if ok := s.runOnce(ctx); !ok {
					break
				}
			}
			wg.Done()
		}(schedulers[i])
	}

	// Benchmark the first scheduler instance.
	s := schedulers[0]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.runOnce(context.Background())
	}

	cancel()
	wg.Wait()
}

func prepareInstance(ctx context.Context, b *testing.B, dsn string, skipLocked bool, maxRunning int) (*Scheduler, func()) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatal(err)
	}
	r := &retry.Retry{
		Interval:    time.Second,
		MaxInterval: time.Second,
		Multiplier:  1,
	}
	tbl := table.New(db, tableName)
	tbl.SkipLocked = skipLocked

	i := instance.New(tbl)
	go i.Run(ctx)
	<-i.NotifyReady()

	s := New(tbl, i.ID(), "http://example.com/", 4*time.Second, r, 0, 250*time.Millisecond, maxRunning)
	s.skipOneOffDelete = true
	s.skipPost = true

	return s, func() {
		<-i.NotifyDone()
		db.Close()
	}
}

func prepareTable(t *testing.B, dsn string, numJobs int) func() {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Log("creating tables")
	tbl := table.New(db, tableName)
	if err := tbl.Drop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Create(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Create some number of one-off jobs.
	t.Logf("adding %d jobs", numJobs)
	nextRun := time.Now().UTC().Add(-time.Hour) // time in past, a job that is ready to run
	for i := 0; i < numJobs; i++ {
		_, err := tbl.AddJob(context.Background(), table.Key{Path: "path", Body: "body" + strconv.Itoa(i)}, duration.Duration{TS: 0}, duration.Duration{}, time.UTC, nextRun)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Log("job table is ready")
	return func() { tbl.Drop(context.Background()) }
}
