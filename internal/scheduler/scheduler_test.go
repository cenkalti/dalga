package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/senseyeio/duration"

	"github.com/cenkalti/dalga/v2/internal/instance"
	"github.com/cenkalti/dalga/v2/internal/retry"
	"github.com/cenkalti/dalga/v2/internal/table"
)

var dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true", "root", "", "127.0.0.1", 3306, "test")

// TestSchedHeader verifies that when the scheduler executes a job, the
// POST includes a header with the unix timestamp of the intended execution.
//
// Retries of a particular execution will preserve the timestamp of the
// original execution, which receivers can use to ensure idempotency.
func TestSchedHeader(t *testing.T) {
	rcv := make(chan int64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr := r.Header.Get("dalga-sched")
		unix, err := strconv.ParseInt(hdr, 10, 64)
		if err != nil {
			t.Error(err)
			return
		}
		rcv <- unix
		http.Error(w, "job failed", 500)
	}))

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl := table.New(db, "sched")
	if err := tbl.Drop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Create(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer tbl.Drop(context.Background()) // nolint: errcheck

	r := &retry.Retry{
		Interval:    time.Second,
		MaxInterval: time.Second,
		Multiplier:  1,
	}
	i := instance.New(tbl)
	s := New(tbl, i.ID(), "http://"+srv.Listener.Addr().String()+"/", 4*time.Second, r, 0, 250*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		<-i.NotifyDone()
		<-s.NotifyDone()
	}()
	go i.Run(ctx)
	go s.Run(ctx)

	nextRun := time.Now().UTC().Truncate(time.Second)
	_, err = tbl.AddJob(context.Background(), table.Key{Path: "abc", Body: "def"}, duration.Duration{TS: 10}, duration.Duration{}, time.UTC, nextRun)
	if err != nil {
		t.Fatal(err)
	}

	// First run
	select {
	case unix := <-rcv:
		if expect := nextRun.Unix(); unix != expect {
			t.Fatalf("Expected unix %d and found %d", expect, unix)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Job never fired.")
	}

	// Retry must preserve original sched time
	select {
	case unix := <-rcv:
		if expect := nextRun.Unix(); unix != expect {
			t.Fatalf("Expected unix %d and found %d", expect, unix)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Job is not retried.")
	}
}
