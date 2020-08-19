package dalga

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/dalga/internal/log"
)

func init() {
	log.EnableDebug()
}

const (
	testBody    = "testBody"
	testTimeout = 5 * time.Second
	testAddr    = "127.0.0.1:5000"
)

func TestSchedule(t *testing.T) {
	config := DefaultConfig
	config.MySQL.SkipLocked = false

	d, lis, cleanup := newDalga(t, config)
	defer cleanup()

	called := make(chan string)
	endpoint := func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		buf.ReadFrom(r.Body)
		r.Body.Close()
		called <- buf.String()
	}

	http.HandleFunc("/", endpoint)
	go http.ListenAndServe(testAddr, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	defer func() {
		cancel()
		<-d.NotifyDone()
	}()

	values := make(url.Values)
	values.Set("one-off", "true")
	values.Set("first-run", "1990-01-01T00:00:00Z")

	scheduleURL := "http://" + lis.Addr() + "/jobs/testPath/" + testBody
	req, err := http.NewRequest("PUT", scheduleURL, strings.NewReader(values.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var client http.Client
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("cannot schedule new job: %s", err.Error())
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	if resp.StatusCode != 201 {
		t.Fatalf("unexpected status code: %d, body: %q", resp.StatusCode, buf.String())
	}
	println("PUT response:", buf.String())

	println("scheduled job")

	select {
	case body := <-called:
		println("endpoint is called")
		if body != testBody {
			t.Fatalf("Invalid body: %s", body)
		}
	case <-time.After(testTimeout):
		t.Fatal("timeout")
	}
	time.Sleep(time.Second)
}

func newDalga(t *testing.T, config Config) (*Dalga, listenConfig, func()) {
	db, err := sql.Open("mysql", config.MySQL.DSN())
	if err != nil {
		t.Fatal(err.Error())
	}
	var cleanups []func()
	cleanups = append(cleanups, func() {
		db.Close()
	})

	err = db.Ping()
	if err != nil {
		t.Fatalf("cannot connect to mysql: %s", err.Error())
	}
	println("connected to db")

	d, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	cleanups = append(cleanups, func() {
		d.Close()
	})

	err = d.table.Drop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	println("dropped table")

	err = d.CreateTable()
	if err != nil {
		t.Fatal(err)
	}
	println("created table")
	cleanups = append(cleanups, func() {
		d.table.Drop(context.Background())
	})

	return d, config.Listen, func() {
		for _, fn := range cleanups {
			fn()
		}
	}
}
