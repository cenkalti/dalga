package dalga

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/dalga/v3/internal/log"
)

func init() {
	log.EnableDebug()
}

const (
	testBody    = "testBody"
	testTimeout = 5 * time.Second
)

func TestSchedule(t *testing.T) {
	called := make(chan string)
	endpoint := func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		buf.ReadFrom(r.Body)
		r.Body.Close()
		called <- buf.String()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", endpoint)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	config := DefaultConfig
	config.MySQL.SkipLocked = false
	config.Endpoint.BaseURL = "http://" + srv.Listener.Addr().String() + "/"

	d, lis, cleanup := newDalga(t, config)
	defer cleanup()

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
	t.Log("PUT response:", buf.String())

	t.Log("scheduled job")

	select {
	case body := <-called:
		t.Log("endpoint is called")
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
	t.Log("connected to db")

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
	t.Log("dropped table")

	err = d.CreateTable()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("created table")
	cleanups = append(cleanups, func() {
		d.table.Drop(context.Background())
	})

	return d, config.Listen, func() {
		for _, fn := range cleanups {
			fn()
		}
	}
}
