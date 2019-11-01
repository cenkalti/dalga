package dalga

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func init() {
	*debugging = true
}

const (
	testPath     = "testPath"
	testBody     = "testBody"
	testInterval = time.Duration(0)
	testOneOff   = "true"
	testDelay    = time.Second
)

func TestSchedule(t *testing.T) {
	config := DefaultConfig

	db, err := sql.Open("mysql", config.MySQL.DSN())
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatalf("cannot connect to mysql: %s", err.Error())
	}

	println("connected to db")

	dropSQL := "DROP TABLE " + config.MySQL.Table
	_, err = db.Exec(dropSQL)
	if err != nil {
		if myErr, ok := err.(*mysql.MySQLError); !ok || myErr.Number != 1051 { // Unknown table
			t.Fatal(err)
		}
	}

	println("dropped table")

	called := make(chan string)
	endpoint := func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		buf.ReadFrom(r.Body)
		r.Body.Close()
		called <- buf.String()
	}

	http.HandleFunc("/", endpoint)
	go http.ListenAndServe("127.0.0.1:5000", nil)

	d, err := New(config)
	if err != nil {
		t.Fatal(err)
	}

	err = d.CreateTable()
	if err != nil {
		t.Fatal(err)
	}

	println("created table")

	var gerr error
	done := make(chan struct{})
	go func() {
		gerr = d.Run()
		close(done)
	}()

	select {
	case <-d.NotifyReady():
	case <-done:
		t.Fatal(gerr)
	}

	values := make(url.Values)
	values.Set("interval", strconv.Itoa(int(testInterval/time.Second)))
	values.Set("one-off", testOneOff)

	scheduleURL := "http://" + config.Listen.Addr() + "/jobs/" + testPath + "/" + testBody
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
	case <-time.After(testInterval + testDelay):
		t.Fatal("timeout")
	}

	// Teardown
	d.Shutdown()
	<-done

	// Cleanup
	db.Exec(dropSQL)
}
