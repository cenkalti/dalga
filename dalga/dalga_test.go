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

	"github.com/cenkalti/dalga/vendor/github.com/go-sql-driver/mysql"
	"github.com/cenkalti/dalga/vendor/github.com/streadway/amqp"
)

var (
	testRoutingKey  = "testRoutingKey"
	testDescription = "jobDescription"
	testInterval    = time.Second
	testDelay       = time.Second
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
		t.Fatalf("cannot connect to MySQL: %s", err.Error())
	}

	println("connected to db")

	drop_sql := "DROP TABLE " + config.MySQL.Table
	_, err = db.Exec(drop_sql)
	if err != nil {
		if myErr, ok := err.(*mysql.MySQLError); !ok || myErr.Number != 1051 { // Unknown table
			t.Fatal(err)
		}
	}

	println("dropped table")

	mq, err := amqp.Dial(config.RabbitMQ.URL())
	if err != nil {
		t.Fatalf("cannot connect to RabbitMQ: %s", err.Error())
	}

	defer mq.Close()

	channel, err := mq.Channel()
	if err != nil {
		t.Fatal(err)
	}

	println("connected to mq")

	_, err = channel.QueueDelete(testRoutingKey, false, false, false)
	if err != nil {
		if mqErr, ok := err.(*amqp.Error); !ok || mqErr.Code != 404 { // NOT_FOUND
			t.Fatal(err)
		}

		// Channel is closed after an error, need to re-open.
		channel, err = mq.Channel()
		if err != nil {
			t.Fatal(err)
		}
	}

	println("deleted queue")

	d := New(config)

	err = d.CreateTable()
	if err != nil {
		t.Fatal(err)
	}

	println("created table")

	_, err = channel.QueueDeclare(testRoutingKey, false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	println("declared queue")

	done := make(chan struct{})
	go func() {
		if err := d.Run(); err != nil {
			t.Fatal(err)
		}
		close(done)
	}()

	<-d.NotifyReady()

	values := make(url.Values)
	values.Set("interval", strconv.Itoa(int(testInterval/time.Second)))

	endpoint := "http://" + config.HTTP.Addr() + "/jobs/" + testRoutingKey + "/" + testDescription
	req, err := http.NewRequest("PUT", endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var client http.Client
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Cannot schedule new job: %s", err.Error())
	}
	if resp.StatusCode != 201 {
		var buf bytes.Buffer
		buf.ReadFrom(resp.Body)
		t.Fatalf("Unexpected status code: %d, body: %q", resp.StatusCode, buf.String())
	}

	println("scheduled job")

	deliveries, err := channel.Consume(testRoutingKey, "", false, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case d, ok := <-deliveries:
		if !ok {
			t.Fatal("Consumer closed")
		}
		println("got message from queue")
		if string(d.Body) != testDescription {
			t.Fatalf("Invalid body: %s", string(d.Body))
		}
	case <-time.After(testInterval + testDelay):
		t.Fatal("timeout")
	}

	// Teardown
	if err := d.Shutdown(); err != nil {
		t.Fatal(err)
	}

	<-done

	// Cleanup
	db.Exec(drop_sql)
}
