package dalga

import (
	"bytes"
	"database/sql"
	"testing"
	"time"

	"github.com/cenkalti/dalga/vendor/github.com/go-sql-driver/mysql"
	"github.com/cenkalti/dalga/vendor/github.com/streadway/amqp"
)

var (
	testKey      = "testKey"
	testBody     = []byte("body")
	testInterval = 1 * time.Second
	testDelay    = 1 * time.Second
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

	_, err = channel.QueueDelete(testKey, false, false, false)
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

	_, err = channel.QueueDeclare(testKey, false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	println("declared queue")

	err = d.Start()
	if err != nil {
		t.Fatalf("cannot start Dalga: %s", err.Error())
	}

	println("started dalga")

	defer d.Shutdown()

	err = d.Schedule(testKey, testBody, uint32(testInterval/time.Second))
	if err != nil {
		t.Fatalf("Cannot schedule new job: %s", err.Error())
	}

	println("scheduled job")

	deliveries, err := channel.Consume(testKey, "", false, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case d, ok := <-deliveries:
		if !ok {
			t.Fatal("Consumer closed")
		}
		println("got message from queue")
		if !bytes.Equal(d.Body, testBody) {
			t.Fatalf("Invalid body: %s", string(d.Body))
		}
	case <-time.After(testInterval + testDelay):
		t.Fatal("timeout")
	}

	// Cleanup
	db.Exec(drop_sql)
}
