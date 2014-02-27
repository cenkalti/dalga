package dalga

import (
	"bytes"
	"database/sql"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
)

var (
	testKey      = "testKey"
	testBody     = []byte("body")
	testInterval = 1 * time.Second
	testDelay    = 100 * time.Millisecond
)

func TestSchedule(t *testing.T) {
	config := NewConfig() // default config

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

	sql := "DROP TABLE " + config.MySQL.Table
	_, err = db.Exec(sql)
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

	d := NewDalga(config)

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

	time.Sleep(testInterval + testDelay)

	msg, ok, err := channel.Get(testKey, true)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("No message")
	}
	if bytes.Compare(msg.Body, testBody) != 0 {
		t.Fatalf("Invalid body: %s", string(msg.Body))
	}

	println("got message from queue")
}
