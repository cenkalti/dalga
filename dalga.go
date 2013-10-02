package main

import (
	"code.google.com/p/gcfg"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"strconv"
	"time"
)

var (
	cfg struct {
		DB struct {
			Driver string
			Dsn    string
			Table  string
		}
		RabbitMQ struct {
			Uri      string
			Exchange string
		}
		HTTP struct {
			Host string
			Port string
		}
	}

	db     *sql.DB
	broker *amqp.Connection
)

func handleSchedule(w http.ResponseWriter, r *http.Request) {
	routingKey, body, interval_s := r.FormValue("routing_key"), r.FormValue("body"), r.FormValue("interval")
	log.Println("/schedule", routingKey, body)
	interval, err := strconv.ParseInt(interval_s, 10, 64)
	if err != nil {
		panic(err)
	}

	next_run := time.Now().Add(time.Duration(interval) * time.Second)
	_, err = db.Exec("INSERT IGNORE INTO "+cfg.DB.Table+" "+
		"(routing_key, body, `interval`, next_run, state) "+
		"VALUES(?, ?, ?, ?, 'WAITING')",
		routingKey, body, interval, next_run)
	if err != nil {
		panic(err)
	}
}

func handleCancel(w http.ResponseWriter, r *http.Request) {
	fmt.Println("/cancel")
}

func main() {
	// Read config
	err := gcfg.ReadFileInto(&cfg, "dalga.ini")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Read config: ", cfg)

	// Connect to database
	db, err = sql.Open(cfg.DB.Driver, cfg.DB.Dsn)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to DB")

	// Connect to RabbitMQ
	_, err = amqp.Dial(cfg.RabbitMQ.Uri)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to RabbitMQ")

	// Start HTTP server
	addr := cfg.HTTP.Host + ":" + cfg.HTTP.Port
	http.HandleFunc("/schedule", handleSchedule)
	http.HandleFunc("/cancel", handleSchedule)
	http.ListenAndServe(addr, nil)
}
