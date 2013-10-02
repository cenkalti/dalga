package main

import (
	"code.google.com/p/gcfg"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
)

var cfg struct {
	MySQL struct {
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

var db *sql.DB

func handleSchedule(w http.ResponseWriter, r *http.Request) {
	fmt.Println("schedule")
}

func handleCancel(w http.ResponseWriter, r *http.Request) {
	fmt.Println("cancel")
}

func main() {
	// Read config
	err := gcfg.ReadFileInto(&cfg, "dalga.ini")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Read config: ", cfg)

	// Connect to MySQL
	_, err = sql.Open(cfg.MySQL.Driver, cfg.MySQL.Dsn)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MySQL")

	// Connect to RabbitMQ
	// fmt.Println("Connected to MySQL")

	// Start HTTP server
	addr := cfg.HTTP.Host + ":" + cfg.HTTP.Port
	http.HandleFunc("/schedule", handleSchedule)
	http.HandleFunc("/cancel", handleSchedule)
	http.ListenAndServe(addr, nil)
}
