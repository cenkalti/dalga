package main

import (
	"code.google.com/p/gcfg"
	"fmt"
	"net/http"
)

type Config struct {
	MySQL struct {
		Uri   string
		Table string
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

func handleSchedule(w http.ResponseWriter, r *http.Request) {
	fmt.Println("schedule")
}

func main() {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, "dalga.ini")
	if err != nil {
		fmt.Errorf("Cannot read config file")
	}

	addr := cfg.HTTP.Host + ":" + cfg.HTTP.Port
	http.HandleFunc("/schedule", handleSchedule)
	http.ListenAndServe(addr, nil)
}
