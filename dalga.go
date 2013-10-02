package main

import (
	"code.google.com/p/gcfg"
	"fmt"
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
}

func main() {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, "dalga.ini")
	if err != nil {
		fmt.Errorf("Cannot read config file")
	}
}
