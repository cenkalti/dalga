package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/cenkalti/dalga/dalga"
	"github.com/cenkalti/dalga/vendor/code.google.com/p/gcfg"
)

var (
	config      = flag.String("config", "", "config file")
	createTable = flag.Bool("create-table", false, "create table for storing jobs")
)

func main() {
	flag.Parse()

	c := dalga.DefaultConfig
	if *config != "" {
		if err := gcfg.ReadFileInto(&c, *config); err != nil {
			log.Fatal(err)
		}
	}

	d := dalga.New(c)

	if *createTable {
		if err := d.CreateTable(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Table created successfully")
		return
	}

	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, os.Kill)
	go func() {
		<-signals
		if err := d.Shutdown(); err != nil {
			log.Fatal(err)
		}
	}()

	if err := d.Run(); err != nil {
		log.Fatal(err)
	}
}
