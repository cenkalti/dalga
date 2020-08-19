package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/cenkalti/dalga/v2"
	"github.com/cenkalti/dalga/v2/internal/log"
)

var (
	config      = flag.String("config", "", "config file")
	version     = flag.Bool("version", false, "print version")
	createTable = flag.Bool("create-table", false, "create table for storing jobs")
	debug       = flag.Bool("debug", false, "turn on debug messages")
)

func main() {
	flag.Parse()

	if *version {
		fmt.Println(dalga.Version)
		return
	}

	if *debug {
		log.EnableDebug()
	}

	c := dalga.DefaultConfig
	if *config != "" {
		if err := c.LoadFromFile(*config); err != nil {
			log.Fatal(err)
		}
	}

	d, err := dalga.New(c)
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	if *createTable {
		if err := d.CreateTable(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Table created successfully")
		return
	}

	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-signals
		cancel()
	}()

	d.Run(ctx)
}
