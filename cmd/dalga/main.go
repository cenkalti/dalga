package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/cenkalti/dalga/v3"
	"github.com/cenkalti/dalga/v3/internal/log"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/file"
)

var (
	config      = flag.String("config", "dalga.toml", "config file")
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

	c, err := readConfig()
	if err != nil {
		log.Fatal(err)
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

func readConfig() (c dalga.Config, err error) {
	c = dalga.DefaultConfig
	k := koanf.New(".")
	err = k.Load(file.Provider(*config), toml.Parser())
	if err != nil {
		return
	}
	err = k.Unmarshal("", &c)
	return
}
