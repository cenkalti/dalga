package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cenkalti/dalga/v3"
	"github.com/cenkalti/dalga/v3/internal/log"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
)

// These variables are set by goreleaser on build.
var (
	version = "0.0.0"
	commit  = ""
	date    = ""
)

var (
	configFlag   = flag.String("config", "dalga.toml", "config file")
	versionFlag  = flag.Bool("version", false, "print version")
	createTables = flag.Bool("create-tables", false, "create table for storing jobs")
	debug        = flag.Bool("debug", false, "turn on debug messages")
)

func versionString() string {
	if len(commit) > 7 {
		commit = commit[:7]
	}
	return fmt.Sprintf("%s (%s) [%s]", version, commit, date)
}

func main() {
	flag.Parse()

	if *versionFlag {
		fmt.Println(versionString())
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

	if *createTables {
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
	err = k.Load(file.Provider(*configFlag), toml.Parser())
	if err != nil {
		return
	}
	err = k.Load(env.Provider("DALGA_", ".", func(s string) string {
		return strings.Replace(strings.TrimPrefix(s, "DALGA_"), "_", ".", -1)
	}), nil)
	if err != nil {
		return
	}
	err = k.Unmarshal("", &c)
	return
}
