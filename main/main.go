package main

import (
	"code.google.com/p/gcfg"
	"flag"
	"fmt"
	"github.com/cenkalti/dalga"
	"log"
)

var configPath = flag.String("c", "", "config file path")

func main() {
	var err error
	flag.Parse()

	// Read config
	c := dalga.NewConfig()
	if *configPath != "" {
		err = gcfg.ReadFileInto(c, *configPath)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println("Read config: ", c)
	}

	// Run Dalga
	d := dalga.NewDalga(c)
	err = d.Run()
	if err != nil {
		log.Fatalln(err)
	}
}
