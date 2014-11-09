package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/cenkalti/dalga/dalga"
)

var config = dalga.DefaultConfig

var createTable = flag.Bool("create-table", false, "create table for storing jobs")

func init() {
	flag.StringVar(&config.MySQL.Host, "mysql-host", config.MySQL.Host, "")
	flag.StringVar(&config.MySQL.Port, "mysql-port", config.MySQL.Port, "")
	flag.StringVar(&config.MySQL.DB, "mysql-db", config.MySQL.DB, "")
	flag.StringVar(&config.MySQL.Table, "mysql-table", config.MySQL.Table, "")
	flag.StringVar(&config.MySQL.User, "mysql-user", config.MySQL.User, "")
	flag.StringVar(&config.MySQL.Password, "mysql-password", config.MySQL.Password, "")

	flag.StringVar(&config.RabbitMQ.Host, "rabbitmq-host", config.RabbitMQ.Host, "")
	flag.StringVar(&config.RabbitMQ.Port, "rabbitmq-port", config.RabbitMQ.Port, "")
	flag.StringVar(&config.RabbitMQ.VHost, "rabbitmq-vhost", config.RabbitMQ.VHost, "")
	flag.StringVar(&config.RabbitMQ.Exchange, "rabbitmq-exchange", config.RabbitMQ.Exchange, "")
	flag.StringVar(&config.RabbitMQ.User, "rabbitmq-user", config.RabbitMQ.User, "")
	flag.StringVar(&config.RabbitMQ.Password, "rabbitmq-password", config.RabbitMQ.Password, "")

	flag.StringVar(&config.HTTP.Host, "http-host", config.HTTP.Host, "")
	flag.StringVar(&config.HTTP.Port, "http-port", config.HTTP.Port, "")
}

func main() {
	flag.Parse()

	// Initialize Dalga object
	d := dalga.New(config)

	// Create jobs table
	if *createTable {
		if err := d.CreateTable(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Table created successfully")
		return
	}

	// Run Dalga
	if err := d.Run(); err != nil {
		log.Fatal(err)
	}
}
