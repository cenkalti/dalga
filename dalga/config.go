package dalga

import (
	"fmt"
)

type Config struct {
	MySQL    mysqlConfig
	RabbitMQ rabbitmqConfig
	HTTP     httpConfig
}

// NewConfig returns a pointer to a newly created Config initialized with default parameters.
func NewConfig() *Config {
	return &Config{
		MySQL: mysqlConfig{
			User:  "root",
			Host:  "localhost",
			Port:  "3306",
			DB:    "test",
			Table: "dalga",
		},
		RabbitMQ: rabbitmqConfig{
			User:     "guest",
			Password: "guest",
			Host:     "localhost",
			Port:     "5672",
			VHost:    "/",
		},
		HTTP: httpConfig{
			Host: "0.0.0.0",
			Port: "17500",
		},
	}
}

type mysqlConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	DB       string
	Table    string
}

func (c mysqlConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", c.User, c.Password, c.Host, c.Port, c.DB)
}

type rabbitmqConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	VHost    string
	Exchange string
}

func (c rabbitmqConfig) URL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s%s", c.User, c.Password, c.Host, c.Port, c.VHost)
}

type httpConfig struct {
	Host string
	Port string
}

func (c httpConfig) Addr() string {
	return c.Host + ":" + c.Port
}
