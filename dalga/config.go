package dalga

import "fmt"

var DefaultConfig = Config{
	MySQL: mysqlConfig{
		Host:     "localhost",
		Port:     "3306",
		DB:       "test",
		Table:    "dalga",
		User:     "root",
		Password: "",
	},
	RabbitMQ: rabbitmqConfig{
		Host:     "localhost",
		Port:     "5672",
		VHost:    "/",
		Exchange: "",
		User:     "guest",
		Password: "guest",
	},
	HTTP: httpConfig{
		Host: "0.0.0.0",
		Port: "17500",
	},
}

type Config struct {
	MySQL    mysqlConfig
	RabbitMQ rabbitmqConfig
	HTTP     httpConfig
}

type mysqlConfig struct {
	Host     string
	Port     string
	DB       string
	Table    string
	User     string
	Password string
}

func (c mysqlConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", c.User, c.Password, c.Host, c.Port, c.DB)
}

type rabbitmqConfig struct {
	Host     string
	Port     string
	VHost    string
	Exchange string
	User     string
	Password string
}

func (c rabbitmqConfig) URL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s%s", c.User, c.Password, c.Host, c.Port, c.VHost)
}

type httpConfig struct {
	Host string
	Port string
}

func (c httpConfig) Addr() string {
	return fmt.Sprintf("%s:%s", c.Host, c.Port)
}
