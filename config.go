package main

type Config struct {
	MySQL    mysqlConfig
	RabbitMQ rabbitmqConfig
	HTTP     httpConfig
}

// NewConfig returns a pointer to a newly created Config initialized with default parameters.
func NewConfig() *Config {
	c := &Config{}
	c.MySQL.User = "root"
	c.MySQL.Host = "localhost"
	c.MySQL.Port = "3306"
	c.MySQL.Db = "test"
	c.MySQL.Table = "dalga"
	c.RabbitMQ.User = "guest"
	c.RabbitMQ.Password = "guest"
	c.RabbitMQ.Host = "localhost"
	c.RabbitMQ.Port = "5672"
	c.RabbitMQ.VHost = "/"
	c.HTTP.Host = "0.0.0.0"
	c.HTTP.Port = "17500"
	return c
}

type mysqlConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	Db       string
	Table    string
}

func (c mysqlConfig) DSN() string {
	return c.User + ":" + c.Password + "@" + "tcp(" + c.Host + ":" + c.Port + ")/" + c.Db + "?parseTime=true"
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
	return "amqp://" + c.User + ":" + c.Password + "@" + c.Host + ":" + c.Port + c.VHost
}

type httpConfig struct {
	Host string
	Port string
}

func (c httpConfig) Addr() string {
	return c.Host + ":" + c.Port
}
