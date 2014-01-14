package main

type Config struct {
	MySQL struct {
		User     string
		Password string
		Host     string
		Port     string
		Db       string
		Table    string
	}
	RabbitMQ struct {
		User     string
		Password string
		Host     string
		Port     string
		VHost    string
		Exchange string
	}
	HTTP struct {
		Host string
		Port string
	}
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
