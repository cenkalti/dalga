package dalga

import (
	"fmt"

	gcfg "gopkg.in/gcfg.v1"
)

var DefaultConfig = Config{
	Jobs: jobsConfig{
		RetryInterval: 60,
	},
	MySQL: mysqlConfig{
		Host:         "127.0.0.1",
		Port:         3306,
		DB:           "test",
		Table:        "dalga",
		User:         "root",
		Password:     "",
		MaxOpenConns: 50,
		SkipLocked:   true,
	},
	Listen: listenConfig{
		Host: "127.0.0.1",
		Port: 34006,
	},
	Endpoint: endpointConfig{
		BaseURL: "http://127.0.0.1:5000/",
		Timeout: 10,
	},
}

type Config struct {
	Jobs     jobsConfig
	MySQL    mysqlConfig
	Listen   listenConfig
	Endpoint endpointConfig
}

func (c *Config) LoadFromFile(filename string) error {
	return gcfg.ReadFileInto(c, filename)
}

type jobsConfig struct {
	RandomizationFactor float64
	RetryInterval       int
}

type mysqlConfig struct {
	Host         string
	Port         int
	DB           string
	Table        string
	User         string
	Password     string
	MaxOpenConns int
	SkipLocked   bool
}

func (c mysqlConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true", c.User, c.Password, c.Host, c.Port, c.DB)
}

type listenConfig struct {
	Host string
	Port int
}

func (c listenConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type endpointConfig struct {
	BaseURL string
	Timeout int
}
