package dalga

import (
	"fmt"

	"github.com/cenkalti/dalga/dalga/Godeps/_workspace/src/code.google.com/p/gcfg"
)

var DefaultConfig = Config{
	MySQL: mysqlConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		DB:       "test",
		Table:    "dalga",
		User:     "root",
		Password: "",
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
	MySQL    mysqlConfig
	Listen   listenConfig
	Endpoint endpointConfig
	Redis    redisConfig
}

func (c *Config) LoadFromFile(filename string) error {
	return gcfg.ReadFileInto(c, filename)
}

type mysqlConfig struct {
	Host     string
	Port     int
	DB       string
	Table    string
	User     string
	Password string
}

func (c mysqlConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", c.User, c.Password, c.Host, c.Port, c.DB)
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

type redisConfig struct {
	Host string
	Port int
	DB   int
}

func (c redisConfig) Zero() bool {
	zero := redisConfig{}
	return c == zero
}

func (c redisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
