package dalga

import (
	"fmt"
	"net/url"
	"time"
)

// DefaultConfig contains sensible defaults for Dalga instance.
// For a simple deployment, you only need to override MySQL options.
var DefaultConfig = Config{
	Jobs: jobsConfig{
		RetryInterval:    time.Minute,
		RetryMultiplier:  1,
		RetryMaxInterval: time.Minute,
		ScanFrequency:    time.Second,
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
		Host:            "127.0.0.1",
		Port:            34006,
		ShutdownTimeout: 10 * time.Second,
	},
	Endpoint: endpointConfig{
		BaseURL: "http://127.0.0.1:5000/",
		Timeout: 10 * time.Second,
	},
}

// Config values for Dalga instance.
type Config struct {
	Jobs     jobsConfig
	MySQL    mysqlConfig
	Listen   listenConfig
	Endpoint endpointConfig
}

type jobsConfig struct {
	RandomizationFactor float64
	RetryInterval       time.Duration
	RetryMultiplier     float64
	RetryMaxInterval    time.Duration
	RetryStopAfter      time.Duration
	FixedIntervals      bool
	ScanFrequency       time.Duration
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
	v := url.Values{}
	v.Set("parseTime", "true")
	v.Set("transaction_isolation", "'READ-COMMITTED'")
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?", c.User, c.Password, c.Host, c.Port, c.DB) + v.Encode()
}

type listenConfig struct {
	Host            string
	Port            int
	ShutdownTimeout time.Duration
}

func (c listenConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type endpointConfig struct {
	BaseURL string
	Timeout time.Duration
}
