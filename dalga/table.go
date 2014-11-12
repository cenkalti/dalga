package dalga

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/dalga/vendor/github.com/go-sql-driver/mysql"
)

type table struct {
	db   *sql.DB
	name string
}

const createTableSQL = "" +
	"CREATE TABLE `%s` (" +
	"  `routing_key` VARCHAR(255)    NOT NULL," +
	"  `job`         VARCHAR(255)    NOT NULL," +
	"  `interval`    INT UNSIGNED    NOT NULL," +
	"  `next_run`    DATETIME        NOT NULL," +
	"" +
	"  PRIMARY KEY (`routing_key`, `job`)," +
	"  KEY `idx_next_run` (`next_run`)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8"

var (
	ErrExist    = errors.New("job already exists")
	ErrNotExist = errors.New("job does not exist")
)

// Create jobs table.
func (t *table) Create() error {
	sql := fmt.Sprintf(createTableSQL, t.name)
	_, err := t.db.Exec(sql)
	return err
}

// Get returns a job with description and routingKey from the table.
func (t *table) Get(description, routingKey string) (*Job, error) {
	row := t.db.QueryRow("SELECT job, routing_key, `interval`, next_run "+
		"FROM "+t.name+" "+
		"WHERE job = ? AND routing_key = ?",
		description, routingKey)
	var j Job
	var interval uint32
	err := row.Scan(&j.Description, &j.RoutingKey, &interval, &j.NextRun)
	if err == sql.ErrNoRows {
		return nil, ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	j.Interval = time.Duration(interval) * time.Second
	return &j, nil
}

// Insert the job to to scheduler table.
func (t *table) Insert(j *Job) error {
	_, err := t.db.Exec("INSERT INTO "+t.name+
		"(job, routing_key, `interval`, next_run) "+
		"VALUES (?, ?, ?, ?)",
		j.Description, j.RoutingKey, j.Interval.Seconds(), j.NextRun)
	if myErr, ok := err.(*mysql.MySQLError); ok && myErr.Number == 1062 {
		return ErrExist
	}
	return err
}

// Delete the job from scheduler table.
func (t *table) Delete(description, routingKey string) error {
	result, err := t.db.Exec("DELETE FROM "+t.name+" "+"WHERE job=? AND routing_key=?", description, routingKey)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotExist
	}
	return nil
}

// Front returns the next scheduled job from the table.
func (t *table) Front() (*Job, error) {
	row := t.db.QueryRow("SELECT job, routing_key, `interval`, next_run " +
		"FROM " + t.name + " " +
		"ORDER BY next_run ASC LIMIT 1")
	var j Job
	var interval uint32
	err := row.Scan(&j.Description, &j.RoutingKey, &interval, &j.NextRun)
	if err != nil {
		return nil, err
	}
	j.Interval = time.Duration(interval) * time.Second
	return &j, nil
}

// UpdateNextRun sets next_run to now+interval.
func (t *table) UpdateNextRun(j *Job) error {
	_, err := t.db.Exec("UPDATE "+t.name+" "+
		"SET next_run=? "+
		"WHERE job=? AND routing_key=?",
		time.Now().UTC().Add(j.Interval), j.Description, j.RoutingKey)
	return err
}
