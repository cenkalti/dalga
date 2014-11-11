package dalga

import (
	"database/sql"
	"fmt"
	"time"
)

type table struct {
	db   *sql.DB
	name string
}

const createTableSQL = "" +
	"CREATE TABLE `%s` (" +
	"  `job`         VARCHAR(65535)  NOT NULL," +
	"  `routing_key` VARCHAR(255)    NOT NULL," +
	"  `interval`    INT UNSIGNED    NOT NULL," +
	"  `next_run`    DATETIME        NOT NULL," +
	"" +
	"  PRIMARY KEY (`job`, `routing_key`)," +
	"  KEY `idx_next_run` (`next_run`)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8"

func (t *table) Create() error {
	sql := fmt.Sprintf(createTableSQL, t.name)
	_, err := t.db.Exec(sql)
	return err
}

// Insert job to to scheduler table.
func (t *table) Insert(j *Job) error {
	interval := j.Interval.Seconds()
	_, err := t.db.Exec("INSERT INTO "+t.name+" "+
		"(job, routing_key, `interval`, next_run) "+
		"VALUES(?, ?, ?, ?) "+
		"ON DUPLICATE KEY UPDATE "+
		"next_run=DATE_ADD(next_run, INTERVAL (? - `interval`) SECOND), "+
		"`interval`=?",
		j.Description, j.RoutingKey, interval, j.NextRun, interval, interval)
	return err
}

// Delete the job from scheduler table.
func (t *table) Delete(job, routingKey string) error {
	_, err := t.db.Exec("DELETE FROM "+t.name+" "+"WHERE job=? AND routing_key=?", job, routingKey)
	return err
}

// Front returns the next scheduled job from the table.
func (t *table) Front() (*Job, error) {
	var interval uint32
	var j Job
	row := t.db.QueryRow("SELECT job, routing_key, `interval`, next_run " +
		"FROM " + t.name + " " +
		"ORDER BY next_run ASC LIMIT 1")
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
