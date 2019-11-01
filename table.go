package dalga

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type table struct {
	db         *sql.DB
	name       string
	stmtGet    *sql.Stmt
	stmtInsert *sql.Stmt
	stmtDelete *sql.Stmt
	stmtFront  *sql.Stmt
	stmtUpdate *sql.Stmt
	stmtCount  *sql.Stmt
}

const createTableSQL = "" +
	"CREATE TABLE `%s` (" +
	"  `path`        VARCHAR(255)    NOT NULL," +
	"  `body`        VARCHAR(255)    NOT NULL," +
	"  `interval`    INT UNSIGNED    NOT NULL," +
	"  `next_run`    DATETIME        NOT NULL," +
	"" +
	"  PRIMARY KEY (`path`, `body`)," +
	"  KEY `idx_next_run` (`next_run`)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8"

var (
	ErrExist    = errors.New("job already exists")
	ErrNotExist = errors.New("job does not exist")
)

func (t *table) Prepare() error {
	var err error
	t.stmtGet, err = t.db.Prepare("SELECT path, body, `interval`, next_run " +
		"FROM " + t.name + " " +
		"WHERE path = ? AND body = ?")
	if err != nil {
		return err
	}
	t.stmtInsert, err = t.db.Prepare("REPLACE INTO " + t.name +
		"(path, body, `interval`, next_run) " +
		"VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	t.stmtDelete, err = t.db.Prepare("DELETE FROM " + t.name + " " + "WHERE path = ? AND body = ?")
	if err != nil {
		return err
	}
	t.stmtFront, err = t.db.Prepare("SELECT path, body, `interval`, next_run " +
		"FROM " + t.name + " " +
		"ORDER BY next_run ASC LIMIT 1")
	if err != nil {
		return err
	}
	t.stmtUpdate, err = t.db.Prepare("UPDATE " + t.name + " " +
		"SET next_run=? " +
		"WHERE path = ? AND body = ?")
	if err != nil {
		return err
	}
	t.stmtCount, err = t.db.Prepare("SELECT COUNT(*) FROM " + t.name)
	if err != nil {
		return err
	}
	return nil
}

// Create jobs table.
func (t *table) Create() error {
	sql := fmt.Sprintf(createTableSQL, t.name)
	_, err := t.db.Exec(sql)
	return err
}

// Get returns a job with body and path from the table.
func (t *table) Get(path, body string) (*Job, error) {
	row := t.stmtGet.QueryRow(path, body)
	var j Job
	var interval uint32
	err := row.Scan(&j.Path, &j.Body, &interval, &j.NextRun)
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
	_, err := t.stmtInsert.Exec(j.Path, j.Body, j.Interval.Seconds(), j.NextRun)
	return err
}

// Delete the job from scheduler table.
func (t *table) Delete(path, body string) error {
	result, err := t.stmtDelete.Exec(path, body)
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
	row := t.stmtFront.QueryRow()
	var j Job
	var interval uint32
	err := row.Scan(&j.Path, &j.Body, &interval, &j.NextRun)
	if err != nil {
		return nil, err
	}
	j.Interval = time.Duration(interval) * time.Second
	return &j, nil
}

// UpdateNextRun sets next_run to now+interval.
func (t *table) UpdateNextRun(j *Job) error {
	_, err := t.stmtUpdate.Exec(j.NextRun, j.Path, j.Body)
	return err
}

// Count returns the count of scheduled jobs in the table.
func (t *table) Count() (int64, error) {
	var count int64
	return count, t.stmtCount.QueryRow().Scan(&count)
}
