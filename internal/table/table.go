package table

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var ErrNotExist = errors.New("job does not exist")

type Table struct {
	db         *sql.DB
	name       string
	SkipLocked bool
}

func New(db *sql.DB, name string) *Table {
	return &Table{
		db:   db,
		name: name,
	}
}

// Create jobs table.
func (t *Table) Create(ctx context.Context) error {
	const createTableSQL = "" +
		"CREATE TABLE `%s` (" +
		"  `path`        VARCHAR(255)        NOT NULL," +
		"  `body`        VARCHAR(255)        NOT NULL," +
		"  `interval`    INT UNSIGNED        NOT NULL," +
		"  `next_run`    DATETIME            NOT NULL," +
		"  `instance_id` INT                 UNSIGNED," +
		"  PRIMARY KEY (`path`, `body`)," +
		"  KEY         (`next_run`)," +
		"  FOREIGN KEY (`instance_id`) REFERENCES `%s_instances` (`id`) ON DELETE SET NULL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	const createInstancesTableSQL = "" +
		"CREATE TABLE `%s_instances` (" +
		"  `id`          INT            UNSIGNED NOT NULL," +
		"  `updated_at`  DATETIME       NOT NULL," +
		"  PRIMARY KEY (`id`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	_, err := t.db.ExecContext(ctx, fmt.Sprintf(createInstancesTableSQL, t.name))
	if err != nil {
		return err
	}
	_, err = t.db.ExecContext(ctx, fmt.Sprintf(createTableSQL, t.name, t.name))
	return err
}

func (t *Table) Get(ctx context.Context, path, body string) (*Job, error) {
	s := "SELECT path, body, `interval`, next_run, instance_id " +
		"FROM " + t.name + " " +
		"WHERE path = ? AND body = ?"
	row := t.db.QueryRowContext(ctx, s, path, body)
	var j Job
	var interval uint32
	var instanceID sql.NullInt64
	err := row.Scan(&j.Path, &j.Body, &interval, &j.NextRun, &instanceID)
	if err == sql.ErrNoRows {
		return nil, ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	j.Interval = time.Duration(interval) * time.Second
	if instanceID.Valid {
		id := uint32(instanceID.Int64)
		j.InstanceID = &id
	}
	return &j, nil
}

// Insert the job to to scheduler table.
func (t *Table) AddJob(ctx context.Context, key Key, interval, delay time.Duration, nextRun time.Time) (*Job, error) {
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // nolint: errcheck
	if nextRun.IsZero() {
		s := "REPLACE INTO " + t.name + // nolint: gosec
			"(path, body, `interval`, next_run) " +
			"VALUES (?, ?, ?, UTC_TIMESTAMP() + INTERVAL ? SECOND)"
		_, err = tx.ExecContext(ctx, s, key.Path, key.Body, interval/time.Second, delay/time.Second)
	} else {
		s := "REPLACE INTO " + t.name + // nolint: gosec
			"(path, body, `interval`, next_run) " +
			"VALUES (?, ?, ?, ?)"
		_, err = tx.ExecContext(ctx, s, key.Path, key.Body, interval/time.Second, nextRun)
	}
	if err != nil {
		return nil, err
	}
	s := "SELECT next_run FROM " + t.name + " WHERE path=? AND body=?" // nolint: gosec
	row := tx.QueryRowContext(ctx, s, key.Path, key.Body)
	err = row.Scan(&nextRun)
	if err != nil {
		return nil, err
	}
	job := &Job{
		Key:      key,
		Interval: interval,
		NextRun:  nextRun,
	}
	return job, tx.Commit()
}

// Delete the job from scheduler table.
func (t *Table) DeleteJob(ctx context.Context, key Key) error {
	s := "DELETE FROM " + t.name + " WHERE path=? AND body=?" // nolint: gosec
	_, err := t.db.ExecContext(ctx, s, key.Path, key.Body)
	return err
}

// Front returns the next scheduled job from the table.
func (t *Table) Front(ctx context.Context, instanceID uint32) (*Job, error) {
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // nolint: errcheck
	s := "SELECT path, body, `interval`, next_run " +
		"FROM " + t.name + " " +
		"WHERE next_run < UTC_TIMESTAMP() " +
		"AND instance_id IS NULL " +
		"ORDER BY next_run ASC LIMIT 1 " +
		"FOR UPDATE"
	if t.SkipLocked {
		s += " SKIP LOCKED"
	}
	row := tx.QueryRowContext(ctx, s)
	var j Job
	var interval uint32
	err = row.Scan(&j.Path, &j.Body, &interval, &j.NextRun)
	if err != nil {
		return nil, err
	}
	j.Interval = time.Duration(interval) * time.Second
	s = "UPDATE " + t.name + " SET instance_id=? WHERE path=? AND body=?" // nolint: gosec
	_, err = tx.ExecContext(ctx, s, instanceID, j.Path, j.Body)
	if err != nil {
		return nil, err
	}
	return &j, tx.Commit()
}

// UpdateNextRun sets next_run to now+interval.
func (t *Table) UpdateNextRun(ctx context.Context, key Key, delay time.Duration) error {
	s := "UPDATE " + t.name + " " + // nolint: gosec
		"SET next_run=UTC_TIMESTAMP() + INTERVAL ? SECOND, instance_id=NULL " +
		"WHERE path = ? AND body = ?"
	_, err := t.db.ExecContext(ctx, s, delay/time.Second, key.Path, key.Body)
	return err
}

func (t *Table) UpdateInstanceID(ctx context.Context, key Key, instanceID uint32) error {
	s := "UPDATE " + t.name + " " + // nolint: gosec
		"SET instance_id=? " +
		"WHERE path = ? AND body = ?"
	_, err := t.db.ExecContext(ctx, s, instanceID, key.Path, key.Body)
	return err
}

// Count returns the count of scheduled jobs in the table.
func (t *Table) Count(ctx context.Context) (int64, error) {
	s := "SELECT COUNT(*) FROM " + t.name // nolint: gosec
	var count int64
	return count, t.db.QueryRowContext(ctx, s).Scan(&count)
}

// Pending returns the count of pending jobs in the table.
func (t *Table) Pending(ctx context.Context) (int64, error) {
	s := "SELECT COUNT(*) FROM " + t.name + " " + // nolint: gosec
		"WHERE next_run < UTC_TIMESTAMP()"
	var count int64
	return count, t.db.QueryRowContext(ctx, s).Scan(&count)
}

// Lag returns the number of seconds passed from the execution time of the oldest pending job.
func (t *Table) Lag(ctx context.Context) (int64, error) {
	s := "SELECT TIMESTAMPDIFF(SECOND, next_run, UTC_TIMESTAMP()) FROM " + t.name + " " + // nolint: gosec
		"WHERE next_run < UTC_TIMESTAMP() AND instance_id is NULL " +
		"ORDER BY next_run ASC LIMIT 1"
	var lag int64
	err := t.db.QueryRowContext(ctx, s).Scan(&lag)
	if err == sql.ErrNoRows {
		err = nil
	}
	return lag, err
}

// Running returns the count of total running jobs in the table.
func (t *Table) Running(ctx context.Context) (int64, error) {
	s := "SELECT COUNT(*) FROM " + t.name + " " + // nolint: gosec
		"WHERE instance_id IS NOT NULL"
	var count int64
	return count, t.db.QueryRowContext(ctx, s).Scan(&count)
}

// Instances returns the count of running Dalga instances.
func (t *Table) Instances(ctx context.Context) (int64, error) {
	s := "SELECT COUNT(*) FROM " + t.name + "_instances " // nolint: gosec
	var count int64
	return count, t.db.QueryRowContext(ctx, s).Scan(&count)
}

func (t *Table) UpdateInstance(ctx context.Context, id uint32) error {
	s := "INSERT INTO " + t.name + "_instances(id, updated_at) VALUES (" + strconv.FormatUint(uint64(id), 10) + ",UTC_TIMESTAMP()) ON DUPLICATE KEY UPDATE updated_at=UTC_TIMESTAMP()" // nolint: gosec
	s += ";DELETE FROM " + t.name + "_instances WHERE updated_at < UTC_TIMESTAMP() - INTERVAL 1 MINUTE"                                                                                // nolint: gosec
	_, err := t.db.ExecContext(ctx, s)
	return err
}

func (t *Table) DeleteInstance(ctx context.Context, id uint32) error {
	s := "DELETE FROM " + t.name + "_instances WHERE id=?" // nolint: gosec
	_, err := t.db.ExecContext(ctx, s, id)
	return err
}
