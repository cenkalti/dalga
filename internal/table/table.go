package table

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/senseyeio/duration"

	"github.com/cenkalti/dalga/v2/internal/clock"
)

var ErrNotExist = errors.New("job does not exist")

type Table struct {
	db             *sql.DB
	name           string
	SkipLocked     bool
	FixedIntervals bool
	Clk            *clock.Clock
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
		"  `interval`    VARCHAR(255)        NOT NULL," +
		"  `location`    VARCHAR(255)        NOT NULL," +
		"  `next_run`    DATETIME                NULL," +
		"  `next_sched`  DATETIME            NOT NULL," +
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

func (t *Table) Drop(ctx context.Context) error {
	dropSQL := "DROP TABLE " + t.name
	_, err := t.db.Exec(dropSQL)
	if err != nil {
		if myErr, ok := err.(*mysql.MySQLError); !ok || myErr.Number != 1051 { // Unknown table
			return err
		}
	}
	dropSQL = "DROP TABLE " + t.name + "_instances"
	_, err = t.db.Exec(dropSQL)
	if err != nil {
		if myErr, ok := err.(*mysql.MySQLError); !ok || myErr.Number != 1051 { // Unknown table
			return err
		}
	}
	return nil
}

func (t *Table) Get(ctx context.Context, path, body string) (*Job, error) {
	s := "SELECT path, body, `interval`, location, next_run, next_sched, instance_id " +
		"FROM " + t.name + " " +
		"WHERE path = ? AND body = ?"
	row := t.db.QueryRowContext(ctx, s, path, body)
	var j Job
	var interval, locationName string
	var instanceID sql.NullInt64
	err := row.Scan(&j.Path, &j.Body, &interval, &locationName, &j.NextRun, &j.NextSched, &instanceID)
	if err == sql.ErrNoRows {
		return nil, ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	if locationName == "" {
		locationName = time.UTC.String() // Default to UTC in case it's omitted somehow in the database.
	}
	j.Location, err = time.LoadLocation(locationName)
	if err != nil {
		return nil, err
	}
	j.NextRun.Time = j.NextRun.Time.In(j.Location)
	j.NextSched = j.NextSched.In(j.Location)
	j.Interval, err = duration.ParseISO8601(interval)
	if err != nil {
		return nil, err
	}
	if instanceID.Valid {
		id := uint32(instanceID.Int64)
		j.InstanceID = &id
	}
	return &j, nil
}

// Insert the job to to scheduler table.
func (t *Table) AddJob(ctx context.Context, key Key, interval, delay duration.Duration, location *time.Location, nextRun time.Time) (*Job, error) {
	log.Printf("Adding job in location: %v", location)
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // nolint: errcheck
	if nextRun.IsZero() {
		row := tx.QueryRowContext(ctx, "SELECT IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP())", t.Clk.NowUTC())
		var now time.Time
		if err := row.Scan(&now); err != nil {
			return nil, err
		}
		now = now.In(location)
		nextRun = delay.Shift(now)
	}
	s := "REPLACE INTO " + t.name + // nolint: gosec
		"(path, body, `interval`, location, next_run, next_sched) " +
		"VALUES (?, ?, ?, ?, ?, ?)"
	_, err = tx.ExecContext(ctx, s, key.Path, key.Body, interval.String(), location.String(), nextRun.UTC(), nextRun.UTC())
	if err != nil {
		return nil, err
	}
	job := &Job{
		Key:       key,
		Interval:  interval,
		Location:  location,
		NextRun:   sql.NullTime{Valid: true, Time: nextRun},
		NextSched: nextRun,
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
	s := "SELECT path, body, `interval`, location, next_run, next_sched " +
		"FROM " + t.name + " " +
		"WHERE next_run < IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP()) " +
		"AND instance_id IS NULL " +
		"ORDER BY next_run ASC LIMIT 1 " +
		"FOR UPDATE"
	if t.SkipLocked {
		s += " SKIP LOCKED"
	}
	row := tx.QueryRowContext(ctx, s, t.Clk.NowUTC())
	var j Job
	var interval, locationName string
	err = row.Scan(&j.Path, &j.Body, &interval, &locationName, &j.NextRun, &j.NextSched)
	if err != nil {
		return nil, err
	}
	j.Interval, err = duration.ParseISO8601(interval)
	if err != nil {
		return nil, err
	}
	if locationName == "" {
		locationName = time.UTC.String() // Default to UTC in case it's omitted somehow in the database.
	}
	j.Location, err = time.LoadLocation(locationName)
	if err != nil {
		return nil, err
	}
	j.NextRun.Time = j.NextRun.Time.In(j.Location)
	j.NextSched = j.NextSched.In(j.Location)
	s = "UPDATE " + t.name + " SET instance_id=? WHERE path=? AND body=?" // nolint: gosec
	_, err = tx.ExecContext(ctx, s, instanceID, j.Path, j.Body)
	if err != nil {
		return nil, err
	}
	return &j, tx.Commit()
}

// UpdateNextRun sets next_run to now+interval.
func (t *Table) UpdateNextRun(ctx context.Context, key Key, delay duration.Duration, randFactor float64, isRetry bool, isResume bool) error {
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var locationName string
	var now, nextSched time.Time
	var nextRun sql.NullTime
	s := "SELECT location, next_run, next_sched, IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP())" +
		" FROM " + t.name + " WHERE path = ? AND body = ?"
	row := tx.QueryRowContext(ctx, s, t.Clk.NowUTC(), key.Path, key.Body)
	if err := row.Scan(&locationName, &nextRun, &nextSched, &now); err != nil {
		return fmt.Errorf("failed to get last run of job: %w", err)
	}
	if locationName == "" {
		locationName = time.UTC.String() // Default to UTC in case it's omitted somehow in the database.
	}
	location, err := time.LoadLocation(locationName)
	if err != nil {
		return err
	}
	now = now.In(location)
	nextSched = nextSched.In(location)
	switch {
	case !t.FixedIntervals:
		nextSched = delay.Shift(now)
		if randFactor > 0 {
			diff := randomize(nextSched.Sub(now), randFactor)
			nextSched = now.Add(diff)
		}
	case t.FixedIntervals && !isRetry:
		for nextSched.Before(now) {
			nextSched = delay.Shift(nextSched)
		}
	}
	switch {
	case !nextRun.Valid && !isResume:
	case t.FixedIntervals && isRetry:
		nextRun.Time = delay.Shift(now).UTC()
	default:
		nextRun.Time = nextSched.UTC()
	}
	s = "UPDATE " + t.name + " " + // nolint: gosec
		"SET next_run=?, next_sched=?, instance_id=NULL, location=? " +
		"WHERE path = ? AND body = ?"
	_, err = tx.ExecContext(ctx, s, nextRun, nextSched.UTC(), locationName, key.Path, key.Body)
	if err != nil {
		return fmt.Errorf("failed to set next run: %w", err)
	}
	return tx.Commit()
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
		"WHERE next_run < IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP())"
	var count int64
	return count, t.db.QueryRowContext(ctx, s, t.Clk.NowUTC()).Scan(&count)
}

// Lag returns the number of seconds passed from the execution time of the oldest pending job.
func (t *Table) Lag(ctx context.Context) (int64, error) {
	s := "SELECT TIMESTAMPDIFF(SECOND, next_run, IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP())) FROM " + t.name + " " + // nolint: gosec
		"WHERE next_run < IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP()) AND instance_id is NULL " +
		"ORDER BY next_run ASC LIMIT 1"
	now := t.Clk.NowUTC()
	var lag int64
	err := t.db.QueryRowContext(ctx, s, now, now).Scan(&lag)
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
	// The MySQL driver doesn't support multiple statements in a single Exec if they contain placeholders.
	// That's why we use a transaction.
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint: errcheck
	now := t.Clk.NowUTC()
	s1 := "INSERT INTO " + t.name + "_instances(id, updated_at) VALUES (" + strconv.FormatUint(uint64(id), 10) + ",IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP())) ON DUPLICATE KEY UPDATE updated_at=IFNULL(?, UTC_TIMESTAMP())" // nolint: gosec
	if _, err = tx.ExecContext(ctx, s1, now, now); err != nil {
		return err
	}
	s2 := "DELETE FROM " + t.name + "_instances WHERE updated_at < IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP) - INTERVAL 1 MINUTE" // nolint: gosec
	if _, err = tx.ExecContext(ctx, s2, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (t *Table) DeleteInstance(ctx context.Context, id uint32) error {
	s := "DELETE FROM " + t.name + "_instances WHERE id=?" // nolint: gosec
	_, err := t.db.ExecContext(ctx, s, id)
	return err
}

func randomize(d time.Duration, f float64) time.Duration {
	delta := time.Duration(f * float64(d))
	return d - delta + time.Duration(float64(2*delta)*rand.Float64())
}
