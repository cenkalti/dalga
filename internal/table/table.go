package table

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/cenkalti/dalga/v3/internal/clock"
	"github.com/cenkalti/dalga/v3/internal/retry"
	my "github.com/go-mysql/errors"
	"github.com/go-sql-driver/mysql"
	"github.com/senseyeio/duration"
)

const (
	maxRetries = 10
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

// Get returns a job from the scheduler table, whether or not it is disabled.
func (t *Table) Get(ctx context.Context, path, body string) (*Job, error) {
	s := "SELECT path, body, `interval`, location, next_run, next_sched, instance_id " +
		"FROM " + t.name + " " +
		"WHERE path = ? AND body = ?"
	row := t.db.QueryRowContext(ctx, s, path, body)
	j, _, err := t.scanJob(row, false)
	return &j, err
}

func (t *Table) getForUpdate(ctx context.Context, path, body string) (tx *sql.Tx, j Job, now time.Time, err error) {
	tx, err = t.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer func() {
		if err != nil && tx != nil {
			tx.Rollback() // nolint: errcheck
		}
	}()
	s := "SELECT path, body, `interval`, location, next_run, next_sched, instance_id, IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP()) " +
		"FROM " + t.name + " " +
		"WHERE path = ? AND body = ? FOR UPDATE"
	row := tx.QueryRowContext(ctx, s, t.Clk.NowUTC(), path, body)
	j, now, err = t.scanJob(row, true)
	return
}

func (t *Table) scanJob(row *sql.Row, withCurrentTime bool) (j Job, now time.Time, err error) {
	var interval, locationName string
	var instanceID sql.NullInt64
	if withCurrentTime {
		err = row.Scan(&j.Path, &j.Body, &interval, &locationName, &j.NextRun, &j.NextSched, &instanceID, &now)
	} else {
		err = row.Scan(&j.Path, &j.Body, &interval, &locationName, &j.NextRun, &j.NextSched, &instanceID)
	}
	if err == sql.ErrNoRows {
		err = ErrNotExist
	}
	if err != nil {
		return
	}
	err = j.setLocation(locationName)
	if err != nil {
		return
	}
	if interval != "" {
		j.Interval, err = duration.ParseISO8601(interval)
		if err != nil {
			return
		}
	}
	if instanceID.Valid {
		id := uint32(instanceID.Int64)
		j.InstanceID = &id
	}
	now = now.In(j.Location)
	return
}

// AddJob inserts a job into the scheduler table.
func (t *Table) AddJob(ctx context.Context, key Key, interval, delay duration.Duration, location *time.Location, nextRun time.Time) (*Job, error) {
	if location == nil {
		location = time.UTC
	}
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
	s := "REPLACE INTO " + t.name +
		"(path, body, `interval`, location, next_run, next_sched) " +
		"VALUES (?, ?, ?, ?, ?, ?)"
	var locationName string
	if location != time.UTC {
		locationName = location.String()
	}
	var intervalString string
	if !interval.IsZero() {
		intervalString = interval.String()
	}
	_, err = tx.ExecContext(ctx, s, key.Path, key.Body, intervalString, locationName, nextRun.UTC(), nextRun.UTC())
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

// EnableJob marks the job as enabled by setting next_run to next_sched.
//
// If next_sched is in the past, the job will then be picked up for execution immediately.
//
// With FixedIntervals enabled, next_sched is advanced by the value of interval
// until it's in the future and next_run matches it.
func (t *Table) EnableJob(ctx context.Context, key Key) (*Job, error) {
	tx, j, now, err := t.getForUpdate(ctx, key.Path, key.Body)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // nolint: errcheck
	if j.Enabled() {
		// Job is already enabled.
		return &j, nil
	}
	if t.FixedIntervals {
		for j.NextSched.Before(now) {
			j.NextSched = j.Interval.Shift(j.NextSched)
		}
	}
	j.NextRun.Time = j.NextSched
	j.NextRun.Valid = true
	s := "UPDATE " + t.name + " " +
		"SET next_run=?, next_sched=? " +
		"WHERE path = ? AND body = ?"
	_, err = tx.ExecContext(ctx, s, j.NextRun.Time.UTC(), j.NextSched.UTC(), key.Path, key.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to set next run: %w", err)
	}
	return &j, tx.Commit()
}

// DisableJob prevents a job from running by setting next_run to NULL,
// while preserving the value of next_sched.
func (t *Table) DisableJob(ctx context.Context, key Key) (*Job, error) {
	tx, j, _, err := t.getForUpdate(ctx, key.Path, key.Body)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // nolint: errcheck
	if !j.Enabled() {
		// Job is already disabled.
		return &j, nil
	}
	if !t.FixedIntervals {
		// Disabling then enabling the job has a side effect of resetting exponential backoff to initial value.
		// This is not a problem if RetryMultiplier=1.
		j.NextSched = j.NextRun.Time
	}
	s := "UPDATE " + t.name + " SET next_run=NULL, next_sched=? WHERE path = ? AND body = ?"
	_, err = tx.ExecContext(ctx, s, j.NextSched, key.Path, key.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to set next run: %w", err)
	}
	j.NextRun.Valid = false
	return &j, tx.Commit()
}

// DeleteJob removes a job from scheduler table.
func (t *Table) DeleteJob(ctx context.Context, key Key) error {
	s := "DELETE FROM " + t.name + " WHERE path=? AND body=?"
	_, err := t.db.ExecContext(ctx, s, key.Path, key.Body)
	return err
}

// Front returns the next scheduled job from the table,
// based on the value of next_run, and claims it for the calling instance.
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
	if interval != "" {
		j.Interval, err = duration.ParseISO8601(interval)
		if err != nil {
			return nil, err
		}
	}
	err = j.setLocation(locationName)
	if err != nil {
		return nil, err
	}
	s = "UPDATE " + t.name + " SET instance_id=? WHERE path=? AND body=?"
	_, err = tx.ExecContext(ctx, s, instanceID, j.Path, j.Body)
	if err != nil {
		return nil, err
	}
	return &j, tx.Commit()
}

// UpdateNextRun sets next_run and next_sched, and unclaims it from an instance.
//
// With default settings, next_sched and next_run are set to now+delay.
//
// With FixedIntervals enabled, next_sched is advanced by the value of interval
// until it's in the future and next_run matches it.
//
// If this is a retry, next_run is set to a value based on retry parameters and next_sched is not adjusted.
//
// If UpdateNextRun is called on a disabled job, as many happen when a job has
// been disabled during execution, next_sched will advance but next_run will remain NULL.
func (t *Table) UpdateNextRun(ctx context.Context, key Key, randFactor float64, retryParams *retry.Retry) error {
	return withRetries(maxRetries, func() error {
		return t.updateNextRun(ctx, key, randFactor, retryParams)
	})
}

func (t *Table) updateNextRun(ctx context.Context, key Key, randFactor float64, retryParams *retry.Retry) error {
	tx, j, now, err := t.getForUpdate(ctx, key.Path, key.Body)
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint: errcheck
	switch {
	case retryParams != nil:
		j.NextRun.Time = retryParams.NextRun(j.NextSched, now)
		if j.NextRun.Time.IsZero() {
			j.NextRun.Valid = false
		}
	case t.FixedIntervals:
		for j.NextSched.Before(now) {
			j.NextSched = j.Interval.Shift(j.NextSched)
		}
		j.NextRun.Time = j.NextSched
	case !t.FixedIntervals:
		j.NextSched = j.Interval.Shift(now)
		if randFactor > 0 {
			diff := randomize(j.NextSched.Sub(now), randFactor)
			j.NextSched = now.Add(diff)
		}
		j.NextRun.Time = j.NextSched
	}
	s := "UPDATE " + t.name + " " +
		"SET next_run=?, next_sched=?, instance_id=NULL " +
		"WHERE path = ? AND body = ?"
	// Note that we are passing next_run as sql.NullTime value.
	// If next_run is already NULL (j.NextRun.Valid == false), it is not going to be updated.
	// This may happen when the job gets disabled while it is running.
	_, err = tx.ExecContext(ctx, s, j.NextRun, j.NextSched.UTC(), key.Path, key.Body)
	if err != nil {
		return fmt.Errorf("failed to set next run: %w", err)
	}
	return tx.Commit()
}

// UpdateInstanceID claims a job for an instance.
func (t *Table) UpdateInstanceID(ctx context.Context, key Key, instanceID uint32) error {
	s := "UPDATE " + t.name + " " +
		"SET instance_id=? " +
		"WHERE path = ? AND body = ?"
	_, err := t.db.ExecContext(ctx, s, instanceID, key.Path, key.Body)
	return err
}

// Count returns the count of scheduled jobs in the table.
func (t *Table) Count(ctx context.Context) (int64, error) {
	s := "SELECT COUNT(*) FROM " + t.name
	var count int64
	return count, t.db.QueryRowContext(ctx, s).Scan(&count)
}

// Pending returns the count of pending jobs in the table.
func (t *Table) Pending(ctx context.Context) (int64, error) {
	s := "SELECT COUNT(*) FROM " + t.name + " " +
		"WHERE next_run < IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP())"
	var count int64
	return count, t.db.QueryRowContext(ctx, s, t.Clk.NowUTC()).Scan(&count)
}

// Lag returns the number of seconds passed from the execution time of the oldest pending job.
func (t *Table) Lag(ctx context.Context) (int64, error) {
	s := "SELECT TIMESTAMPDIFF(SECOND, next_run, IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP())) FROM " + t.name + " " +
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
	s := "SELECT COUNT(*) FROM " + t.name + " " +
		"WHERE instance_id IS NOT NULL"
	var count int64
	return count, t.db.QueryRowContext(ctx, s).Scan(&count)
}

// Instances returns the count of running Dalga instances.
func (t *Table) Instances(ctx context.Context) (int64, error) {
	s := "SELECT COUNT(*) FROM " + t.name + "_instances "
	var count int64
	return count, t.db.QueryRowContext(ctx, s).Scan(&count)
}

// UpdateInstance adds the instance to the list of active instances,
// and clears out any inactive instances from the list, such as
// instances that were unable to call DeleteInstance during shutdown.
func (t *Table) UpdateInstance(ctx context.Context, id uint32) error {
	now := t.Clk.NowUTC()
	s1 := "INSERT INTO " + t.name + "_instances(id, updated_at) VALUES (" + strconv.FormatUint(uint64(id), 10) + ",IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP())) ON DUPLICATE KEY UPDATE updated_at=IFNULL(?, UTC_TIMESTAMP())"
	if _, err := t.db.ExecContext(ctx, s1, now, now); err != nil {
		return err
	}
	s2 := "DELETE FROM " + t.name + "_instances WHERE updated_at < IFNULL(CAST(? as DATETIME), UTC_TIMESTAMP) - INTERVAL 1 MINUTE"
	if _, err := t.db.ExecContext(ctx, s2, now); err != nil {
		return err
	}
	return nil
}

// DeleteInstance removes an entry from the list of active instances.
func (t *Table) DeleteInstance(ctx context.Context, id uint32) error {
	s := "DELETE FROM " + t.name + "_instances WHERE id=?"
	_, err := t.db.ExecContext(ctx, s, id)
	return err
}

func randomize(d time.Duration, f float64) time.Duration {
	delta := time.Duration(f * float64(d))
	return d - delta + time.Duration(float64(2*delta)*rand.Float64()) // nolint: gosec
}

func withRetries(retryCount int, fn func() error) (err error) {
	for attempt := 0; attempt < retryCount; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if dur := mysqlRetryInterval(err); dur > 0 {
			time.Sleep(dur)
			continue
		}
		break
	}
	return
}

func mysqlRetryInterval(err error) time.Duration {
	if ok, myerr := my.Error(err); ok { // MySQL error
		if my.MySQLErrorCode(err) == 1213 { // deadlock
			return time.Millisecond * 10
		}
		if my.CanRetry(myerr) {
			return time.Second
		}
	}
	return 0
}
