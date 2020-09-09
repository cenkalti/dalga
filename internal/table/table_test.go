package table_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/cenkalti/dalga/v3/internal/clock"
	"github.com/cenkalti/dalga/v3/internal/table"
	"github.com/senseyeio/duration"
)

var dsn = "root:@tcp(127.0.0.1:3306)/test?parseTime=true&multiStatements=true"

func TestAddJob(t *testing.T) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		t.Fatalf("cannot connect to mysql: %s", err.Error())
	}

	ctx := context.Background()

	now := time.Date(2020, time.August, 19, 11, 46, 0, 0, time.Local)
	firstRun := now.Add(time.Minute * 30)

	tbl := table.New(db, "test_jobs")
	if err := tbl.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Create(ctx); err != nil {
		t.Fatal(err)
	}
	tbl.SkipLocked = false
	tbl.FixedIntervals = true

	tbl.Clk = clock.New(now)
	j, err := tbl.AddJob(ctx, table.Key{
		Path: "abc",
		Body: "def",
	}, mustDuration("PT60M"), mustDuration("PT30M"), time.Local, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !j.NextRun.Time.Equal(firstRun) {
		t.Fatalf("expected first run '%v' but found '%v'", firstRun, j.NextRun)
	}
	t.Run("AddJob returns timezoned job", func(t *testing.T) {
		if expect, found := firstRun.Format(time.RFC3339), j.NextRun.Time.Format(time.RFC3339); expect != found {
			t.Fatalf("expected first run '%s' but found '%s'", expect, found)
		}
	})

	var instanceID uint32 = 123456
	if err := tbl.UpdateInstance(ctx, instanceID); err != nil {
		t.Fatal(err)
	}

	_, err = tbl.Front(ctx, instanceID)
	if err != sql.ErrNoRows {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	t.Run("Get returns timezoned job", func(t *testing.T) {
		j, err = tbl.Get(ctx, "abc", "def")
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		if expect, found := firstRun.Format(time.RFC3339), j.NextRun.Time.Format(time.RFC3339); expect != found {
			t.Fatalf("expected first run '%s' but found '%s'", expect, found)
		}
	})

	tbl.Clk.Add(time.Minute * 31)

	j, err = tbl.Front(ctx, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	if j.Key.Path != "abc" || j.Key.Body != "def" {
		t.Fatalf("unexpected key %v", j.Key)
	}
	t.Run("Front returns timezoned job", func(t *testing.T) {
		if expect, found := firstRun.Format(time.RFC3339), j.NextRun.Time.Format(time.RFC3339); expect != found {
			t.Fatalf("expected first run '%s' but found '%s'", expect, found)
		}
	})

	t.Run("Disable hides job", func(t *testing.T) {
		if err := tbl.UpdateNextRun(ctx, j.Key, 0, nil); err != nil {
			t.Fatal(err)
		}
		tbl.Clk.Set(j.Interval.Shift(tbl.Clk.Get()).Add(time.Minute))
		if _, err := tbl.DisableJob(ctx, j.Key); err != nil {
			t.Fatal(err)
		}
		_, err := tbl.Front(ctx, instanceID)
		if err != sql.ErrNoRows {
			t.Fatalf("unexpected error: %s", err.Error())
		}
	})

	t.Run("Disabled jobs have no nextRun", func(t *testing.T) {
		j, err := tbl.Get(ctx, j.Key.Path, j.Key.Body)
		if err != nil {
			t.Fatal(err)
		}
		if j.NextRun.Valid {
			t.Fatalf("expected nextRun to be invalid: %v", j.NextRun)
		}
	})

	t.Run("Generic rescheduling won't re-enable a job", func(t *testing.T) {
		if err := tbl.UpdateNextRun(ctx, j.Key, 0, nil); err != nil {
			t.Fatal(err)
		}
		tbl.Clk.Set(j.Interval.Shift(tbl.Clk.Get()).Add(time.Minute))
		_, err = tbl.Front(ctx, instanceID)
		if err != sql.ErrNoRows {
			t.Fatalf("unexpected error: %s", err.Error())
		}
	})

	t.Run("Can re-enable", func(t *testing.T) {
		if _, err := tbl.EnableJob(ctx, j.Key); err != nil {
			t.Fatal(err)
		}
		_, err = tbl.Front(ctx, instanceID)
		if err != sql.ErrNoRows {
			t.Fatalf("unexpected error: %v", err)
		}
		tbl.Clk.Set(j.Interval.Shift(tbl.Clk.Get()).Add(time.Minute))
		j, err = tbl.Front(ctx, instanceID)
		if err != nil {
			t.Fatal(err)
		}
		if j.Key.Path != "abc" || j.Key.Body != "def" {
			t.Fatalf("unexpected key %v", j.Key)
		}
	})
}

func mustDuration(s string) duration.Duration {
	d, err := duration.ParseISO8601(s)
	if err != nil {
		panic(err)
	}
	return d
}
