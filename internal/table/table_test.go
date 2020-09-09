package table

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/senseyeio/duration"

	"github.com/cenkalti/dalga/v3/internal/clock"
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

	table := New(db, "test_jobs")
	if err := table.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := table.Create(ctx); err != nil {
		t.Fatal(err)
	}
	table.SkipLocked = false
	table.FixedIntervals = true

	table.Clk = clock.New(now)
	j, err := table.AddJob(ctx, Key{
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
	if err := table.UpdateInstance(ctx, instanceID); err != nil {
		t.Fatal(err)
	}

	_, err = table.Front(ctx, instanceID)
	if err != sql.ErrNoRows {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	t.Run("Get returns timezoned job", func(t *testing.T) {
		j, err = table.Get(ctx, "abc", "def")
		if err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		if expect, found := firstRun.Format(time.RFC3339), j.NextRun.Time.Format(time.RFC3339); expect != found {
			t.Fatalf("expected first run '%s' but found '%s'", expect, found)
		}
	})

	table.Clk.Add(time.Minute * 31)

	j, err = table.Front(ctx, instanceID)
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
		if err := table.UpdateNextRun(ctx, j.Key, 0, nil); err != nil {
			t.Fatal(err)
		}
		table.Clk.Set(j.Interval.Shift(table.Clk.Get()).Add(time.Minute))
		if _, err := table.DisableJob(ctx, j.Key); err != nil {
			t.Fatal(err)
		}
		_, err := table.Front(ctx, instanceID)
		if err != sql.ErrNoRows {
			t.Fatalf("unexpected error: %s", err.Error())
		}
	})

	t.Run("Disabled jobs have no nextRun", func(t *testing.T) {
		j, err := table.Get(ctx, j.Key.Path, j.Key.Body)
		if err != nil {
			t.Fatal(err)
		}
		if j.NextRun.Valid {
			t.Fatalf("expected nextRun to be invalid: %v", j.NextRun)
		}
	})

	t.Run("Generic rescheduling won't re-enable a job", func(t *testing.T) {
		if err := table.UpdateNextRun(ctx, j.Key, 0, nil); err != nil {
			t.Fatal(err)
		}
		table.Clk.Set(j.Interval.Shift(table.Clk.Get()).Add(time.Minute))
		_, err = table.Front(ctx, instanceID)
		if err != sql.ErrNoRows {
			t.Fatalf("unexpected error: %s", err.Error())
		}
	})

	t.Run("Can re-enable", func(t *testing.T) {
		if _, err := table.EnableJob(ctx, j.Key); err != nil {
			t.Fatal(err)
		}
		_, err = table.Front(ctx, instanceID)
		if err != sql.ErrNoRows {
			t.Fatalf("unexpected error: %v", err)
		}
		table.Clk.Set(j.Interval.Shift(table.Clk.Get()).Add(time.Minute))
		j, err = table.Front(ctx, instanceID)
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
