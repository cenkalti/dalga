package table

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/cenkalti/dalga/internal/clock"
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

	now := time.Date(2020, time.August, 19, 11, 46, 0, 0, time.UTC)
	firstRun := now.Add(time.Minute * 30)

	table := New(db, "test_jobs")
	if err := table.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := table.Create(ctx); err != nil {
		t.Fatal(err)
	}

	table.Clk = clock.New(now)
	j, err := table.AddJob(ctx, Key{
		Path: "abc",
		Body: "def",
	}, time.Minute*60, time.Minute*30, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !j.NextRun.Equal(firstRun) {
		t.Fatalf("expected first run '%v' but found '%v'", firstRun, j.NextRun)
	}

	var instanceID uint32 = 123456
	if err := table.UpdateInstance(ctx, instanceID); err != nil {
		t.Fatal(err)
	}

	_, err = table.Front(ctx, instanceID)
	if err != sql.ErrNoRows {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	table.Clk.Add(time.Minute * 31)

	j, err = table.Front(ctx, instanceID)
	if err != nil {
		t.Fatal(err)
	}
	if j.Key.Path != "abc" || j.Key.Body != "def" {
		t.Fatalf("unexpected key %v", j.Key)
	}
}
