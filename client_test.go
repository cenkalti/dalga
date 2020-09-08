package dalga

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestClient performs basic functionality tests.
func TestClient(t *testing.T) {

	c := make(chan string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		buf.ReadFrom(r.Body)
		defer r.Body.Close()

		c <- buf.String()
		w.Write([]byte("OK"))
	}))
	defer srv.Close()

	config := DefaultConfig
	config.Endpoint.BaseURL = "http://" + srv.Listener.Addr().String() + "/"
	config.MySQL.SkipLocked = false
	config.Listen.Port = 34007
	config.MySQL.Table = "test_client"
	d, lis, cleanup := newDalga(t, config)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	defer func() {
		cancel()
		<-d.NotifyDone()
	}()

	callCtx := context.Background()

	clnt := NewClient("http://" + lis.Addr())

	t.Run("get nonexistent", func(t *testing.T) {
		_, err := clnt.Get(callCtx, "what", "who")
		if err != ErrNotExist {
			t.Fatal("expected ErrNotExist")
		}
	})

	t.Run("schedule", func(t *testing.T) {
		if j, err := clnt.Schedule(callCtx, "when", "where", MustWithIntervalString("PT1M")); err != nil {
			t.Fatal(err)
		} else if j.Body != "where" {
			t.Fatalf("unexpected body: %s", j.Body)
		}
	})

	t.Run("get", func(t *testing.T) {
		if j, err := clnt.Get(callCtx, "when", "where"); err != nil {
			t.Fatal(err)
		} else if j.Body != "where" {
			t.Fatalf("unexpected body: %s", j.Body)
		}
	})

	t.Run("can't disable nonexistent", func(t *testing.T) {
		if _, err := clnt.Disable(callCtx, "apple", "banana"); err != ErrNotExist {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("disable", func(t *testing.T) {
		if j, err := clnt.Disable(callCtx, "when", "where"); err != nil {
			t.Fatal(err)
		} else if j.NextRun != nil {
			t.Fatalf("unexpected next_run: %v", j.NextRun)
		}
	})

	t.Run("enable", func(t *testing.T) {
		if j, err := clnt.Enable(callCtx, "when", "where"); err != nil {
			t.Fatal(err)
		} else if j.NextRun == nil {
			t.Fatalf("unexpected next_run: %v", j.NextRun)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		if err := clnt.Cancel(callCtx, "when", "where"); err != nil {
			t.Fatal(err)
		}
		if _, err := clnt.Get(callCtx, "when", "where"); err != ErrNotExist {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("idempotent cancel", func(t *testing.T) {
		if err := clnt.Cancel(callCtx, "when", "where"); err != nil {
			t.Fatal(err)
		}
	})

}

func printJob(t *testing.T, j *Job) {
	t.Helper()
	var nextRun string
	if j.NextRun != nil {
		nextRun = *j.NextRun
	}
	t.Logf("Job: interval=%s, next_run=%s, next_sched=%s", j.Interval, nextRun, j.NextSched)
}

// TestEnableScheduling ensures that re-enabled jobs schedule their next run correctly.
func TestEnableScheduling(t *testing.T) {

	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2020, 8, 29, 15, 47, 0, 0, loc)

	tests := []struct {
		name        string
		fixed       bool
		retry       int
		start       time.Time
		firstRun    time.Time
		interval    string
		success     bool
		disableAt   time.Time
		enableAt    time.Time
		expectRunAt time.Time
		notes       string
	}{
		{
			name:        "brief-pause",
			fixed:       false,
			retry:       60,
			start:       start,
			firstRun:    start.Add(time.Hour),
			interval:    "PT5H",
			success:     true,
			disableAt:   start.Add(time.Hour * 2),
			enableAt:    start.Add(time.Hour * 3),
			expectRunAt: start.Add(time.Hour * 6),
			notes:       "should have no effect",
		},
		{
			name:        "brief-pause-fixed",
			fixed:       true,
			retry:       60,
			start:       start,
			firstRun:    start.Add(time.Hour),
			interval:    "PT5H",
			success:     true,
			disableAt:   start.Add(time.Hour * 2),
			enableAt:    start.Add(time.Hour * 3),
			expectRunAt: start.Add(time.Hour * 6),
			notes:       "should have no effect",
		},
		{
			name:        "brief-pause-during-retry",
			fixed:       false,
			retry:       60,
			start:       start,
			firstRun:    start.Add(time.Hour),
			interval:    "PT5H",
			success:     false,
			disableAt:   start.Add(time.Hour + time.Second*30),
			enableAt:    start.Add(time.Hour + time.Second*45),
			expectRunAt: start.Add(time.Hour),
			notes:       "should have no effect",
		},
		{
			name:        "brief-pause-during-retry-fixed",
			fixed:       true,
			retry:       60,
			start:       start,
			firstRun:    start.Add(time.Hour),
			interval:    "PT5H",
			success:     false,
			disableAt:   start.Add(time.Hour + time.Second*30),
			enableAt:    start.Add(time.Hour + time.Second*45),
			expectRunAt: start.Add(time.Hour * 6),
			notes:       "should cancel retries and reschedule",
		},
		{
			name:        "pass-over-schedule-point",
			fixed:       false,
			retry:       60,
			start:       start,
			firstRun:    start.Add(time.Hour),
			interval:    "PT5H",
			success:     true,
			disableAt:   start.Add(time.Hour * 2),
			enableAt:    start.Add(time.Hour * 7),
			expectRunAt: start.Add(time.Hour * 6),
			notes:       "should have run point in the past",
		},
		{
			name:        "pass-over-schedule-point-fixed",
			fixed:       true,
			retry:       60,
			start:       start,
			firstRun:    start.Add(time.Hour),
			interval:    "PT5H",
			success:     true,
			disableAt:   start.Add(time.Hour * 2),
			enableAt:    start.Add(time.Hour * 7),
			expectRunAt: start.Add(time.Hour * 11),
			notes:       "should reschedule for the next future point",
		},
	}

	config := DefaultConfig
	config.MySQL.SkipLocked = false
	config.Listen.Port = 34009
	config.MySQL.Table = "test_enable_scheduling"

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := make(chan string)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var buf bytes.Buffer
				buf.ReadFrom(r.Body)
				defer r.Body.Close()

				c <- buf.String()
				if test.success {
					w.Write([]byte("OK"))
				} else {
					http.Error(w, "failed", 400)
				}
			}))

			config.Endpoint.BaseURL = "http://" + srv.Listener.Addr().String() + "/"
			config.Jobs.FixedIntervals = test.fixed
			config.Jobs.RetryInterval = test.retry
			config.Jobs.RetryMaxInterval = test.retry
			config.Jobs.RetryMultiplier = 1

			d, lis, cleanup := newDalga(t, config)
			defer cleanup()

			t.Log("setting clock to:", test.start.String())
			clk := d.UseClock(test.start)

			runCtx, cancel := context.WithCancel(context.Background())
			go d.Run(runCtx)
			defer func() {
				cancel()
				<-d.NotifyDone()
			}()

			ctx := context.Background()
			clnt := NewClient("http://" + lis.Addr())

			t.Log("scheduling test job")
			j, err := clnt.Schedule(ctx, "abc", test.name,
				WithFirstRun(test.firstRun),
				MustWithIntervalString(test.interval),
				WithLocation(loc),
			)
			if err != nil {
				t.Fatal(err)
			}
			printJob(t, j)

			t.Log("setting clock to:", test.firstRun.Add(time.Second))
			clk.Set(test.firstRun.Add(time.Second))

			select {
			case body := <-c:
				if body != test.name {
					t.Fatalf("expected '%s' but found '%s'", test.name, body)
				}
			case <-time.After(time.Second * 3):
				t.Fatal("never received POST for 1st job execution")
			}
			<-time.After(time.Millisecond * 100)

			j, err = clnt.Get(ctx, j.Path, j.Body)
			if err != nil {
				t.Fatal(err)
			}
			printJob(t, j)

			t.Log("setting clock to:", test.disableAt.String())
			clk.Set(test.disableAt)
			t.Log("disabling test job")
			j, err = clnt.Disable(ctx, "abc", test.name)
			if err != nil {
				t.Fatal(err)
			}
			printJob(t, j)
			if j.NextRun != nil {
				t.Fatalf("unexpected next run: %s", *j.NextRun)
			}

			t.Log("setting clock to:", test.enableAt.String())
			clk.Set(test.enableAt)
			t.Log("disabling test job")
			j, err = clnt.Enable(ctx, "abc", test.name)
			if err != nil {
				t.Fatal(err)
			}
			printJob(t, j)

			if j.NextRun == nil {
				t.Fatalf("unexpected j.NextRun: %v", j.NextRun)
			}
			nextRun, err := time.Parse(time.RFC3339, *j.NextRun)
			if err != nil {
				t.Fatal(err)
			}
			if nextRun.After(test.expectRunAt.Add(time.Second)) || nextRun.Before(test.expectRunAt.Add(-time.Second)) {
				t.Fatalf("run at '%s' too different from expected value '%s'", nextRun.Format(time.RFC3339), test.expectRunAt.Format(time.RFC3339))
			}

		})
	}

}

// TestDisableRunningJob ensures that if a job is disabled after
// it starts executing, the rescheduling action that occurs when
// execution finishes will not inadvertently re-enable the job.
func TestDisableRunningJob(t *testing.T) {

	c1 := make(chan struct{})
	c2 := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c1 <- struct{}{}
		<-c2
		w.Write([]byte("OK"))
	}))

	config := DefaultConfig
	config.MySQL.SkipLocked = false
	config.Listen.Port = 34010
	config.MySQL.Table = "test_disable_running_job"
	config.Endpoint.BaseURL = "http://" + srv.Listener.Addr().String() + "/"

	d, lis, cleanup := newDalga(t, config)
	defer cleanup()

	runCtx, cancel := context.WithCancel(context.Background())
	go d.Run(runCtx)
	defer func() {
		cancel()
		<-d.NotifyDone()
	}()

	ctx := context.Background()
	clnt := NewClient("http://" + lis.Addr())

	_, err := clnt.Schedule(ctx, "alpha", "beta",
		WithFirstRun(time.Now().Add(time.Second)),
		MustWithIntervalString("PT1H"),
	)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-c1:
	case <-time.After(time.Second * 3):
		t.Fatal("never received POST")
	}

	j, err := clnt.Disable(ctx, "alpha", "beta")
	if err != nil {
		t.Fatal(err)
	}
	if j.NextRun != nil {
		t.Fatalf("unexpected nextRun: %v", *j.NextRun)
	}

	c2 <- struct{}{}
	<-time.After(time.Millisecond * 100)

	j, err = clnt.Get(ctx, "alpha", "beta")
	if err != nil {
		t.Fatal(err)
	}
	if j.NextRun != nil {
		t.Fatalf("unexpected nextRun: %v", *j.NextRun)
	}

}
