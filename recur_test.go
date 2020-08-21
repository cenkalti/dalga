package dalga

// These test cases are adapted from job_recurring_test.go in https://github.com/ajvb/kala

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// This test works by using a series of checkpoints, spaced <interval> apart.
// A job is scheduled 5 seconds after the first checkpoint.
// By moving the clock to each checkpoint, and then 6 seconds later,
// you can verify that the job hasn't run between the two checkpoints,
// and only runs at the scheduled point.
//
// This is useful for ensuring that durations behave correctly on a grand scale.
func TestRecur(t *testing.T) {

	tableTests := []struct {
		Name        string
		Location    string
		Start       string
		Interval    string
		Checkpoints []string
	}{
		{
			Name:     "Daily",
			Location: "America/Los_Angeles",
			Start:    "2020-Jan-13 14:09",
			Interval: "P1D",
			Checkpoints: []string{
				"2020-Jan-14 14:09",
				"2020-Jan-15 14:09",
				"2020-Jan-16 14:09",
			},
		},
		{
			Name:     "Daily across DST boundary",
			Location: "America/Los_Angeles",
			Start:    "2020-Mar-05 14:09",
			Interval: "P1D",
			Checkpoints: []string{
				"2020-Mar-06 14:09",
				"2020-Mar-07 14:09",
				"2020-Mar-08 15:09",
				"2020-Mar-09 15:09",
			},
		},
		{
			Name:     "24 Hourly across DST boundary",
			Location: "America/Los_Angeles",
			Start:    "2020-Mar-05 14:09",
			Interval: "PT24H",
			Checkpoints: []string{
				"2020-Mar-06 14:09",
				"2020-Mar-07 14:09",
				"2020-Mar-08 15:09",
				"2020-Mar-09 15:09",
			},
		},
		{
			Name:     "Weekly",
			Location: "America/Los_Angeles",
			Start:    "2020-Jan-13 14:09",
			Interval: "P1W",
			Checkpoints: []string{
				"2020-Jan-20 14:09",
				"2020-Jan-27 14:09",
				"2020-Feb-03 14:09",
			},
		},
		{
			Name:     "Monthly",
			Location: "America/Los_Angeles",
			Start:    "2020-Jan-20 14:09",
			Interval: "P1M",
			Checkpoints: []string{
				"2020-Feb-20 14:09",
				"2020-Mar-20 15:09",
				"2020-Apr-20 15:09",
				"2020-May-20 15:09",
				"2020-Jun-20 15:09",
				"2020-Jul-20 15:09",
				"2020-Aug-20 15:09",
				"2020-Sep-20 15:09",
				"2020-Oct-20 15:09",
				"2020-Nov-20 14:09",
				"2020-Dec-20 14:09",
				"2021-Jan-20 14:09",
			},
		},
		{
			Name:     "Monthly with Normalization",
			Location: "America/Los_Angeles",
			Start:    "2020-Mar-31 14:09",
			Interval: "P1M",
			Checkpoints: []string{
				"2020-May-01 14:09",
				"2020-Jun-01 14:09",
				"2020-Jul-01 14:09",
			},
		},
		{
			Name:     "Yearly across Leap Year boundary",
			Location: "America/Los_Angeles",
			Start:    "2020-Jan-20 14:09",
			Interval: "P1Y",
			Checkpoints: []string{
				"2021-Jan-20 14:09",
				"2022-Jan-20 14:09",
				"2023-Jan-20 14:09",
				"2024-Jan-20 14:09",
				"2025-Jan-20 14:09",
			},
		},
	}

	called := make(chan string)
	endpoint := func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		buf.ReadFrom(r.Body)
		r.Body.Close()
		called <- buf.String()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", endpoint)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	config := DefaultConfig
	config.MySQL.SkipLocked = false
	config.Jobs.FixedIntervals = true
	config.Jobs.ScanFrequency = 1000
	config.Endpoint.BaseURL = "http://" + srv.Listener.Addr().String() + "/"

	d, lis, cleanup := newDalga(t, config)
	defer cleanup()

	clk := d.UseClock(time.Time{})

	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	defer func() {
		cancel()
		<-d.NotifyDone()
	}()

	client := NewClient("http://" + lis.Addr())

	for _, testStruct := range tableTests {
		t.Run(testStruct.Name, func(t *testing.T) {
			ctx := context.Background()

			now := parseTimeInLocation(t, testStruct.Start, testStruct.Location)
			clk.Set(now)

			start := now.Add(time.Second * 5)
			client.Schedule(ctx, "what", testBody, MustWithIntervalString(testStruct.Interval), WithFirstRun(start))

			checkpoints := append([]string{testStruct.Start}, testStruct.Checkpoints...)

			for i, chk := range checkpoints {

				clk.Set(parseTimeInLocation(t, chk, testStruct.Location))

				select {
				case <-called:
					t.Fatalf("Expected job not run on checkpoint %d of test %s.", i, testStruct.Name)
				case <-time.After(time.Second):
				}

				clk.Add(time.Second * 6)

				select {
				case <-called:
				case <-time.After(testTimeout):
					t.Fatalf("Expected job to have run on checkpoint %d of test %s.", i, testStruct.Name)
				}

				time.Sleep(time.Millisecond * 500)
			}

		})

	}

}

func parseTimeInLocation(t *testing.T, value string, location string) time.Time {
	loc, err := time.LoadLocation(location)
	if err != nil {
		t.Fatal(err)
	}
	now, err := time.ParseInLocation("2006-Jan-02 15:04", value, loc)
	if err != nil {
		t.Fatal(err)
	}
	return now
}
