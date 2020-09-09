package dalga

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/dalga/v3/internal/jobmanager"
	"github.com/cenkalti/dalga/v3/internal/server"
	"github.com/cenkalti/dalga/v3/internal/table"
	"github.com/senseyeio/duration"
)

var ErrNotExist = table.ErrNotExist

// ClientOpt is an option that can be provided to a Dalga client.
type ClientOpt func(c *Client)

// WithClient provides a specific HTTP client.
func WithClient(clnt *http.Client) ClientOpt {
	return func(c *Client) {
		c.clnt = clnt
	}
}

// Client is used to interact with a Dalga cluster using REST.
type Client struct {
	clnt    *http.Client
	BaseURL string
}

// NewClient creates a REST Client for a Dalga cluster.
func NewClient(baseURL string, opts ...ClientOpt) *Client {
	c := &Client{
		BaseURL: strings.TrimSuffix(baseURL, "/"),
		clnt:    http.DefaultClient,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Get retrieves the job with path and body.
func (clnt *Client) Get(ctx context.Context, path, body string) (*Job, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, clnt.jobURL(path, body), nil)
	if err != nil {
		return nil, err
	}

	resp, err := clnt.clnt.Do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
		return nil, fmt.Errorf("cannot get job: %w", err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotExist
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, body: %q", resp.StatusCode, buf.String())
	}

	var j Job
	dec := json.NewDecoder(&buf)
	if err := dec.Decode(&j); err != nil {
		return nil, fmt.Errorf("cannot unmarshal body: %q, cause: %w", buf.String(), err)
	}

	return &j, nil
}

// Schedule creates a new job with path and body, and the provided options.
func (clnt *Client) Schedule(ctx context.Context, path, body string, opts ...ScheduleOpt) (*Job, error) {
	so := jobmanager.ScheduleOptions{}
	for _, o := range opts {
		o(&so)
	}

	values := make(url.Values)
	if !so.Interval.IsZero() {
		values.Set("interval", so.Interval.String())
	}
	if so.Location != nil {
		values.Set("location", so.Location.String())
	}
	if !so.FirstRun.IsZero() {
		values.Set("first-run", so.FirstRun.Format(time.RFC3339))
	}
	if so.OneOff {
		values.Set("one-off", "true")
	}
	if so.Immediate {
		values.Set("immediate", "true")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, clnt.jobURL(path, body), strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := clnt.clnt.Do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
		return nil, fmt.Errorf("cannot schedule new job: %w", err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("unexpected status code: %d, body: %q", resp.StatusCode, buf.String())
	}

	var j Job
	dec := json.NewDecoder(&buf)
	if err := dec.Decode(&j); err != nil {
		return nil, fmt.Errorf("cannot unmarshal body: %q, cause: %w", buf.String(), err)
	}

	return &j, nil
}

// Disable stops the job with path and body from running at its scheduled times.
func (clnt *Client) Disable(ctx context.Context, path, body string) (*Job, error) {
	return clnt.setEnabled(ctx, path, body, false)
}

// Enable allows the job with path and body to continue running at its scheduled times.
//
// If the next scheduled run is still in the future, the job will execute at that point.
// If the scheduled run is now in the past, the behavior depends upon the value of
// the FixedIntervals setting:
//
// If FixedIntervals is false, the job will run immediately.
//
// If FixedIntervals is true, the job will reschedule to the next appropriate point
// in the future based on its interval setting, effectively skipping the scheduled
// runs that were missed while the job was disabled.
func (clnt *Client) Enable(ctx context.Context, path, body string) (*Job, error) {
	return clnt.setEnabled(ctx, path, body, true)
}

func (clnt *Client) setEnabled(ctx context.Context, path, body string, enabled bool) (*Job, error) {
	action := "enable"
	if !enabled {
		action = "disable"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, clnt.jobURL(path, body)+"?"+action+"=true", nil)
	if err != nil {
		return nil, err
	}

	resp, err := clnt.clnt.Do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
		return nil, fmt.Errorf("cannot %s job: %w", action, err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotExist
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, body: %q", resp.StatusCode, buf.String())
	}

	var j Job
	dec := json.NewDecoder(&buf)
	if err := dec.Decode(&j); err != nil {
		return nil, fmt.Errorf("cannot unmarshal body: %q, cause: %w", buf.String(), err)
	}

	return &j, nil
}

// Cancel deletes the job with path and body.
func (clnt *Client) Cancel(ctx context.Context, path, body string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, clnt.jobURL(path, body), nil)
	if err != nil {
		return err
	}

	resp, err := clnt.clnt.Do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
		return fmt.Errorf("cannot cancel job: %w", err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status code: %d, body: %q", resp.StatusCode, buf.String())
	}

	return nil
}

// Status returns general information about the Dalga cluster.
func (clnt *Client) Status(ctx context.Context) (*Status, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, clnt.BaseURL+"/status", nil)
	if err != nil {
		return nil, err
	}

	resp, err := clnt.clnt.Do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
		return nil, fmt.Errorf("cannot get status: %w", err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, body: %q", resp.StatusCode, buf.String())
	}

	var s Status
	dec := json.NewDecoder(&buf)
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("cannot unmarshal body: %q, cause: %w", buf.String(), err)
	}

	return &s, nil
}

func (clnt *Client) jobURL(path, body string) string {
	return fmt.Sprintf("%s/jobs/%s/%s", clnt.BaseURL, path, body)
}

// Status contains general information about a Dalga cluster.
type Status = server.Status

// ScheduleOpt is an option that can be provided to the Schedule method.
type ScheduleOpt func(o *jobmanager.ScheduleOptions)

// WithInterval specifies that a job should recur, with frequency
// given as an ISO8601 duration as an interval:
// https://en.wikipedia.org/wiki/ISO_8601#Time_intervals
//
// This option is incompatible with the WithOneOff option.
func WithInterval(d duration.Duration) ScheduleOpt {
	return func(o *jobmanager.ScheduleOptions) {
		o.Interval = d
	}
}

// MustWithIntervalString is identical to WithInterval, except that it performs a parsing step.
// It panics if s is not a valid ISO8601 duration.
func MustWithIntervalString(s string) ScheduleOpt {
	d, err := duration.ParseISO8601(s)
	if err != nil {
		panic(err)
	}
	return WithInterval(d)
}

// WithLocation specifies what location a job's schedule should be relative to.
// This is solely relevant for calculating intervals using an ISO8601 duration,
// since "P1D" can mean 23 or 25 hours of real time if the job's location is
// undergoing a daylight savings shift within that period.
//
// Note that Dalga will not double-execute a job if it's scheduled at a time that repeats
// itself during a daylight savings shift, since it doesn't use wall clock time.
//
// If this option is omitted, the job will default to UTC as a location.
func WithLocation(l *time.Location) ScheduleOpt {
	return func(o *jobmanager.ScheduleOptions) {
		o.Location = l
	}
}

// MustWithLocationName is identical to WithLocation, except that it performs a parsing step.
// It panics if n is not a valid *time.Location name.
func MustWithLocationName(n string) ScheduleOpt {
	l, err := time.LoadLocation(n)
	if err != nil {
		panic(err)
	}
	return WithLocation(l)
}

// WithFirstRun specifies the job's first scheduled execution time.
// It's incompatible with the WithImmediate option.
//
// The timezone of t is used when computing the first execution's
// instant in time, but subsequent intervals are computed within
// the timezone specified by the job's location.
//
// If neither WithFirstRun or WithImmediate are used,
// the job's initial run will occur after one interval has elapsed.
func WithFirstRun(t time.Time) ScheduleOpt {
	return func(o *jobmanager.ScheduleOptions) {
		o.FirstRun = t
	}
}

// WithOneOff specifies that the job should run once and then delete itself.
// It's incompatible with the WithInterval option.
func WithOneOff() ScheduleOpt {
	return func(o *jobmanager.ScheduleOptions) {
		o.OneOff = true
	}
}

// WithImmediate specifies that the job should run immediately.
// It's incompatible with the WithFirstRun option.
//
// If neither WithFirstRun or WithImmediate are used,
// the job's initial run will occur after one interval has elapsed.
func WithImmediate() ScheduleOpt {
	return func(o *jobmanager.ScheduleOptions) {
		o.Immediate = true
	}
}

// Job is the external representation of a job in Dalga.
type Job = table.JobJSON
