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

	"github.com/senseyeio/duration"

	"github.com/cenkalti/dalga/v2/internal/jobmanager"
	"github.com/cenkalti/dalga/v2/internal/server"
	"github.com/cenkalti/dalga/v2/internal/table"
)

var (
	ErrNotExist = table.ErrNotExist
)

type ClientOpt func(c *Client)

func WithClient(clnt *http.Client) ClientOpt {
	return func(c *Client) {
		c.clnt = clnt
	}
}

type Client struct {
	clnt    *http.Client
	BaseURL string
}

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
	buf.ReadFrom(resp.Body)
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

func (clnt *Client) Schedule(ctx context.Context, path, body string, opts ...ScheduleOpt) (*Job, error) {
	so := ScheduleOptions{}
	for _, o := range opts {
		o(&so)
	}

	values := make(url.Values)
	if !so.Interval.IsZero() {
		values.Set("interval", so.Interval.String())
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
	buf.ReadFrom(resp.Body)
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
	buf.ReadFrom(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status code: %d, body: %q", resp.StatusCode, buf.String())
	}

	return nil
}

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
	buf.ReadFrom(resp.Body)
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

type Status = server.Status

type ScheduleOptions = jobmanager.ScheduleOptions

type ScheduleOpt func(o *ScheduleOptions)

func WithInterval(d duration.Duration) ScheduleOpt {
	return func(o *ScheduleOptions) {
		o.Interval = d
	}
}

func MustWithIntervalString(s string) ScheduleOpt {
	d, err := duration.ParseISO8601(s)
	if err != nil {
		panic(err)
	}
	return WithInterval(d)
}

func WithFirstRun(t time.Time) ScheduleOpt {
	return func(o *ScheduleOptions) {
		o.FirstRun = t
	}
}

func WithOneOff() ScheduleOpt {
	return func(o *ScheduleOptions) {
		o.OneOff = true
	}
}

func WithImmediate() ScheduleOpt {
	return func(o *ScheduleOptions) {
		o.Immediate = true
	}
}

type Job = table.JobJSON
