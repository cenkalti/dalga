package dalga

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/dalga/internal/jobmanager"
	"github.com/cenkalti/dalga/internal/table"
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
		BaseURL: baseURL,
		clnt:    http.DefaultClient,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (clnt *Client) Schedule(ctx context.Context, path, body string, opts ...ScheduleOpt) (*Job, error) {

	so := ScheduleOptions{}
	for _, o := range opts {
		o(&so)
	}

	values := make(url.Values)
	if so.Interval > 0 {
		values.Set("interval", strconv.Itoa(int(so.Interval/time.Second)))
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

	baseURL := strings.TrimSuffix(clnt.BaseURL, "/")
	scheduleURL := fmt.Sprintf("%s/jobs/%s/%s", baseURL, path, body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, scheduleURL, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := clnt.clnt.Do(req)
	if err != nil {
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

type ScheduleOptions = jobmanager.ScheduleOptions

type ScheduleOpt func(o *ScheduleOptions)

func WithInterval(d time.Duration) ScheduleOpt {
	return func(o *ScheduleOptions) {
		o.Interval = d
	}
}

func WithFirstRun(t time.Time) ScheduleOpt {
	return func(o *ScheduleOptions) {
		o.FirstRun = t
	}
}

func WithOneOff(b bool) ScheduleOpt {
	return func(o *ScheduleOptions) {
		o.OneOff = b
	}
}

func WithImmediate(b bool) ScheduleOpt {
	return func(o *ScheduleOptions) {
		o.Immediate = b
	}
}

type Job = table.JobJSON
