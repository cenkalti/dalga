package retry

import (
	"testing"
	"time"
)

func t0(sec float64) time.Time {
	const start = "2000-01-01T00:00:00Z"
	t, _ := time.Parse(time.RFC3339, start)
	return t.Add(time.Duration(sec * float64(time.Second)))
}

func TestNextRun(t *testing.T) {
	r1 := Retry{
		Interval:    time.Second,
		Multiplier:  1,
		MaxInterval: time.Second,
	}
	r2 := Retry{
		Interval:    time.Second,
		Multiplier:  2,
		MaxInterval: 10 * time.Second,
	}
	r3 := Retry{
		Interval:    time.Second,
		Multiplier:  3,
		MaxInterval: 100 * time.Second,
	}
	r4 := Retry{
		Interval:    8 * time.Second,
		Multiplier:  1.5,
		MaxInterval: 100 * time.Second,
	}
	r5 := Retry{
		Interval:    1500 * time.Millisecond,
		Multiplier:  1.5,
		MaxInterval: 100 * time.Second,
	}
	cases := []struct {
		Retry   Retry
		Now     float64
		NextRun float64
	}{
		{r1, 0, 1},
		{r1, 10, 11},
		{r2, 0, 1},
		{r2, 1, 3},
		{r2, 3, 7},
		{r2, 7, 15},
		{r3, 0, 1},
		{r3, 1, 4},
		{r3, 4, 13},
		{r3, 13, 40},
		{r4, 0, 8},
		{r4, 8, 20},
		{r5, 0, 1.5},
		{r5, 1.5, 3.75},
	}
	for _, c := range cases {
		nextRun := c.Retry.NextRun(t0(0), t0(c.Now))
		if !nextRun.Equal(t0(c.NextRun)) {
			t.Log(c.Retry.Interval, c.Retry.MaxInterval, c.Retry.Multiplier, c.Now, c.NextRun, nextRun.String())
			t.FailNow()
		}
	}

}
