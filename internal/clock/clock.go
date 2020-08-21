package clock

import (
	"sync"
	"time"
)

type Clock struct {
	t time.Time
	l sync.RWMutex
}

func New(t time.Time) *Clock {
	return &Clock{t: t}
}

func (clk *Clock) Set(t time.Time) {
	clk.l.Lock()
	clk.t = t
	clk.l.Unlock()
}

func (clk *Clock) Get() time.Time {
	clk.l.RLock()
	defer clk.l.RUnlock()
	return clk.t
}

func (clk *Clock) Add(d time.Duration) {
	clk.l.Lock()
	clk.t = clk.t.Add(d)
	clk.l.Unlock()
}

func (clk *Clock) NowUTC() *time.Time {
	if clk == nil {
		return nil
	}
	t := clk.Get().UTC()
	return &t
}
