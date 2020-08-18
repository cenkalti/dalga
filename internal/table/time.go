package table

import (
	"sync"
	"time"
)

type Clock struct {
	t time.Time
	l sync.RWMutex
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

func (clk *Clock) NowUTC() *time.Time {
	if clk == nil {
		return nil
	}
	t := clk.Get().UTC()
	return &t
}
