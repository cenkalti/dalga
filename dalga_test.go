package dalga

import (
	"testing"
	"time"
)

func TestSchedule(t *testing.T) {
	d := NewDalga(NewConfig())
	started, err := d.Start()
	if err != nil {
		t.Error("Cannot start Dalga")
	}
	defer d.Shutdown()

	select {
	case <-started:
		err := d.Schedule("key", []byte("body"), 1)
		if err != nil {
			t.Error("Cannot schedule new job")
		}
	case <-time.After(time.Duration(2) * time.Second):
		t.Error("Dalga did not start in allowed time")
	}
}
