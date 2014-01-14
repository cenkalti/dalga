package dalga

import (
	"testing"
)

func TestSchedule(t *testing.T) {
	d := NewDalga(NewConfig())
	err := d.Start()
	if err != nil {
		t.Error("Cannot start Dalga:", err)
		return
	}

	defer d.Shutdown()

	err = d.Schedule("key", []byte("body"), 1)
	if err != nil {
		t.Error("Cannot schedule new job")
	}
}
