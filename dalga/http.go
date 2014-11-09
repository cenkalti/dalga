package dalga

import (
	"net"
	"net/http"
	"strconv"
)

func (d *Dalga) makeServer() (func(), error) {
	var err error
	d.listener, err = net.Listen("tcp", d.Config.HTTP.Addr())
	if err != nil {
		return nil, err
	}

	handler := http.NewServeMux()
	handler.HandleFunc("/schedule", d.handleSchedule)
	handler.HandleFunc("/cancel", d.handleCancel)

	return func() {
		http.Serve(d.listener, handler)
		debug("HTTP server is done")
		d.quitPublisher <- true
		debug("Sent quit message")
	}, nil
}

// hadleSchedule is the web server endpoint for path: /schedule
func (d *Dalga) handleSchedule(w http.ResponseWriter, r *http.Request) {
	id, routingKey, intervalString := r.FormValue("id"), r.FormValue("routing_key"), r.FormValue("interval")
	debug("/schedule", id, routingKey, intervalString)

	intervalUint64, err := strconv.ParseUint(intervalString, 10, 32)
	if err != nil {
		http.Error(w, "Cannot parse interval", http.StatusBadRequest)
		return
	}

	if intervalUint64 < 1 {
		http.Error(w, "interval must be >= 1", http.StatusBadRequest)
		return
	}

	err = d.Schedule(id, routingKey, uint32(intervalUint64))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleCancel is the web server endpoint for path: /cancel
func (d *Dalga) handleCancel(w http.ResponseWriter, r *http.Request) {
	id, routingKey := r.FormValue("id"), r.FormValue("routing_key")
	debug("/cancel", id, routingKey)

	err := d.Cancel(id, routingKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
