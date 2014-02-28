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
	routingKey, body, intervalString := r.FormValue("routing_key"), r.FormValue("body"), r.FormValue("interval")
	debug("/schedule", routingKey, body, intervalString)

	intervalUint64, err := strconv.ParseUint(intervalString, 10, 32)
	if err != nil {
		http.Error(w, "Cannot parse interval", http.StatusBadRequest)
		return
	}

	if intervalUint64 < 1 {
		http.Error(w, "interval must be >= 1", http.StatusBadRequest)
		return
	}

	err = d.Schedule(routingKey, []byte(body), uint32(intervalUint64))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleCancel is the web server endpoint for path: /cancel
func (d *Dalga) handleCancel(w http.ResponseWriter, r *http.Request) {
	routingKey, body := r.FormValue("routing_key"), r.FormValue("body")
	debug("/cancel", routingKey, body)

	err := d.Cancel(routingKey, []byte(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
