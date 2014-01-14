package dalga

import (
	"net"
	"net/http"
	"strconv"
)

func (d *Dalga) makeServer() (func(), error) {
	var err error
	addr := d.C.HTTP.Host + ":" + d.C.HTTP.Port
	d.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	handler := http.NewServeMux()
	handler.HandleFunc("/schedule", d.makeHandler(handleSchedule))
	handler.HandleFunc("/cancel", d.makeHandler(handleCancel))

	return func() {
		http.Serve(d.listener, handler)
		debug("HTTP server is done")
		d.quitPublisher <- true
		debug("Sent quit message")
	}, nil
}

func (dalga *Dalga) makeHandler(fn func(http.ResponseWriter, *http.Request, *Dalga)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fn(w, r, dalga)
	}
}

// hadleSchedule is the web server endpoint for path: /schedule
func handleSchedule(w http.ResponseWriter, r *http.Request, d *Dalga) {
	routingKey, body, intervalString := r.FormValue("routing_key"), r.FormValue("body"), r.FormValue("interval")
	debug("/schedule", routingKey, body)

	intervalUint64, err := strconv.ParseUint(intervalString, 10, 64)
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
		panic(err)
	}
}

// handleCancel is the web server endpoint for path: /cancel
func handleCancel(w http.ResponseWriter, r *http.Request, d *Dalga) {
	routingKey, body := r.FormValue("routing_key"), r.FormValue("body")
	debug("/cancel", routingKey, body)

	err := d.Cancel(routingKey, []byte(body))
	if err != nil {
		panic(err)
	}
}
