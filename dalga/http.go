package dalga

import (
	"net/http"
	"strconv"
)

func (d *Dalga) serveHTTP() error {
	handler := http.NewServeMux()
	handler.HandleFunc("/schedule", d.handleSchedule)
	handler.HandleFunc("/cancel", d.handleCancel)
	return http.Serve(d.listener, handler)
}

// hadleSchedule is the web server endpoint for path: /schedule
func (d *Dalga) handleSchedule(w http.ResponseWriter, r *http.Request) {
	job, routingKey, intervalString := r.FormValue("job"), r.FormValue("routing_key"), r.FormValue("interval")
	debug("/schedule", job, routingKey, intervalString)

	intervalUint64, err := strconv.ParseUint(intervalString, 10, 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if intervalUint64 == 0 {
		http.Error(w, "interval can't be 0", http.StatusBadRequest)
		return
	}

	err = d.Schedule(job, routingKey, uint32(intervalUint64))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleCancel is the web server endpoint for path: /cancel
func (d *Dalga) handleCancel(w http.ResponseWriter, r *http.Request) {
	job, routingKey := r.FormValue("job"), r.FormValue("routing_key")
	debug("/cancel", job, routingKey)

	err := d.Cancel(job, routingKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
