package dalga

import (
	"net/http"
	"strconv"

	"github.com/cenkalti/dalga/vendor/github.com/bmizerany/pat"
)

func (d *Dalga) serveHTTP() error {
	m := pat.New()
	m.Put("/jobs/:routing_key/:description", http.HandlerFunc(d.handleSchedule))
	m.Del("/jobs/:routing_key/:description", http.HandlerFunc(d.handleCancel))
	return http.Serve(d.listener, m)
}

func (d *Dalga) handleSchedule(w http.ResponseWriter, r *http.Request) {
	routingKey := r.URL.Query().Get(":routing_key")
	description := r.URL.Query().Get(":description")
	intervalString := r.FormValue("interval")

	debug("http: schedule", r.RequestURI, intervalString)

	intervalUint64, err := strconv.ParseUint(intervalString, 10, 32)
	if err != nil {
		http.Error(w, "cannot parse interval", http.StatusBadRequest)
		return
	}

	if intervalUint64 == 0 {
		http.Error(w, "interval can't be 0", http.StatusBadRequest)
		return
	}

	updated, err := d.Schedule(description, routingKey, uint32(intervalUint64))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if updated {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
}

func (d *Dalga) handleCancel(w http.ResponseWriter, r *http.Request) {
	routingKey := r.URL.Query().Get(":routing_key")
	description := r.URL.Query().Get(":description")

	debug("http: cancel", r.RequestURI)

	err := d.Cancel(description, routingKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
