package dalga

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/cenkalti/dalga/vendor/github.com/bmizerany/pat"
)

func (d *Dalga) serveHTTP() error {
	const path = "/jobs/:routingKey/:jobDescription"
	m := pat.New()
	m.Get(path, handler(d.handleGet))
	m.Put(path, handler(d.handleSchedule))
	m.Del(path, handler(d.handleCancel))
	return http.Serve(d.listener, m)
}

func handler(f func(w http.ResponseWriter, r *http.Request, description, routingKey string)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		debug("http:", r.Method, r.RequestURI)
		routingKey := r.URL.Query().Get(":routingKey")
		if routingKey == "" {
			http.Error(w, "empty routing key", http.StatusBadRequest)
			return
		}
		jobDescription := r.URL.Query().Get(":jobDescription")
		if jobDescription == "" {
			http.Error(w, "empty job", http.StatusBadRequest)
			return
		}
		f(w, r, jobDescription, routingKey)
	})
}

func getInterval(r *http.Request) (uint32, error) {
	s := r.FormValue("interval")
	if s == "" {
		return 0, errors.New("empty interval")
	}
	i64, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, errors.New("cannot parse interval")
	}
	return uint32(i64), nil
}

func (d *Dalga) handleGet(w http.ResponseWriter, r *http.Request, description, routingKey string) {
	job, err := d.table.Get(description, routingKey)
	if err == ErrNotExist {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := json.Marshal(job)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (d *Dalga) handleSchedule(w http.ResponseWriter, r *http.Request, description, routingKey string) {
	interval, err := getInterval(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	oneOff := r.FormValue("one-off") != ""
	if !oneOff && interval == 0 {
		http.Error(w, "interval can't be 0 for periodic jobs", http.StatusBadRequest)
		return
	}
	job, err := d.Schedule(description, routingKey, interval, oneOff)
	if err == ErrExist {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, err := json.Marshal(job)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(data)
}

func (d *Dalga) handleCancel(w http.ResponseWriter, r *http.Request, description, routingKey string) {
	err := d.Cancel(description, routingKey)
	if err == ErrNotExist {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
