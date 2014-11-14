package dalga

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cenkalti/dalga/vendor/github.com/bmizerany/pat"
)

func (d *Dalga) serveHTTP() error {
	const path = "/jobs/:routingKey/:jobDescription"
	m := pat.New()
	m.Get(path, handler(d.handleGet))
	m.Put(path, handler(d.handleSchedule))
	m.Post(path, handler(d.handleTrigger))
	m.Del(path, handler(d.handleCancel))
	m.Get("/status", http.HandlerFunc(d.handleStatus))
	return http.Serve(d.listener, m)
}

func handler(f func(w http.ResponseWriter, r *http.Request, description, routingKey string)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		debug("http:", r.Method, r.RequestURI)

		routingKeyParam := r.URL.Query().Get(":routingKey")
		if routingKeyParam == "" {
			http.Error(w, "empty routing key", http.StatusBadRequest)
			return
		}
		routingKey, err := url.QueryUnescape(routingKeyParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		jobDescriptionParam := r.URL.Query().Get(":jobDescription")
		if jobDescriptionParam == "" {
			http.Error(w, "empty job", http.StatusBadRequest)
			return
		}
		jobDescription, err := url.QueryUnescape(jobDescriptionParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
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
	job, err := d.ScheduleJob(description, routingKey, interval, oneOff)
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

func (d *Dalga) handleTrigger(w http.ResponseWriter, r *http.Request, description, routingKey string) {
	job, err := d.TriggerJob(description, routingKey)
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

func (d *Dalga) handleCancel(w http.ResponseWriter, r *http.Request, description, routingKey string) {
	err := d.CancelJob(description, routingKey)
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

func (d *Dalga) handleStatus(w http.ResponseWriter, r *http.Request) {
	count, err := d.table.Count()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	d.m.Lock()
	m := map[string]interface{}{
		"running_jobs": len(d.runningJobs),
		"total_jobs":   count,
	}
	d.m.Unlock()
	data, err := json.Marshal(m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
