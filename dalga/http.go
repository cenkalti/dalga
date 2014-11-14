package dalga

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cenkalti/dalga/dalga/Godeps/_workspace/src/github.com/bmizerany/pat"
)

func (d *Dalga) serveHTTP() error {
	const path = "/jobs/:jobPath/:jobBody"
	m := pat.New()
	m.Get(path, handler(d.handleGet))
	m.Put(path, handler(d.handleSchedule))
	m.Post(path, handler(d.handleTrigger))
	m.Del(path, handler(d.handleCancel))
	m.Get("/status", http.HandlerFunc(d.handleStatus))
	return http.Serve(d.listener, m)
}

func handler(f func(w http.ResponseWriter, r *http.Request, jobPath, body string)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		debug("http:", r.Method, r.RequestURI)
		var err error

		jobPath := r.URL.Query().Get(":jobPath")
		if jobPath == "" {
			http.Error(w, "empty routing key", http.StatusBadRequest)
			return
		}
		jobPath, err = url.QueryUnescape(jobPath)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		jobBody := r.URL.Query().Get(":jobBody")
		if jobBody == "" {
			http.Error(w, "empty job", http.StatusBadRequest)
			return
		}
		jobBody, err = url.QueryUnescape(jobBody)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		f(w, r, jobPath, jobBody)
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

func (d *Dalga) handleGet(w http.ResponseWriter, r *http.Request, path, body string) {
	job, err := d.Jobs.Get(path, body)
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

func (d *Dalga) handleSchedule(w http.ResponseWriter, r *http.Request, path, body string) {
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
	job, err := d.Jobs.Schedule(path, body, interval, oneOff)
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

func (d *Dalga) handleTrigger(w http.ResponseWriter, r *http.Request, path, body string) {
	job, err := d.Jobs.Trigger(path, body)
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

func (d *Dalga) handleCancel(w http.ResponseWriter, r *http.Request, path, body string) {
	err := d.Jobs.Cancel(path, body)
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
	count, err := d.Jobs.Total()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	m := map[string]interface{}{
		"running_jobs": d.Jobs.Running(),
		"total_jobs":   count,
	}
	data, err := json.Marshal(m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
