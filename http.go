package dalga

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bmizerany/pat"
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

func parseBool(r *http.Request, param string) (bool, error) {
	switch strings.ToLower(r.FormValue(param)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off", "":
		return false, nil
	default:
		return false, fmt.Errorf("invalid param: %s", param)
	}
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
	oneOff, err := parseBool(r, "one-off")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	immediate, err := parseBool(r, "immediate")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var interval *time.Duration
	intervalParam := r.FormValue("interval")
	if intervalParam != "" {
		i64, err := strconv.ParseUint(intervalParam, 10, 32)
		if err != nil {
			http.Error(w, "cannot parse interval", http.StatusBadRequest)
			return
		}
		d := time.Duration(uint32(i64)) * time.Second
		interval = &d
	}

	var firstRun *time.Time
	firstRunParam := r.FormValue("first-run")
	if firstRunParam != "" {
		t, err := time.Parse(time.RFC3339, firstRunParam)
		if err != nil {
			http.Error(w, "cannot parse first-run", http.StatusBadRequest)
			return
		}
		firstRun = &t
	}

	job, err := d.Jobs.Schedule(path, body, oneOff, immediate, firstRun, interval)
	if err == errInvalidArgs {
		http.Error(w, "invalid params", http.StatusBadRequest)
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
	if err != nil && err != ErrNotExist {
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
