package dalga

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
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
	_, _ = w.Write(data)
}

func (d *Dalga) handleSchedule(w http.ResponseWriter, r *http.Request, path, body string) {
	var opt ScheduleOptions
	var err error

	oneOffParam := r.FormValue("one-off")
	if oneOffParam != "" {
		opt.OneOff, err = strconv.ParseBool(oneOffParam)
		if err != nil {
			http.Error(w, "cannot parse one-off", http.StatusBadRequest)
			return
		}
	}

	immediateParam := r.FormValue("immediate")
	if immediateParam != "" {
		opt.Immediate, err = strconv.ParseBool(immediateParam)
		if err != nil {
			http.Error(w, "cannot parse immediate", http.StatusBadRequest)
			return
		}
	}

	intervalParam := r.FormValue("interval")
	if intervalParam != "" {
		i64, err := strconv.ParseUint(intervalParam, 10, 32)
		if err != nil {
			http.Error(w, "cannot parse interval", http.StatusBadRequest)
			return
		}
		opt.Interval = time.Duration(uint32(i64)) * time.Second
	}

	firstRunParam := r.FormValue("first-run")
	if firstRunParam != "" {
		opt.FirstRun, err = time.Parse(time.RFC3339, firstRunParam)
		if err != nil {
			http.Error(w, "cannot parse first-run", http.StatusBadRequest)
			return
		}
	}

	job, err := d.Jobs.Schedule(path, body, opt)
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
	_, _ = w.Write(data)
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
	_, _ = w.Write(data)
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
	_, _ = w.Write(data)
}
