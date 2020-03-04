package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/bmizerany/pat"
	"github.com/cenkalti/dalga/internal/jobmanager"
	"github.com/cenkalti/dalga/internal/log"
	"github.com/cenkalti/dalga/internal/table"
)

type Server struct {
	shutdownTimeout time.Duration
	jobs            *jobmanager.JobManager
	instanceID      uint32
	listener        net.Listener
	httpServer      http.Server
	done            chan struct{}
}

func New(j *jobmanager.JobManager, instanceID uint32, l net.Listener, shutdownTimeout time.Duration) *Server {
	s := &Server{
		shutdownTimeout: shutdownTimeout,
		jobs:            j,
		instanceID:      instanceID,
		listener:        l,
		done:            make(chan struct{}),
	}
	s.httpServer = s.createServer()
	return s
}

func (s *Server) NotifyDone() chan struct{} {
	return s.done
}

func (s *Server) Run(ctx context.Context) {
	defer close(s.done)
	shutdownDone := make(chan struct{})
	go s.waitShutdown(ctx, shutdownDone)
	_ = s.httpServer.Serve(s.listener)
	<-shutdownDone
}

func (s *Server) waitShutdown(ctx context.Context, shutdownDone chan struct{}) {
	defer close(shutdownDone)
	select {
	case <-s.done:
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()
		_ = s.httpServer.Shutdown(shutdownCtx)
	}
}

func (s *Server) createServer() http.Server {
	const path = "/jobs/:jobPath/:jobBody"
	m := pat.New()
	m.Get(path, handler(s.handleGet))
	m.Put(path, handler(s.handleSchedule))
	m.Del(path, handler(s.handleCancel))
	m.Get("/status", http.HandlerFunc(s.handleStatus))
	return http.Server{
		Handler:      m,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  10 * time.Second,
	}
}

func handler(f func(w http.ResponseWriter, r *http.Request, jobPath, body string)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Debugln("http:", r.Method, r.RequestURI)
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

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, path, body string) {
	job, err := s.jobs.Get(r.Context(), path, body)
	if err == table.ErrNotExist {
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

func (s *Server) handleSchedule(w http.ResponseWriter, r *http.Request, path, body string) {
	var opt jobmanager.ScheduleOptions
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

	job, err := s.jobs.Schedule(r.Context(), path, body, opt)
	if err == jobmanager.ErrInvalidArgs {
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

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request, path, body string) {
	err := s.jobs.Cancel(r.Context(), path, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	total, err := s.jobs.Total(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pending, err := s.jobs.Pending(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	running, err := s.jobs.TotalRunning(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	instances, err := s.jobs.Instances(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	m := map[string]interface{}{
		"instance_id":           s.instanceID,
		"instance_running_jobs": s.jobs.Running(),
		"running_jobs":          running,
		"total_jobs":            total,
		"pending_jobs":          pending,
		"total_instances":       instances,
	}
	data, err := json.Marshal(m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}
