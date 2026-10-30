// Package api is the HTTP side: decode, check the token, map errors. The
// actual rules live in service.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
	"github.com/JaipreethTiruvaipati/cabflow/internal/safelog"
	"github.com/JaipreethTiruvaipati/cabflow/internal/service"
)

type Server struct {
	jobs    *service.Jobs
	auth    Auth
	log     *slog.Logger
	metrics http.Handler
	ready   func(*http.Request) error
	// OnServerError is called on every 5xx; main hooks the error-rate metric
	// into it.
	OnServerError func()
}

func NewServer(jobs *service.Jobs, auth Auth, log *slog.Logger, metrics http.Handler, ready func(*http.Request) error) *Server {
	return &Server{jobs: jobs, auth: auth, log: log, metrics: metrics, ready: ready}
}

func (s *Server) Routes() http.Handler {
	read := []string{RoleEmployee, RoleAdmin, RoleOperator}
	manage := []string{RoleAdmin, RoleOperator}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", s.metrics)

	mux.HandleFunc("GET /v1/jobs", s.auth.require(s.listJobs, read...))
	mux.HandleFunc("POST /v1/jobs", s.auth.require(s.createJob, manage...))
	mux.HandleFunc("GET /v1/jobs/{id}", s.auth.require(s.getJob, read...))
	mux.HandleFunc("GET /v1/jobs/{id}/runs", s.auth.require(s.listRuns, read...))
	mux.HandleFunc("GET /v1/jobs/{id}/next-fire-times", s.auth.require(s.nextFireTimes, read...))
	mux.HandleFunc("POST /v1/jobs/{id}/trigger", s.auth.require(s.trigger, manage...))
	mux.HandleFunc("POST /v1/tenants/{id}/pause", s.auth.require(s.pause(true), manage...))
	mux.HandleFunc("POST /v1/tenants/{id}/resume", s.auth.require(s.pause(false), manage...))
	mux.HandleFunc("PUT /v1/tenants/{id}/max-running", s.auth.require(s.setMaxRunning, manage...))
	mux.HandleFunc("GET /v1/dead-letters", s.auth.require(s.deadLetters, manage...))
	mux.HandleFunc("POST /v1/dead-letters/{id}/replay", s.auth.require(s.replay, manage...))
	return s.logRequests(mux)
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if err := s.ready(r); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req service.CreateJob
	if !decode(w, r, &req) {
		return
	}
	job, err := s.jobs.Create(r.Context(), principalFrom(r).TenantID, req)
	s.respond(w, http.StatusCreated, job, err)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r)
	jobs, err := s.jobs.List(r.Context(), p.TenantID)
	for i := range jobs {
		jobs[i] = visibleTo(p, jobs[i])
	}
	s.respond(w, http.StatusOK, jobs, err)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	p := principalFrom(r)
	job, err := s.jobs.Get(r.Context(), p.TenantID, id)
	s.respond(w, http.StatusOK, visibleTo(p, job), err)
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	runs, err := s.jobs.Runs(r.Context(), principalFrom(r).TenantID, id, queryInt(r, "limit"))
	s.respond(w, http.StatusOK, runs, err)
}

func (s *Server) nextFireTimes(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	times, err := s.jobs.NextFireTimes(r.Context(), principalFrom(r).TenantID, id, queryInt(r, "limit"))
	s.respond(w, http.StatusOK, map[string][]time.Time{"next_fire_times": times}, err)
}

// trigger runs a job right away. A client retrying with the same
// Idempotency-Key gets the same run back.
func (s *Server) trigger(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	run, err := s.jobs.Trigger(r.Context(), principalFrom(r).TenantID, id, r.Header.Get("Idempotency-Key"))
	s.respond(w, http.StatusAccepted, run, err)
}

// Admins can pause their own tenant; operators can pause anyone.
func (s *Server) pause(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := s.tenantInPath(w, r)
		if !ok {
			return
		}
		err := s.jobs.Pause(r.Context(), tenant, paused)
		s.respond(w, http.StatusOK, map[string]bool{"paused": paused}, err)
	}
}

func (s *Server) setMaxRunning(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenantInPath(w, r)
	if !ok {
		return
	}
	var body struct {
		MaxRunning int `json:"max_running"`
	}
	if !decode(w, r, &body) {
		return
	}
	err := s.jobs.SetMaxRunning(r.Context(), tenant, body.MaxRunning)
	s.respond(w, http.StatusOK, body, err)
}

func (s *Server) deadLetters(w http.ResponseWriter, r *http.Request) {
	runs, err := s.jobs.DeadLetters(r.Context(), principalFrom(r).TenantID, queryInt(r, "limit"))
	s.respond(w, http.StatusOK, runs, err)
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	err := s.jobs.Replay(r.Context(), principalFrom(r).TenantID, id)
	s.respond(w, http.StatusAccepted, map[string]string{"status": "requeued"}, err)
}

func (s *Server) tenantInPath(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	tenant, ok := pathID(w, r)
	if !ok {
		return uuid.Nil, false
	}
	p := principalFrom(r)
	if tenant != p.TenantID && !p.Has(RoleOperator) {
		writeError(w, http.StatusForbidden, "forbidden")
		return uuid.Nil, false
	}
	return tenant, true
}

// visibleTo strips the payload for employees. Payloads can carry other
// people's addresses and phone numbers.
func visibleTo(p Principal, job domain.Job) domain.Job {
	if !p.Has(RoleAdmin, RoleOperator) {
		job.Payload = nil
	}
	return job
}

// respond only lets validation messages through to the client. Anything else
// gets logged and goes back as a plain 500.
func (s *Server) respond(w http.ResponseWriter, status int, body any, err error) {
	var invalid *domain.ValidationError
	switch {
	case err == nil:
		writeJSON(w, status, body)
	case errors.As(err, &invalid):
		writeError(w, http.StatusBadRequest, invalid.Message)
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	default:
		s.log.Error("request failed", "error", safelog.Error(err))
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func decode(w http.ResponseWriter, r *http.Request, into any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "not found")
		return uuid.Nil, false
	}
	return id, true
}

func queryInt(r *http.Request, name string) int {
	value, _ := strconv.Atoi(r.URL.Query().Get(name))
	return value
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// logRequests writes one line per request. Bodies, headers and query strings
// stay out of it since they can hold tokens or personal data.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		if sw.status >= 500 && s.OnServerError != nil {
			s.OnServerError()
		}
		if r.URL.Path == "/metrics" || r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			return
		}
		s.log.Info("http", "method", r.Method, "route", r.Pattern, "status", sw.status, "ms", time.Since(start).Milliseconds())
	})
}
