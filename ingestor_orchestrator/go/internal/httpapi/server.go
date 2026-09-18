// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/go/internal/app"
)

type Dispatcher interface {
	RetryJob(context.Context, string) (*app.Job, error)
	SyncInstance(context.Context, string) (*app.MetadataSync, error)
	SyncAll(context.Context) []app.SyncDispatchResult
}
type Pinger interface{ Ping(context.Context) error }
type Server struct {
	dispatcher       Dispatcher
	database, broker Pinger
	ready            func() bool
	queries          app.QueryStore
	staticDir        string
}

type Option func(*Server)

func WithQueries(queries app.QueryStore) Option { return func(s *Server) { s.queries = queries } }
func WithStaticDir(directory string) Option     { return func(s *Server) { s.staticDir = directory } }

func New(dispatcher Dispatcher, database, broker Pinger, ready func() bool, options ...Option) http.Handler {
	server := &Server{dispatcher: dispatcher, database: database, broker: broker, ready: ready}
	for _, option := range options {
		option(server)
	}
	return server.router()
}

func (s *Server) router() http.Handler {
	router := chi.NewRouter()
	router.NotFound(func(w http.ResponseWriter, _ *http.Request) { writeError(w, http.StatusNotFound, "Not found") })
	router.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	router.Get("/ready", s.handleReady)
	router.Post("/api/jobs/{jobID}/retry", func(w http.ResponseWriter, r *http.Request) { s.handleRetry(w, r, chi.URLParam(r, "jobID")) })
	router.Post("/api/metadata/sync", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.dispatcher.SyncAll(r.Context()))
	})
	router.Post("/api/metadata/sync/{instanceID}", func(w http.ResponseWriter, r *http.Request) { s.handleSync(w, r, chi.URLParam(r, "instanceID")) })
	if s.queries != nil {
		s.registerQueries(router)
	}
	router.Get("/openapi.json", func(w http.ResponseWriter, _ *http.Request) { writeOpenAPI(w) })
	router.Get("/docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(swaggerHTML))
	})
	if s.staticDir != "" {
		router.Get("/*", s.serveStatic)
	}
	return s.withCORS(s.trimTrailingSlash(router))
}

func (s *Server) registerQueries(router chi.Router) {
	router.Get("/api/instances", s.listInstances)
	router.Post("/api/instances", s.createInstance)
	router.Get("/api/instances/{instanceID}", func(w http.ResponseWriter, r *http.Request) { s.getInstance(w, r, chi.URLParam(r, "instanceID")) })
	router.Delete("/api/instances/{instanceID}", func(w http.ResponseWriter, r *http.Request) { s.deleteInstance(w, r, chi.URLParam(r, "instanceID")) })
	router.Get("/api/jobs", s.listJobs)
	router.Get("/api/jobs/{jobID}", func(w http.ResponseWriter, r *http.Request) { s.getJob(w, r, chi.URLParam(r, "jobID")) })
	router.Get("/api/resources", s.listResources)
	router.Get("/api/resources/{resourceID}", func(w http.ResponseWriter, r *http.Request) { s.getResource(w, r, chi.URLParam(r, "resourceID")) })
	router.Get("/api/datasets", s.listDatasets)
	router.Get("/api/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
		value, err := s.queries.Dashboard(r.Context())
		s.writeQueryResult(w, value, err, "")
	})
	router.Get("/api/syncs", s.listSyncs)
	router.Get("/api/syncs/{syncID}", func(w http.ResponseWriter, r *http.Request) {
		value, err := s.queries.GetSync(r.Context(), chi.URLParam(r, "syncID"))
		s.writeQueryResult(w, value, err, "Sync not found")
	})
}

func (s *Server) trimTrailingSlash(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && strings.HasSuffix(r.URL.Path, "/") {
			request := r.Clone(r.Context())
			requestURL := *r.URL
			requestURL.Path = strings.TrimSuffix(requestURL.Path, "/")
			requestURL.RawPath = ""
			request.URL = &requestURL
			r = request
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.cors(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) listInstances(w http.ResponseWriter, r *http.Request) {
	value, err := s.queries.ListInstances(r.Context())
	s.writeQueryResult(w, value, err, "")
}
func (s *Server) getInstance(w http.ResponseWriter, r *http.Request, id string) {
	value, err := s.queries.GetInstance(r.Context(), id)
	s.writeQueryResult(w, value, err, "Instance not found")
}
func (s *Server) createInstance(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil || strings.TrimSpace(input.Name) == "" || strings.TrimSpace(input.URL) == "" {
		writeError(w, http.StatusUnprocessableEntity, "Invalid request body")
		return
	}
	now := time.Now().UTC()
	instance := &app.Instance{ID: uuid.NewString(), Name: input.Name, URL: input.URL, CreatedAt: now, UpdatedAt: now}
	if err := s.queries.CreateInstance(r.Context(), instance); err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, instance)
}
func (s *Server) deleteInstance(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.queries.DeleteInstance(r.Context(), id); err != nil {
		writeNamedError(w, err, "Instance not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func pagination(r *http.Request) (int, int, error) {
	limit, offset := 50, 0
	var err error
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil {
			return 0, 0, err
		}
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		offset, err = strconv.Atoi(raw)
		if err != nil {
			return 0, 0, err
		}
	}
	if limit < 1 || limit > 500 || offset < 0 {
		return 0, 0, errors.New("invalid pagination")
	}
	return limit, offset, nil
}
func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, 422, "Invalid pagination")
		return
	}
	q := r.URL.Query()
	if status := q.Get("status"); status != "" && !oneOf(status, "pending", "processing", "completed", "failed", "deleted") {
		writeError(w, http.StatusUnprocessableEntity, "Invalid status")
		return
	}
	query := app.JobQuery{Status: q.Get("status"), ResourceID: q.Get("resource_id"), InstanceID: q.Get("instance_id"), Tags: q.Get("tags"), OrderBy: q.Get("order_by"), OrderDir: q.Get("order_dir"), Limit: limit, Offset: offset}
	value, err := s.queries.ListJobs(r.Context(), query)
	s.writeQueryResult(w, value, err, "")
}
func (s *Server) getJob(w http.ResponseWriter, r *http.Request, id string) {
	value, err := s.queries.GetJob(r.Context(), id)
	s.writeQueryResult(w, value, err, "Job not found")
}
func resourceQuery(r *http.Request) (app.ResourceQuery, error) {
	limit, offset, err := pagination(r)
	q := r.URL.Query()
	return app.ResourceQuery{Status: q.Get("status"), InstanceID: q.Get("instance_id"), Search: q.Get("search"), Limit: limit, Offset: offset}, err
}
func (s *Server) listResources(w http.ResponseWriter, r *http.Request) {
	query, err := resourceQuery(r)
	if err != nil {
		writeError(w, 422, "Invalid pagination")
		return
	}
	if query.Status != "" && !oneOf(query.Status, "pending", "processing", "completed", "failed", "outdated", "deleted") {
		writeError(w, http.StatusUnprocessableEntity, "Invalid status")
		return
	}
	value, err := s.queries.ListResources(r.Context(), query)
	s.writeQueryResult(w, value, err, "")
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
func (s *Server) getResource(w http.ResponseWriter, r *http.Request, id string) {
	value, err := s.queries.GetResource(r.Context(), id)
	s.writeQueryResult(w, value, err, "Resource not found")
}
func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request) {
	query, err := resourceQuery(r)
	if err != nil {
		writeError(w, 422, "Invalid pagination")
		return
	}
	value, err := s.queries.ListDatasets(r.Context(), query)
	s.writeQueryResult(w, value, err, "")
}
func (s *Server) listSyncs(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, 422, "Invalid pagination")
		return
	}
	value, err := s.queries.ListSyncs(r.Context(), app.SyncQuery{InstanceID: r.URL.Query().Get("instance_id"), Limit: limit, Offset: offset})
	s.writeQueryResult(w, value, err, "")
}
func (s *Server) writeQueryResult(w http.ResponseWriter, value any, err error, notFound string) {
	if err != nil {
		writeNamedError(w, err, notFound)
		return
	}
	writeJSON(w, http.StatusOK, value)
}
func writeNamedError(w http.ResponseWriter, err error, notFound string) {
	if errors.Is(err, app.ErrNotFound) && notFound != "" {
		writeError(w, 404, notFound)
		return
	}
	writeAppError(w, err)
}

func (s *Server) serveStatic(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		writeError(w, http.StatusNotFound, "Not found")
		return
	}
	requested := filepath.Join(s.staticDir, filepath.Clean(r.URL.Path))
	info, err := os.Stat(requested)
	if err == nil && !info.IsDir() {
		http.ServeFile(w, r, requested)
		return
	}
	http.ServeFile(w, r, filepath.Join(s.staticDir, "index.html"))
}

const swaggerHTML = `<!doctype html><html><body><div id="swagger-ui"></div><script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:'/openapi.json',dom_id:'#swagger-ui'})</script></body></html>`

func writeOpenAPI(w http.ResponseWriter) {
	op := func(method, summary string) map[string]any {
		return map[string]any{method: map[string]any{"summary": summary, "responses": map[string]any{"200": map[string]string{"description": "Successful response"}}}}
	}
	writeJSON(w, 200, map[string]any{
		"openapi": "3.1.0",
		"info":    map[string]string{"title": "CKAN Ingestor Orchestrator API", "version": "1.0.0"},
		"paths": map[string]any{
			"/api/instances/":                  map[string]any{"get": op("get", "List instances")["get"], "post": op("post", "Create instance")["post"]},
			"/api/instances/{instance_id}":     map[string]any{"get": op("get", "Get instance")["get"], "delete": op("delete", "Delete instance")["delete"]},
			"/api/jobs/":                       op("get", "List jobs"),
			"/api/jobs/{job_id}":               op("get", "Get job"),
			"/api/jobs/{job_id}/retry":         op("post", "Retry job"),
			"/api/resources/":                  op("get", "List resources"),
			"/api/resources/{resource_id}":     op("get", "Get resource"),
			"/api/datasets/":                   op("get", "List datasets"),
			"/api/dashboard/stats":             op("get", "Dashboard statistics"),
			"/api/syncs/":                      op("get", "List metadata syncs"),
			"/api/syncs/{sync_id}":             op("get", "Get metadata sync"),
			"/api/metadata/sync":               op("post", "Sync all instances"),
			"/api/metadata/sync/{instance_id}": op("post", "Sync one instance"),
		},
	})
}

func (s *Server) handleRetry(w http.ResponseWriter, r *http.Request, id string) {
	job, err := s.dispatcher.RetryJob(r.Context(), id)
	if err != nil {
		writeAppError(w, err)
		return
	}
	resourceURL := strings.TrimRight(job.InstanceURL, "/") + "/dataset/" + job.DatasetName + "/resource/" + job.ResourceID
	encoded, _ := json.Marshal(job)
	var response map[string]any
	_ = json.Unmarshal(encoded, &response)
	response["ckan_resource_url"], response["labels"], response["results"] = resourceURL, []string{}, []any{}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request, id string) {
	sync, err := s.dispatcher.SyncInstance(r.Context(), id)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sync_id": sync.ID, "instance_id": sync.InstanceID, "status": sync.Status})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{"database": "ok", "iggy": "ok", "consumers": "ok"}
	if err := s.database.Ping(ctx); err != nil {
		checks["database"] = "unreachable"
	}
	if err := s.broker.Ping(ctx); err != nil {
		checks["iggy"] = "unreachable"
	}
	if s.ready != nil && !s.ready() {
		checks["consumers"] = "unreachable"
	}
	status := http.StatusOK
	for _, value := range checks {
		if value != "ok" {
			status = http.StatusServiceUnavailable
		}
	}
	response := map[string]string{"status": "ok"}
	if status != http.StatusOK {
		response["status"] = "error"
	}
	for key, value := range checks {
		response[key] = value
	}
	writeJSON(w, status, response)
}

func writeAppError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, app.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, app.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, app.ErrInvalidMessage):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, err.Error())
	}
}
func writeError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func (s *Server) cors(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	parsed, err := url.Parse(origin)
	requestHost, _, splitErr := net.SplitHostPort(r.Host)
	if splitErr != nil {
		requestHost = r.Host
	}
	if origin != "" && err == nil && parsed.Hostname() == requestHost {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
	}
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}
