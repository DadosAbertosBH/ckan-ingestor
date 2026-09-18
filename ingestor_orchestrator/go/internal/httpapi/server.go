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
	"net/http"
	"net/url"
	"strings"
	"time"

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
}

func New(dispatcher Dispatcher, database, broker Pinger, ready func() bool) http.Handler {
	return &Server{dispatcher: dispatcher, database: database, broker: broker, ready: ready}
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.cors(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodGet && r.URL.Path == "/ready":
		s.handleReady(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/jobs/") && strings.HasSuffix(r.URL.Path, "/retry"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/jobs/"), "/retry")
		s.handleRetry(w, r, strings.TrimSuffix(id, "/"))
	case r.Method == http.MethodPost && r.URL.Path == "/api/metadata/sync":
		writeJSON(w, http.StatusOK, s.dispatcher.SyncAll(r.Context()))
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/metadata/sync/"):
		s.handleSync(w, r, strings.TrimPrefix(r.URL.Path, "/api/metadata/sync/"))
	default:
		writeError(w, http.StatusNotFound, "Not found")
	}
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
	requestHost := strings.Split(r.Host, ":")[0]
	if origin != "" && err == nil && parsed.Hostname() == requestHost {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
	}
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}
