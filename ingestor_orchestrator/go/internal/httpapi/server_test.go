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
	"net/http/httptest"
	"testing"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/go/internal/app"
)

type fakeDispatcher struct {
	retry    *app.Job
	retryErr error
	sync     *app.MetadataSync
	syncErr  error
}

func (f *fakeDispatcher) RetryJob(context.Context, string) (*app.Job, error) {
	return f.retry, f.retryErr
}
func (f *fakeDispatcher) SyncInstance(context.Context, string) (*app.MetadataSync, error) {
	return f.sync, f.syncErr
}
func (f *fakeDispatcher) SyncAll(context.Context) []app.SyncDispatchResult {
	return []app.SyncDispatchResult{{SyncID: "sync", InstanceID: "instance", Status: "pending"}}
}

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func TestRetryRouteReturnsCompatibleJobResponse(t *testing.T) {
	partition := 2
	topic := "jobs-retry"
	handler := New(&fakeDispatcher{retry: &app.Job{ID: "retry", ResourceID: "resource", DatasetName: "dataset", InstanceID: "instance", InstanceURL: "https://example.test", Status: app.JobPending, MessageTopic: &topic, MessagePartition: &partition}}, fakePinger{}, fakePinger{}, func() bool { return true })
	request := httptest.NewRequest(http.MethodPost, "/api/jobs/failed/retry", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["id"] != "retry" || body["ckan_resource_url"] != "https://example.test/dataset/dataset/resource/resource" {
		t.Fatalf("body = %#v", body)
	}
}

func TestRetryErrorsUseDetailAndStatus(t *testing.T) {
	for _, item := range []struct {
		err    error
		status int
	}{{app.ErrNotFound, 404}, {app.ErrConflict, 409}, {errors.New("iggy down"), 503}} {
		handler := New(&fakeDispatcher{retryErr: item.err}, fakePinger{}, fakePinger{}, func() bool { return true })
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/jobs/id/retry", nil))
		if response.Code != item.status {
			t.Fatalf("error %v: status = %d", item.err, response.Code)
		}
	}
}

func TestMetadataAndHealthRoutes(t *testing.T) {
	handler := New(&fakeDispatcher{sync: &app.MetadataSync{ID: "sync", InstanceID: "instance", Status: "pending"}}, fakePinger{}, fakePinger{}, func() bool { return true })
	for _, path := range []string{"/api/metadata/sync", "/api/metadata/sync/instance", "/health", "/ready"} {
		method := http.MethodPost
		if path == "/health" || path == "/ready" {
			method = http.MethodGet
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, path, nil))
		if response.Code != 200 {
			t.Fatalf("%s status = %d body=%s", path, response.Code, response.Body.String())
		}
	}
}

func TestReadyFailsWhenConsumerOrDependencyIsDown(t *testing.T) {
	for _, handler := range []http.Handler{
		New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return false }),
		New(&fakeDispatcher{}, fakePinger{err: errors.New("db")}, fakePinger{}, func() bool { return true }),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
		if response.Code != 503 {
			t.Fatalf("status = %d", response.Code)
		}
	}
}

func TestSyncErrorsAndUnknownRoutes(t *testing.T) {
	for _, item := range []struct {
		path   string
		method string
		err    error
		status int
	}{
		{"/api/metadata/sync/instance", http.MethodPost, app.ErrNotFound, http.StatusNotFound},
		{"/api/metadata/sync/instance", http.MethodPost, app.ErrInvalidMessage, http.StatusUnprocessableEntity},
		{"/missing", http.MethodGet, nil, http.StatusNotFound},
	} {
		handler := New(&fakeDispatcher{syncErr: item.err}, fakePinger{}, fakePinger{}, func() bool { return true })
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(item.method, item.path, nil))
		if response.Code != item.status {
			t.Fatalf("%s: status=%d body=%s", item.path, response.Code, response.Body.String())
		}
	}
}

func TestCORSPreflightAcceptsSameHostOrigin(t *testing.T) {
	handler := New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return true })
	request := httptest.NewRequest(http.MethodOptions, "http://api.test/api/jobs/id/retry", nil)
	request.Header.Set("Origin", "https://api.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Origin") != "https://api.test" {
		t.Fatalf("status=%d headers=%#v", response.Code, response.Header())
	}
}
