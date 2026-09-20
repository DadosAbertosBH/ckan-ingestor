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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/app"
)

type fakeDispatcher struct {
	retry    *app.Job
	retryErr error
	retryFn  func(string) (*app.Job, error)
	retryIDs []string
	sync     *app.MetadataSync
	syncErr  error
}

func (f *fakeDispatcher) RetryJob(_ context.Context, id string) (*app.Job, error) {
	f.retryIDs = append(f.retryIDs, id)
	if f.retryFn != nil {
		return f.retryFn(id)
	}
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

type fakeQueries struct {
	instances     []app.Instance
	jobs          []app.JobView
	resources     []app.ResourceView
	datasets      []app.DatasetView
	stats         []app.InstanceStats
	syncs         []app.MetadataSync
	err           error
	jobQuery      app.JobQuery
	resourceQuery app.ResourceQuery
	created       *app.Instance
}

func (f *fakeQueries) ListInstances(context.Context) ([]app.Instance, error) {
	return f.instances, f.err
}
func (f *fakeQueries) GetInstance(_ context.Context, id string) (*app.Instance, error) {
	for i := range f.instances {
		if f.instances[i].ID == id {
			return &f.instances[i], nil
		}
	}
	return nil, app.ErrNotFound
}
func (f *fakeQueries) CreateInstance(_ context.Context, instance *app.Instance) error {
	f.created = instance
	return f.err
}
func (f *fakeQueries) DeleteInstance(context.Context, string) error { return f.err }
func (f *fakeQueries) ListJobs(_ context.Context, query app.JobQuery) ([]app.JobView, error) {
	f.jobQuery = query
	return f.jobs, f.err
}
func (f *fakeQueries) GetJob(_ context.Context, id string) (*app.JobView, error) {
	for i := range f.jobs {
		if f.jobs[i].ID == id {
			value := f.jobs[i]
			if value.Results == nil {
				empty := []app.JobResult{}
				value.Results = &empty
			}
			return &value, nil
		}
	}
	return nil, app.ErrNotFound
}
func (f *fakeQueries) ListResources(_ context.Context, query app.ResourceQuery) ([]app.ResourceView, error) {
	f.resourceQuery = query
	return f.resources, f.err
}
func (f *fakeQueries) GetResource(_ context.Context, id string) (*app.ResourceView, error) {
	for i := range f.resources {
		if f.resources[i].ResourceID == id {
			return &f.resources[i], nil
		}
	}
	return nil, app.ErrNotFound
}
func (f *fakeQueries) ListDatasets(_ context.Context, query app.ResourceQuery) ([]app.DatasetView, error) {
	f.resourceQuery = query
	return f.datasets, f.err
}
func (f *fakeQueries) Dashboard(context.Context) ([]app.InstanceStats, error) { return f.stats, f.err }
func (f *fakeQueries) ListSyncs(context.Context, app.SyncQuery) ([]app.MetadataSync, error) {
	return f.syncs, f.err
}
func (f *fakeQueries) GetSync(_ context.Context, id string) (*app.MetadataSync, error) {
	for i := range f.syncs {
		if f.syncs[i].ID == id {
			return &f.syncs[i], nil
		}
	}
	return nil, app.ErrNotFound
}

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

func TestRetryBatchReturnsSuccessesAndFailures(t *testing.T) {
	dispatcher := &fakeDispatcher{retryFn: func(id string) (*app.Job, error) {
		if id == "missing" {
			return nil, app.ErrNotFound
		}
		return &app.Job{ID: "retry-" + id}, nil
	}}
	handler := New(dispatcher, fakePinger{}, fakePinger{}, func() bool { return true })
	request := httptest.NewRequest(http.MethodPost, "/api/jobs/retry", strings.NewReader(`{"job_ids":["completed","missing"]}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Jobs     []app.Job `json:"jobs"`
		Failures []struct {
			JobID string `json:"job_id"`
			Error string `json:"error"`
		} `json:"failures"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Jobs) != 1 || body.Jobs[0].ID != "retry-completed" || len(body.Failures) != 1 || body.Failures[0].JobID != "missing" {
		t.Fatalf("body = %#v", body)
	}
	if got := strings.Join(dispatcher.retryIDs, ","); got != "completed,missing" {
		t.Fatalf("retry IDs = %s", got)
	}
}

func TestRetryBatchRejectsEmptySelection(t *testing.T) {
	handler := New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return true })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/jobs/retry", strings.NewReader(`{"job_ids":[]}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
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
	if !strings.Contains(response.Header().Get("Access-Control-Allow-Methods"), http.MethodDelete) {
		t.Fatalf("DELETE missing from CORS methods: %s", response.Header().Get("Access-Control-Allow-Methods"))
	}
}

func TestCORSPreflightAcceptsSameIPv6Origin(t *testing.T) {
	handler := New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return true })
	request := httptest.NewRequest(http.MethodOptions, "http://[::1]:8081/api/jobs/id/retry", nil)
	request.Header.Set("Origin", "http://[::1]:8000")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Origin") != "http://[::1]:8000" {
		t.Fatalf("status=%d headers=%#v", response.Code, response.Header())
	}
}

func TestReadAPIRoutesAndQueryParameters(t *testing.T) {
	queries := &fakeQueries{
		instances: []app.Instance{{ID: "i"}}, jobs: []app.JobView{{Job: app.Job{ID: "j"}}},
		resources: []app.ResourceView{{ResourceID: "r"}}, datasets: []app.DatasetView{{DatasetName: "d"}},
		stats: []app.InstanceStats{{}}, syncs: []app.MetadataSync{{ID: "s"}},
	}
	handler := New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return true }, WithQueries(queries))
	for _, path := range []string{
		"/api/instances/", "/api/instances/i", "/api/jobs/?status=failed&tags=a,b&limit=12&offset=3&order_by=duration&order_dir=asc",
		"/api/jobs/j", "/api/resources/?status=outdated&instance_id=i&search=csv&limit=9&offset=2", "/api/resources/r",
		"/api/datasets/?instance_id=i", "/api/dashboard/stats", "/api/syncs/", "/api/syncs/s",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	if queries.jobQuery.Limit != 12 || queries.jobQuery.Offset != 3 || queries.jobQuery.Tags != "a,b" {
		t.Fatalf("job query=%+v", queries.jobQuery)
	}
	if queries.resourceQuery.InstanceID != "i" {
		t.Fatalf("resource query=%+v", queries.resourceQuery)
	}
}

func TestJobListOmitsResultsAndDetailIncludesEmptyResults(t *testing.T) {
	queries := &fakeQueries{jobs: []app.JobView{{Job: app.Job{ID: "j"}}}}
	handler := New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return true }, WithQueries(queries))
	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/jobs/", nil))
	if strings.Contains(list.Body.String(), `"results"`) {
		t.Fatalf("list contains detail-only results: %s", list.Body.String())
	}
	detail := httptest.NewRecorder()
	handler.ServeHTTP(detail, httptest.NewRequest(http.MethodGet, "/api/jobs/j", nil))
	if !strings.Contains(detail.Body.String(), `"results":[]`) {
		t.Fatalf("detail does not contain results: %s", detail.Body.String())
	}
}

func TestInstanceCreateDeleteAndValidation(t *testing.T) {
	queries := &fakeQueries{}
	handler := New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return true }, WithQueries(queries))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/instances/", strings.NewReader(`{"name":"Portal","url":"https://dados.test/"}`)))
	if response.Code != http.StatusCreated || queries.created == nil || queries.created.URL != "https://dados.test/" || queries.created.ID == "" {
		t.Fatalf("status=%d body=%s created=%+v", response.Code, response.Body.String(), queries.created)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/api/instances/i", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d", response.Code)
	}
	for _, path := range []string{"/api/jobs/?limit=0", "/api/jobs/?status=outdated", "/api/resources/?limit=501", "/api/resources/?status=unknown"} {
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%s status=%d", path, response.Code)
		}
	}
}

func TestKnownRouteWithWrongMethodReturnsMethodNotAllowed(t *testing.T) {
	handler := New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return true }, WithQueries(&fakeQueries{}))
	for _, item := range []struct{ method, path string }{{http.MethodGet, "/api/jobs/id/retry"}, {http.MethodPost, "/api/jobs/"}, {http.MethodDelete, "/api/resources/id"}} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(item.method, item.path, nil))
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s status=%d body=%s", item.method, item.path, response.Code, response.Body.String())
		}
	}
}

func TestMethodNotAllowedIncludesAllowedMethods(t *testing.T) {
	handler := New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return true }, WithQueries(&fakeQueries{}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/jobs/", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Header().Get("Allow"), http.MethodGet) {
		t.Fatalf("Allow=%q", response.Header().Get("Allow"))
	}
}

func TestServesSPAAndOpenAPI(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "index.html"), []byte("<main>app</main>"), 0o600); err != nil {
		t.Fatal(err)
	}
	handler := New(&fakeDispatcher{}, fakePinger{}, fakePinger{}, func() bool { return true }, WithStaticDir(directory))
	for _, path := range []string{"/", "/resources/r", "/openapi.json", "/docs"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		body, _ := io.ReadAll(response.Result().Body)
		if response.Code != http.StatusOK || len(body) == 0 {
			t.Fatalf("%s status=%d body=%s", path, response.Code, body)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/unknown", nil))
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"detail":"Not found"`) {
		t.Fatalf("API fallback status=%d body=%s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	var document struct {
		Paths map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/instances/", "/api/instances/{instance_id}", "/api/jobs/", "/api/jobs/{job_id}", "/api/jobs/{job_id}/retry", "/api/resources/", "/api/resources/{resource_id}", "/api/datasets/", "/api/dashboard/stats", "/api/syncs/", "/api/syncs/{sync_id}", "/api/metadata/sync", "/api/metadata/sync/{instance_id}"} {
		if _, ok := document.Paths[path]; !ok {
			t.Errorf("OpenAPI is missing %s", path)
		}
	}
}
