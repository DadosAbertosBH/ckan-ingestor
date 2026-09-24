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

package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func ptr[T any](value T) *T { return &value }

type memoryStore struct {
	jobs      map[string]*Job
	results   []*JobResult
	latest    map[string]*LatestResource
	terminal  map[string]*TerminalState
	labels    map[string]map[string]bool
	hints     map[string]string
	syncs     map[string]*MetadataSync
	instances map[string]*Instance
}

type failureStore struct{ tx Tx }

func (s failureStore) InTx(_ context.Context, fn func(Tx) error) error { return fn(s.tx) }
func (failureStore) Instances(context.Context) ([]Instance, error) {
	return nil, errors.New("instances unavailable")
}
func (failureStore) Ping(context.Context) error { return nil }

type failureTx struct {
	Tx
	fail map[string]error
}

func (t failureTx) err(operation string) error { return t.fail[operation] }
func (t failureTx) Job(ctx context.Context, id string) (*Job, error) {
	if err := t.err("job"); err != nil {
		return nil, err
	}
	return t.Tx.Job(ctx, id)
}
func (t failureTx) InsertJob(ctx context.Context, job *Job) error {
	if err := t.err("insert-job"); err != nil {
		return err
	}
	return t.Tx.InsertJob(ctx, job)
}
func (t failureTx) UpdateJob(ctx context.Context, job *Job) error {
	if err := t.err("update-job"); err != nil {
		return err
	}
	return t.Tx.UpdateJob(ctx, job)
}
func (t failureTx) InsertResult(ctx context.Context, result *JobResult) error {
	if err := t.err("insert-result"); err != nil {
		return err
	}
	return t.Tx.InsertResult(ctx, result)
}
func (t failureTx) PutCSVHint(ctx context.Context, id, value string) error {
	if err := t.err("hint"); err != nil {
		return err
	}
	return t.Tx.PutCSVHint(ctx, id, value)
}
func (t failureTx) UpdateInstance(ctx context.Context, instance *Instance) error {
	if err := t.err("update-instance"); err != nil {
		return err
	}
	return t.Tx.UpdateInstance(ctx, instance)
}

func failingProcessor(store *memoryStore, operation string) *Processor {
	return &Processor{Store: failureStore{tx: failureTx{Tx: store, fail: map[string]error{operation: errors.New(operation)}}}, Now: fixedProcessor(store).Now, NewID: fixedProcessor(store).NewID}
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		jobs: map[string]*Job{}, latest: map[string]*LatestResource{},
		terminal: map[string]*TerminalState{}, labels: map[string]map[string]bool{},
		hints: map[string]string{}, syncs: map[string]*MetadataSync{},
		instances: map[string]*Instance{},
	}
}

func (m *memoryStore) InTx(_ context.Context, fn func(Tx) error) error { return fn(m) }
func (m *memoryStore) Instances(context.Context) ([]Instance, error) {
	values := make([]Instance, 0, len(m.instances))
	for _, value := range m.instances {
		values = append(values, *value)
	}
	return values, nil
}
func (m *memoryStore) Ping(context.Context) error { return nil }
func (m *memoryStore) Job(_ context.Context, id string) (*Job, error) {
	job := m.jobs[id]
	if job == nil {
		return nil, ErrNotFound
	}
	return job, nil
}
func (m *memoryStore) InsertJob(_ context.Context, job *Job) error { m.jobs[job.ID] = job; return nil }
func (m *memoryStore) UpdateJob(context.Context, *Job) error       { return nil }
func (m *memoryStore) InsertResult(_ context.Context, result *JobResult) error {
	m.results = append(m.results, result)
	return nil
}
func (m *memoryStore) Latest(_ context.Context, id string) (*LatestResource, error) {
	value := m.latest[id]
	if value == nil {
		return nil, ErrNotFound
	}
	return value, nil
}
func (m *memoryStore) PutLatest(_ context.Context, value *LatestResource) error {
	m.latest[value.ResourceID] = value
	return nil
}
func (m *memoryStore) Terminal(_ context.Context, id string) (*TerminalState, error) {
	value := m.terminal[id]
	if value == nil {
		return nil, ErrNotFound
	}
	return value, nil
}
func (m *memoryStore) PutTerminal(_ context.Context, value *TerminalState) error {
	m.terminal[value.ResourceID] = value
	return nil
}
func (m *memoryStore) AddLabel(_ context.Context, id, label string) error {
	if m.labels[id] == nil {
		m.labels[id] = map[string]bool{}
	}
	m.labels[id][label] = true
	return nil
}
func (m *memoryStore) RemoveLabel(_ context.Context, id, label string) error {
	delete(m.labels[id], label)
	return nil
}
func (m *memoryStore) RemoveLabelsWithPrefix(_ context.Context, id, prefix string) error {
	for label := range m.labels[id] {
		if strings.HasPrefix(label, prefix) {
			delete(m.labels[id], label)
		}
	}
	return nil
}
func (m *memoryStore) PutCSVHint(_ context.Context, id, value string) error {
	m.hints[id] = value
	return nil
}
func (m *memoryStore) CSVHint(_ context.Context, id string) (*string, error) {
	value, ok := m.hints[id]
	if !ok {
		return nil, ErrNotFound
	}
	return &value, nil
}
func (m *memoryStore) Sync(_ context.Context, id string) (*MetadataSync, error) {
	value := m.syncs[id]
	if value == nil {
		return nil, ErrNotFound
	}
	return value, nil
}
func (m *memoryStore) InsertSync(_ context.Context, value *MetadataSync) error {
	m.syncs[value.ID] = value
	return nil
}
func (m *memoryStore) UpdateSync(context.Context, *MetadataSync) error { return nil }
func (m *memoryStore) Instance(_ context.Context, id string) (*Instance, error) {
	value := m.instances[id]
	if value == nil {
		return nil, ErrNotFound
	}
	return value, nil
}
func (m *memoryStore) UpdateInstance(context.Context, *Instance) error { return nil }

func fixedProcessor(store *memoryStore) *Processor {
	return &Processor{Store: store, Now: func() time.Time { return time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC) }, NewID: func() string { return "result-1" }}
}

func TestApplyPendingCreatesCoordinatedJobAndLatestProjection(t *testing.T) {
	store := newMemoryStore()
	processor := fixedProcessor(store)
	err := processor.ApplyJobResult(context.Background(), JobResultMessage{
		JobID: "job-1", Status: "PENDING", ResourceID: "resource-1",
		DatasetName: ptr("dataset"), InstanceID: ptr("instance-1"), CKANURL: ptr("https://example.test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.jobs["job-1"] == nil || store.jobs["job-1"].Status != JobPending {
		t.Fatalf("job not created: %#v", store.jobs)
	}
	if store.latest["resource-1"] == nil || store.latest["resource-1"].LatestJobID != "job-1" {
		t.Fatalf("latest not created: %#v", store.latest)
	}
}

func TestApplySuccessStoresResultTerminalLabelsAndHint(t *testing.T) {
	store := newMemoryStore()
	store.jobs["job-1"] = &Job{ID: "job-1", ResourceID: "resource-1", InstanceID: "instance-1", DatasetName: "dataset", Status: JobProcessing}
	store.latest["resource-1"] = &LatestResource{ResourceID: "resource-1", LatestJobID: "job-1"}
	processor := fixedProcessor(store)
	rows, expectedRows, size, expectedColumns := int64(1), int64(2), int64(100), int64(2)
	err := processor.ApplyJobResult(context.Background(), JobResultMessage{
		JobID: "job-1", Status: "SUCCESS", ResourceID: "resource-1", RowsProcessed: &rows,
		ExpectedRows: &expectedRows, ResourceSize: &size, ExpectedColumns: &expectedColumns,
		Encoding: ptr("latin-1"), CSVStrictMode: ptr(false), CSVDelimiter: ptr(";"),
		CSVSamples: ptr("800000"), Reader: ptr("csv"), DatastoreActive: ptr(true),
		Preview: []byte(`[{"id":"1"}]`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.jobs["job-1"].Status != JobCompleted || len(store.results) != 1 {
		t.Fatalf("result not applied: %#v %#v", store.jobs["job-1"], store.results)
	}
	for _, label := range []string{"single-row", "single-column", "row-count-mismatch", "column-count-mismatch", "size:small", "encoding:latin-1", "csv-strict-mode:false", "csv-delimiter:;", "csv-samples:800000", "reader:csv", "datastore"} {
		if !store.labels["resource-1"][label] {
			t.Errorf("missing label %q", label)
		}
	}
	if store.hints["resource-1"] != ";" {
		t.Fatalf("hint = %q", store.hints["resource-1"])
	}
	if store.terminal["resource-1"].LastTerminalStatus != JobCompleted {
		t.Fatalf("terminal = %#v", store.terminal["resource-1"])
	}
}

func TestApplyFailureTruncatesErrorAndIsIdempotent(t *testing.T) {
	store := newMemoryStore()
	store.jobs["job-1"] = &Job{ID: "job-1", ResourceID: "resource-1", Status: JobPending}
	processor := fixedProcessor(store)
	message := JobResultMessage{JobID: "job-1", Status: "FAILED", ResourceID: "resource-1", ErrorMessage: ptr(string(make([]byte, 17000)))}
	if err := processor.ApplyJobResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if err := processor.ApplyJobResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(store.results) != 1 || len(*store.results[0].ErrorMessage) != 16000 {
		t.Fatalf("results = %#v", store.results)
	}
}

func TestApplyFailureRemovesEmptyLabel(t *testing.T) {
	store := newMemoryStore()
	store.jobs["job-1"] = &Job{ID: "job-1", ResourceID: "resource-1", Status: JobProcessing}
	store.labels["resource-1"] = map[string]bool{"empty": true}

	if err := fixedProcessor(store).ApplyJobResult(context.Background(), JobResultMessage{
		JobID: "job-1", ResourceID: "resource-1", Status: "FAILED",
	}); err != nil {
		t.Fatal(err)
	}
	if store.labels["resource-1"]["empty"] {
		t.Fatal("empty label was retained after failure")
	}
}

func TestApplyFailureReplacesHTTPStatusLabel(t *testing.T) {
	store := newMemoryStore()
	store.jobs["job-1"] = &Job{ID: "job-1", ResourceID: "resource-1", Status: JobProcessing}
	store.labels["resource-1"] = map[string]bool{"empty": true, "http-code:500": true}
	status := int64(403)

	if err := fixedProcessor(store).ApplyJobResult(context.Background(), JobResultMessage{
		JobID: "job-1", ResourceID: "resource-1", Status: "FAILED", HTTPStatus: &status,
	}); err != nil {
		t.Fatal(err)
	}
	if store.labels["resource-1"]["empty"] || store.labels["resource-1"]["http-code:500"] {
		t.Fatalf("stale labels were retained: %#v", store.labels["resource-1"])
	}
	if !store.labels["resource-1"]["http-code:403"] {
		t.Fatalf("HTTP status label missing: %#v", store.labels["resource-1"])
	}
}

func TestApplyDeletedRemovesResultStateLabels(t *testing.T) {
	store := newMemoryStore()
	store.jobs["job-1"] = &Job{ID: "job-1", ResourceID: "resource-1", Status: JobProcessing}
	store.labels["resource-1"] = map[string]bool{"empty": true, "http-code:500": true}

	if err := fixedProcessor(store).ApplyJobResult(context.Background(), JobResultMessage{
		JobID: "job-1", ResourceID: "resource-1", Status: "DELETED",
	}); err != nil {
		t.Fatal(err)
	}
	if store.labels["resource-1"]["empty"] || store.labels["resource-1"]["http-code:500"] {
		t.Fatalf("result state labels were retained: %#v", store.labels["resource-1"])
	}
}

func TestApplySuccessRemovesHTTPStatusLabel(t *testing.T) {
	store := newMemoryStore()
	store.jobs["job-1"] = &Job{ID: "job-1", ResourceID: "resource-1", Status: JobProcessing}
	store.labels["resource-1"] = map[string]bool{"http-code:500": true}
	rows := int64(2)

	if err := fixedProcessor(store).ApplyJobResult(context.Background(), JobResultMessage{
		JobID: "job-1", ResourceID: "resource-1", Status: "SUCCESS", RowsProcessed: &rows,
	}); err != nil {
		t.Fatal(err)
	}
	if store.labels["resource-1"]["http-code:500"] {
		t.Fatalf("HTTP status label was retained: %#v", store.labels["resource-1"])
	}
}

func TestApplyMetadataSuccessAndDuplicate(t *testing.T) {
	store := newMemoryStore()
	store.syncs["sync-1"] = &MetadataSync{ID: "sync-1", InstanceID: "instance-1", Status: "pending"}
	store.instances["instance-1"] = &Instance{ID: "instance-1"}
	processor := fixedProcessor(store)
	message := MetadataSyncResultMessage{SyncID: "sync-1", Status: "success", DatasetCount: 7, ResourceCount: 9, TotalPackages: 10, OutdatedResources: 5, DeletedResources: 2}
	if err := processor.ApplyMetadataResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if err := processor.ApplyMetadataResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if store.syncs["sync-1"].Status != "success" || store.instances["instance-1"].DatasetCount != 7 {
		t.Fatalf("sync not applied: %#v %#v", store.syncs, store.instances)
	}
	if store.syncs["sync-1"].OutdatedResources != 5 {
		t.Fatalf("outdated resources not applied: %#v", store.syncs["sync-1"])
	}
}

func TestUnknownStatusesAreRejected(t *testing.T) {
	store := newMemoryStore()
	processor := fixedProcessor(store)
	if err := processor.ApplyJobResult(context.Background(), JobResultMessage{Status: "BOGUS"}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("error = %v", err)
	}
}

func TestCoordinatorSuccessWithoutJobIDIsAcknowledged(t *testing.T) {
	store := newMemoryStore()
	if err := fixedProcessor(store).ApplyJobResult(context.Background(), JobResultMessage{Status: "SUCCESS", ResourceID: "resource"}); err != nil {
		t.Fatal(err)
	}
}

func TestProcessingAndDeletedTransitions(t *testing.T) {
	store := newMemoryStore()
	store.jobs["job"] = &Job{ID: "job", ResourceID: "resource", Status: JobPending}
	store.latest["resource"] = &LatestResource{ResourceID: "resource", LatestJobID: "job"}
	processor := fixedProcessor(store)
	if err := processor.ApplyJobResult(context.Background(), JobResultMessage{JobID: "job", ResourceID: "resource", Status: "PROCESSING"}); err != nil {
		t.Fatal(err)
	}
	if store.jobs["job"].Status != JobProcessing || store.latest["resource"].Status != "processing" {
		t.Fatalf("processing transition failed")
	}
	if err := processor.ApplyJobResult(context.Background(), JobResultMessage{JobID: "job", ResourceID: "resource", Status: "DELETED"}); err != nil {
		t.Fatal(err)
	}
	if err := processor.ApplyJobResult(context.Background(), JobResultMessage{JobID: "job", ResourceID: "resource", Status: "DELETED"}); err != nil {
		t.Fatal(err)
	}
	if store.jobs["job"].Status != JobDeleted || len(store.results) != 1 || store.terminal["resource"].LastTerminalStatus != JobDeleted {
		t.Fatalf("deleted transition failed: %#v", store)
	}
}

func TestSuccessLabelBoundariesAndEmptyState(t *testing.T) {
	for _, item := range []struct {
		size  int64
		label string
	}{{1_000_000, "size:medium"}, {1_000_000_000, "size:large"}} {
		store := newMemoryStore()
		id := item.label
		store.jobs[id] = &Job{ID: id, ResourceID: id, Status: JobPending}
		message := JobResultMessage{JobID: id, ResourceID: id, Status: "SUCCESS", RowsProcessed: ptr(int64(2)), ResourceSize: &item.size, Encoding: ptr("utf-8"), Preview: []byte(`[{"a":1,"b":2}]`)}
		if err := fixedProcessor(store).ApplyJobResult(context.Background(), message); err != nil {
			t.Fatal(err)
		}
		if !store.labels[id][item.label] || store.labels[id]["encoding:utf-8"] {
			t.Fatalf("labels = %#v", store.labels[id])
		}
	}
	store := newMemoryStore()
	store.jobs["empty"] = &Job{ID: "empty", ResourceID: "resource", Status: JobPending}
	if err := fixedProcessor(store).ApplyJobResult(context.Background(), JobResultMessage{JobID: "empty", ResourceID: "resource", Status: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}
	if !store.labels["resource"]["empty"] {
		t.Fatal("empty label missing")
	}
}

func TestPendingUsesPreviousTerminalProjection(t *testing.T) {
	for _, item := range []struct {
		terminal JobStatus
		expected string
	}{{JobCompleted, "outdated"}, {JobFailed, "failed"}} {
		store := newMemoryStore()
		store.terminal["resource"] = &TerminalState{ResourceID: "resource", LastTerminalStatus: item.terminal}
		message := JobResultMessage{JobID: string(item.terminal), ResourceID: "resource", Status: "PENDING", DatasetName: ptr("dataset"), InstanceID: ptr("instance")}
		if err := fixedProcessor(store).ApplyJobResult(context.Background(), message); err != nil {
			t.Fatal(err)
		}
		if store.latest["resource"].Status != item.expected {
			t.Fatalf("status = %q", store.latest["resource"].Status)
		}
	}
}

func TestProcessingReopensTerminalJobForReprocessing(t *testing.T) {
	for _, terminal := range []JobStatus{JobFailed, JobCompleted} {
		completed := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
		store := newMemoryStore()
		store.jobs["job"] = &Job{ID: "job", ResourceID: "resource", Status: terminal, CompletedAt: &completed}
		store.latest["resource"] = &LatestResource{ResourceID: "resource", LatestJobID: "job", Status: string(terminal)}
		store.terminal["resource"] = &TerminalState{ResourceID: "resource", LastTerminalJobID: "job", LastTerminalStatus: terminal}
		processor := fixedProcessor(store)

		// The discovery announcement stays idempotent: it never reopens a finished job.
		message := JobResultMessage{JobID: "job", ResourceID: "resource", Status: "PENDING", DatasetName: ptr("dataset"), InstanceID: ptr("instance")}
		if err := processor.ApplyJobResult(context.Background(), message); err != nil {
			t.Fatal(err)
		}
		if store.jobs["job"].Status != terminal {
			t.Fatalf("PENDING reopened a terminal job: %#v", store.jobs["job"])
		}

		// A worker starting the new attempt reopens the job for reprocessing.
		if err := processor.ApplyJobResult(context.Background(), JobResultMessage{JobID: "job", ResourceID: "resource", Status: "PROCESSING"}); err != nil {
			t.Fatal(err)
		}
		if store.jobs["job"].Status != JobProcessing || store.jobs["job"].StartedAt == nil || store.jobs["job"].CompletedAt != nil {
			t.Fatalf("PROCESSING did not reopen the job: %#v", store.jobs["job"])
		}
		if store.latest["resource"].Status != string(JobProcessing) {
			t.Fatalf("latest = %q", store.latest["resource"].Status)
		}

		// The terminal guards no longer block the reopened attempt's outcome.
		if err := processor.ApplyJobResult(context.Background(), JobResultMessage{JobID: "job", ResourceID: "resource", Status: "SUCCESS", RowsProcessed: ptr(int64(2))}); err != nil {
			t.Fatal(err)
		}
		if store.jobs["job"].Status != JobCompleted || store.latest["resource"].Status != string(JobCompleted) {
			t.Fatalf("success did not advance: job=%#v latest=%#v", store.jobs["job"], store.latest["resource"])
		}
	}
}

func TestPendingKeepsExistingJobUntouched(t *testing.T) {
	for _, status := range []JobStatus{JobPending, JobProcessing, JobFailed, JobCompleted, JobDeleted} {
		store := newMemoryStore()
		store.jobs["job"] = &Job{ID: "job", ResourceID: "resource", Status: status}
		message := JobResultMessage{JobID: "job", ResourceID: "resource", Status: "PENDING", DatasetName: ptr("dataset"), InstanceID: ptr("instance")}
		if err := fixedProcessor(store).ApplyJobResult(context.Background(), message); err != nil {
			t.Fatal(err)
		}
		if store.jobs["job"].Status != status {
			t.Fatalf("PENDING changed status %q -> %q", status, store.jobs["job"].Status)
		}
	}
}

func TestInvalidPendingAndPreviewAreRejected(t *testing.T) {
	processor := fixedProcessor(newMemoryStore())
	if err := processor.ApplyJobResult(context.Background(), JobResultMessage{JobID: "job", ResourceID: "resource", Status: "PENDING"}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("error = %v", err)
	}
	store := newMemoryStore()
	store.jobs["job"] = &Job{ID: "job", ResourceID: "resource", Status: JobPending}
	if err := fixedProcessor(store).ApplyJobResult(context.Background(), JobResultMessage{JobID: "job", ResourceID: "resource", Status: "SUCCESS", Preview: []byte(`{`)}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("error = %v", err)
	}
}

func TestMetadataFailureUnknownAndMissingInstance(t *testing.T) {
	store := newMemoryStore()
	processor := fixedProcessor(store)
	if err := processor.ApplyMetadataResult(context.Background(), MetadataSyncResultMessage{SyncID: "unknown", Status: "failure"}); err != nil {
		t.Fatal(err)
	}
	store.syncs["failed"] = &MetadataSync{ID: "failed", InstanceID: "instance", Status: "pending"}
	long := string(make([]byte, 17000))
	if err := processor.ApplyMetadataResult(context.Background(), MetadataSyncResultMessage{SyncID: "failed", Status: "failure", ErrorMessage: &long}); err != nil {
		t.Fatal(err)
	}
	if store.syncs["failed"].Status != "failure" || len(*store.syncs["failed"].ErrorMessage) != 16000 {
		t.Fatalf("sync = %#v", store.syncs["failed"])
	}
	store.syncs["missing-instance"] = &MetadataSync{ID: "missing-instance", InstanceID: "missing", Status: "pending"}
	if err := processor.ApplyMetadataResult(context.Background(), MetadataSyncResultMessage{SyncID: "missing-instance", Status: "success"}); err != nil {
		t.Fatal(err)
	}
	if store.syncs["missing-instance"].Status != "failure" {
		t.Fatalf("sync = %#v", store.syncs["missing-instance"])
	}
}

func TestPendingIsIdempotentAndAcceptsOptionalCoordinatorFields(t *testing.T) {
	store := newMemoryStore()
	processor := fixedProcessor(store)
	message := JobResultMessage{
		JobID: "job", ResourceID: "resource", Status: "PENDING",
		DatasetName: ptr("dataset"), InstanceID: ptr("instance"),
		CKANURL: ptr("https://ckan.test"), DatastoreActive: ptr(true),
	}
	if err := processor.ApplyJobResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if err := processor.ApplyJobResult(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(store.jobs) != 1 || !store.jobs["job"].DatastoreActive || store.jobs["job"].CKANURL != "https://ckan.test" {
		t.Fatalf("job = %#v", store.jobs["job"])
	}
}

func TestTerminalStatesAreIdempotentAndDoNotOverwriteLatestAttempt(t *testing.T) {
	store := newMemoryStore()
	store.jobs["old"] = &Job{ID: "old", ResourceID: "resource", Status: JobPending}
	store.jobs["new"] = &Job{ID: "new", ResourceID: "resource", Status: JobProcessing}
	store.latest["resource"] = &LatestResource{ResourceID: "resource", LatestJobID: "new"}
	processor := fixedProcessor(store)
	if err := processor.ApplyJobResult(context.Background(), JobResultMessage{JobID: "old", ResourceID: "resource", Status: "FAILED"}); err != nil {
		t.Fatal(err)
	}
	if store.latest["resource"].LatestJobID != "new" {
		t.Fatalf("latest = %#v", store.latest["resource"])
	}
	if err := processor.ApplyJobResult(context.Background(), JobResultMessage{JobID: "old", ResourceID: "resource", Status: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}
	if len(store.results) != 1 || store.terminal["resource"].LastTerminalStatus != JobFailed {
		t.Fatalf("terminal = %#v results=%#v", store.terminal["resource"], store.results)
	}
}

func TestSuccessRemovesStaleEmptyLabelAndHandlesNullPreview(t *testing.T) {
	store := newMemoryStore()
	store.jobs["job"] = &Job{ID: "job", ResourceID: "resource", Status: JobPending}
	store.labels["resource"] = map[string]bool{"empty": true}
	rows := int64(2)
	if err := fixedProcessor(store).ApplyJobResult(context.Background(), JobResultMessage{JobID: "job", ResourceID: "resource", Status: "SUCCESS", RowsProcessed: &rows, Preview: []byte("null")}); err != nil {
		t.Fatal(err)
	}
	if store.labels["resource"]["empty"] {
		t.Fatal("stale empty label was retained")
	}
}

func TestTransactionErrorsAreReturnedWithoutAcknowledgingMessages(t *testing.T) {
	for _, item := range []struct {
		name, operation string
		message         JobResultMessage
		setup           func(*memoryStore)
	}{
		{"pending insert", "insert-job", JobResultMessage{JobID: "job", ResourceID: "resource", Status: "PENDING", DatasetName: ptr("dataset"), InstanceID: ptr("instance")}, func(*memoryStore) {}},
		{"processing update", "update-job", JobResultMessage{JobID: "job", ResourceID: "resource", Status: "PROCESSING"}, func(s *memoryStore) { s.jobs["job"] = &Job{ID: "job", ResourceID: "resource", Status: JobPending} }},
		{"failure result", "insert-result", JobResultMessage{JobID: "job", ResourceID: "resource", Status: "FAILED"}, func(s *memoryStore) { s.jobs["job"] = &Job{ID: "job", ResourceID: "resource", Status: JobPending} }},
		{"success hint", "hint", JobResultMessage{JobID: "job", ResourceID: "resource", Status: "SUCCESS", CSVDelimiter: ptr(";")}, func(s *memoryStore) { s.jobs["job"] = &Job{ID: "job", ResourceID: "resource", Status: JobPending} }},
	} {
		t.Run(item.name, func(t *testing.T) {
			store := newMemoryStore()
			item.setup(store)
			if err := failingProcessor(store, item.operation).ApplyJobResult(context.Background(), item.message); err == nil {
				t.Fatal("expected transaction error")
			}
		})
	}
}

func TestMetadataInstanceUpdateErrorIsReturned(t *testing.T) {
	store := newMemoryStore()
	store.syncs["sync"] = &MetadataSync{ID: "sync", InstanceID: "instance", Status: "pending"}
	store.instances["instance"] = &Instance{ID: "instance"}
	if err := failingProcessor(store, "update-instance").ApplyMetadataResult(context.Background(), MetadataSyncResultMessage{SyncID: "sync", Status: "success"}); err == nil {
		t.Fatal("expected update error")
	}
}
