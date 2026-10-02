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
	"encoding/json"
	"errors"
	"testing"
)

type published struct {
	topic, key string
	payload    []byte
}

func TestSICOMSyncIsPausedByDefault(t *testing.T) {
	store := newMemoryStore()
	store.instances["sicom"] = &Instance{ID: "sicom", SourceType: "sicom"}
	pub := &fakePublisher{}
	d := &Dispatcher{Store: store, Publisher: pub, MetadataTopic: "sync"}
	if _, err := d.SyncInstance(context.Background(), "sicom"); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected paused source conflict, got %v", err)
	}
	if len(pub.messages) != 0 || len(store.syncs) != 0 {
		t.Fatal("paused SICOM source created work")
	}
}

func TestSyncAllSkipsPausedSICOMAndKeepsCKAN(t *testing.T) {
	store := newMemoryStore()
	store.instances["sicom"] = &Instance{ID: "sicom", SourceType: "sicom"}
	store.instances["ckan"] = &Instance{ID: "ckan", SourceType: "ckan"}
	pub := &fakePublisher{}
	d := &Dispatcher{Store: store, Publisher: pub, MetadataTopic: "sync"}
	results := d.SyncAll(context.Background())
	if len(results) != 1 || results[0].InstanceID != "ckan" || len(pub.messages) != 1 {
		t.Fatalf("expected CKAN only, got %+v", results)
	}
}

func TestSICOMSyncUsesTheUnifiedCommand(t *testing.T) {
	store := newMemoryStore()
	store.instances["sicom"] = &Instance{ID: "sicom", Name: "TCE-MG", URL: "https://dadosabertos.tce.mg.gov.br/", SourceType: "sicom"}
	pub := &fakePublisher{}
	d := &Dispatcher{Store: store, Publisher: pub, MetadataTopic: "sync", NewID: func() string { return "sync-id" }, SICOMEnabled: true}
	if _, err := d.SyncInstance(context.Background(), "sicom"); err != nil {
		t.Fatal(err)
	}
	var command map[string]string
	if err := json.Unmarshal(pub.messages[0].payload, &command); err != nil {
		t.Fatal(err)
	}
	if command["source_type"] != "sicom" {
		t.Fatalf("command=%v", command)
	}
}

type fakePublisher struct {
	messages []published
	result   Routing
	err      error
}

func (f *fakePublisher) Publish(_ context.Context, topic string, payload []byte, key string) (Routing, error) {
	f.messages = append(f.messages, published{topic: topic, key: key, payload: payload})
	return f.result, f.err
}

func TestRetryJobRequeuesExistingJobAfterPublishingRetryMessage(t *testing.T) {
	store := newMemoryStore()
	store.jobs["failed"] = &Job{ID: "failed", ResourceID: "resource", DatasetName: "dataset", InstanceID: "instance", CKANURL: "https://example.test", Status: JobFailed}
	store.latest["resource"] = &LatestResource{ResourceID: "resource", LatestJobID: "failed", Status: string(JobFailed)}
	store.hints["resource"] = ";"
	pub := &fakePublisher{result: Routing{BrokerType: "iggy", Stream: "ckan-ingestor", Topic: "jobs-retry", Partition: 2}}
	dispatcher := &Dispatcher{Store: store, Publisher: pub, RetryTopic: "jobs-retry"}

	job, err := dispatcher.RetryJob(context.Background(), "failed")
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != "failed" || job.Status != JobFailed {
		t.Fatalf("job = %#v", job)
	}
	if len(store.jobs) != 1 || store.latest["resource"].LatestJobID != "failed" || store.latest["resource"].Status != string(JobFailed) {
		t.Fatalf("retry changed the persisted job or resource projection: jobs=%#v latest=%#v", store.jobs, store.latest)
	}
	if len(pub.messages) != 1 || pub.messages[0].topic != "jobs-retry" || pub.messages[0].key != "resource" {
		t.Fatalf("published = %#v", pub.messages)
	}
	var payload map[string]any
	if err := json.Unmarshal(pub.messages[0].payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["csv_delimiter"] != ";" || payload["job_id"] != "failed" {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestRetryKeepsOriginalSourceVersion(t *testing.T) {
	store := newMemoryStore()
	var pending JobResultMessage
	if err := json.Unmarshal([]byte(`{"job_id":"original","status":"PENDING","resource_id":"resource","dataset_name":"dataset","instance_id":"instance","source_version":"2026-09-01T00:00:00Z"}`), &pending); err != nil {
		t.Fatal(err)
	}
	if err := fixedProcessor(store).ApplyJobResult(context.Background(), pending); err != nil {
		t.Fatal(err)
	}
	pub := &fakePublisher{}
	dispatcher := &Dispatcher{Store: store, Publisher: pub, RetryTopic: "jobs-retry"}
	if _, err := dispatcher.RetryJob(context.Background(), "original"); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(pub.messages[0].payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["source_version"] != "2026-09-01T00:00:00Z" {
		t.Fatalf("retry source version = %#v", payload["source_version"])
	}
	if payload["dataset_name"] != "dataset" || payload["instance_id"] != "instance" {
		t.Fatalf("retry lost child-job discovery metadata: %#v", payload)
	}
}

func TestRetryJobRequiresTheParentForChildJobs(t *testing.T) {
	store := newMemoryStore()
	store.jobs["child"] = &Job{ID: "child", ParentJobID: ptr("parent"), ResourceID: "child-resource", DatasetName: "dataset", InstanceID: "instance", Status: JobFailed}
	pub := &fakePublisher{}
	dispatcher := &Dispatcher{Store: store, Publisher: pub, RetryTopic: "jobs-retry", NewID: func() string { return "retry" }}
	if _, err := dispatcher.RetryJob(context.Background(), "child"); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("child retry error = %v", err)
	}
	if len(pub.messages) != 0 {
		t.Fatal("child retry was published independently")
	}
}

func TestRetryJobRequeuesNonFailedJob(t *testing.T) {
	store := newMemoryStore()
	store.jobs["completed"] = &Job{ID: "completed", ResourceID: "resource", DatasetName: "dataset", InstanceID: "instance", CKANURL: "https://example.test", Status: JobCompleted}
	dispatcher := &Dispatcher{Store: store, Publisher: &fakePublisher{}, RetryTopic: "jobs-retry"}
	job, err := dispatcher.RetryJob(context.Background(), "completed")
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != "completed" || job.Status != JobCompleted {
		t.Fatalf("job = %#v", job)
	}
}

func TestSyncInstanceCreatesRecordAndPublishes(t *testing.T) {
	store := newMemoryStore()
	store.instances["instance"] = &Instance{ID: "instance", Name: "Example", URL: "https://example.test"}
	pub := &fakePublisher{}
	dispatcher := &Dispatcher{Store: store, Publisher: pub, MetadataTopic: "ckan_metadata_sync", NewID: func() string { return "sync" }}
	sync, err := dispatcher.SyncInstance(context.Background(), "instance")
	if err != nil {
		t.Fatal(err)
	}
	if sync.ID != "sync" || sync.Status != "pending" {
		t.Fatalf("sync = %#v", sync)
	}
	if len(pub.messages) != 1 || pub.messages[0].topic != "ckan_metadata_sync" || pub.messages[0].key != "sync" {
		t.Fatalf("published = %#v", pub.messages)
	}
}

func TestSyncPublishFailureMarksRecordFailed(t *testing.T) {
	store := newMemoryStore()
	store.instances["instance"] = &Instance{ID: "instance", Name: "Example", URL: "https://example.test"}
	dispatcher := &Dispatcher{Store: store, Publisher: &fakePublisher{err: errors.New("iggy down")}, MetadataTopic: "metadata", NewID: func() string { return "sync" }}
	if _, err := dispatcher.SyncInstance(context.Background(), "instance"); err == nil {
		t.Fatal("expected error")
	}
	if store.syncs["sync"].Status != "failure" || store.syncs["sync"].EndTime == nil {
		t.Fatalf("sync = %#v", store.syncs["sync"])
	}
}

func TestSyncAllReturnsOneResultPerInstance(t *testing.T) {
	store := newMemoryStore()
	store.instances["one"] = &Instance{ID: "one", Name: "One", URL: "https://one.test"}
	store.instances["two"] = &Instance{ID: "two", Name: "Two", URL: "https://two.test"}
	counter := 0
	dispatcher := &Dispatcher{Store: store, Publisher: &fakePublisher{}, MetadataTopic: "metadata", NewID: func() string { counter++; return string(rune('a' + counter)) }}
	results := dispatcher.SyncAll(context.Background())
	if len(results) != 2 {
		t.Fatalf("results = %#v", results)
	}
}

func TestSyncAllIncludesBothSourceTypes(t *testing.T) {
	store := newMemoryStore()
	var sicom Instance
	if err := json.Unmarshal([]byte(`{"id":"sicom","name":"SICOM","url":"https://dadosabertos.tce.mg.gov.br/","source_type":"sicom"}`), &sicom); err != nil {
		t.Fatal(err)
	}
	store.instances["sicom"] = &sicom
	store.instances["ckan"] = &Instance{ID: "ckan", Name: "CKAN", URL: "https://ckan.test"}
	publisher := &fakePublisher{}
	dispatcher := &Dispatcher{Store: store, Publisher: publisher, MetadataTopic: "metadata", NewID: func() string { return "sync" }, SICOMEnabled: true}
	if _, err := dispatcher.SyncInstance(context.Background(), "sicom"); err != nil {
		t.Fatal(err)
	}
	results := dispatcher.SyncAll(context.Background())
	if len(results) != 2 {
		t.Fatalf("sync results=%+v", results)
	}
}

func TestRetryReturnsNotFoundAndPublishErrors(t *testing.T) {
	store := newMemoryStore()
	dispatcher := &Dispatcher{Store: store, Publisher: &fakePublisher{}}
	if _, err := dispatcher.RetryJob(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v", err)
	}
	store.jobs["failed"] = &Job{ID: "failed", ResourceID: "resource", Status: JobFailed}
	dispatcher.Publisher = &fakePublisher{err: errors.New("iggy down")}
	if _, err := dispatcher.RetryJob(context.Background(), "failed"); err == nil {
		t.Fatal("expected publish error")
	}
	if len(store.jobs) != 1 || store.jobs["failed"].Status != JobFailed {
		t.Fatalf("retry changed the job despite publish failure: %#v", store.jobs)
	}
}
