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
type fakePublisher struct {
	messages []published
	result   Routing
	err      error
}

func (f *fakePublisher) Publish(_ context.Context, topic string, payload []byte, key string) (Routing, error) {
	f.messages = append(f.messages, published{topic: topic, key: key, payload: payload})
	return f.result, f.err
}

func TestRetryJobCreatesAttemptAndPublishesRetryMessage(t *testing.T) {
	store := newMemoryStore()
	store.jobs["failed"] = &Job{ID: "failed", ResourceID: "resource", DatasetName: "dataset", InstanceID: "instance", CKANURL: "https://example.test", Status: JobFailed}
	store.hints["resource"] = ";"
	pub := &fakePublisher{result: Routing{BrokerType: "iggy", Stream: "ckan-ingestor", Topic: "jobs-retry", Partition: 2}}
	dispatcher := &Dispatcher{Store: store, Publisher: pub, RetryTopic: "jobs-retry", NewID: func() string { return "retry" }}

	job, err := dispatcher.RetryJob(context.Background(), "failed")
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != "retry" || job.Status != JobPending || *job.MessagePartition != 2 {
		t.Fatalf("job = %#v", job)
	}
	if len(pub.messages) != 1 || pub.messages[0].topic != "jobs-retry" || pub.messages[0].key != "resource" {
		t.Fatalf("published = %#v", pub.messages)
	}
	var payload map[string]any
	if err := json.Unmarshal(pub.messages[0].payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["csv_delimiter"] != ";" || payload["job_id"] != "retry" {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestRetryRejectsNonFailedJob(t *testing.T) {
	store := newMemoryStore()
	store.jobs["pending"] = &Job{ID: "pending", Status: JobPending}
	dispatcher := &Dispatcher{Store: store, Publisher: &fakePublisher{}}
	if _, err := dispatcher.RetryJob(context.Background(), "pending"); !errors.Is(err, ErrConflict) {
		t.Fatalf("error = %v", err)
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
}
