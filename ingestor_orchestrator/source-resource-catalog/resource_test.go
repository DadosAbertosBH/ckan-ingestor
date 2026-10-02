// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package resourcecatalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

type testRepo struct {
	source       Source
	observed     []Resource
	observeCalls int
	complete     int
	outgoing     []Outgoing
	ack          []string
}

func (r *testRepo) Source(context.Context, string) (Source, error) { return r.source, nil }
func (r *testRepo) Lock(context.Context, string) (func(), error)   { return func() {}, nil }
func (r *testRepo) ObserveBatch(_ context.Context, _ string, values []Resource) error {
	r.observeCalls++
	r.observed = append(r.observed, values...)
	return nil
}
func (r *testRepo) Complete(context.Context, string, Source) (Counts, []Outgoing, error) {
	r.complete++
	return Counts{}, r.outgoing, nil
}
func (r *testRepo) Pending(context.Context) ([]Outgoing, error) { return r.outgoing, nil }
func (r *testRepo) Acknowledge(_ context.Context, id string) error {
	r.ack = append(r.ack, id)
	for i, m := range r.outgoing {
		if m.ID == id {
			r.outgoing = append(r.outgoing[:i], r.outgoing[i+1:]...)
			break
		}
	}
	return nil
}

type testAdapter struct{ fail bool }

func (a testAdapter) Discover(ctx context.Context, s Source, emit func(Resource) error) error {
	if err := emit(Resource{ID: "package", Kind: "collection"}); err != nil {
		return err
	}
	if a.fail {
		return errors.New("page failed")
	}
	return emit(Resource{ID: "file", ParentID: "package", Kind: "file"})
}

type resourceListAdapter []Resource

func (a resourceListAdapter) Discover(_ context.Context, _ Source, emit func(Resource) error) error {
	for _, resource := range a {
		if err := emit(resource); err != nil {
			return err
		}
	}
	return nil
}

type testPublisher struct {
	fail bool
	sent []string
}

func (p *testPublisher) Send(_ context.Context, _ string, key string, _ []byte) (int, error) {
	p.sent = append(p.sent, key)
	if p.fail {
		return 0, errors.New("broker down")
	}
	return 0, nil
}

func TestIdentitiesAreScopedToTheSourceAndContentVersion(t *testing.T) {
	if ResourceID("a", "same") == ResourceID("b", "same") {
		t.Fatal("resource identity collides across sources")
	}
	r := Resource{ID: ResourceID("a", "same"), SourceID: "a", Version: "v1"}
	first := Job(r)
	r.Version = "v2"
	second := Job(r)
	if first.JobID == second.JobID || first.ResourceID != second.ResourceID {
		t.Fatal("content versions need separate jobs and stable resource identity")
	}
}
func TestSyncPreservesAdapterResourceID(t *testing.T) {
	repo := &testRepo{source: Source{ID: "source", Type: "ckan"}}
	w := Worker{Catalog: repo, Adapters: map[string]Adapter{"ckan": resourceListAdapter{{ID: "original-ckan-id"}}}, Publisher: &testPublisher{}}
	if err := w.Sync(context.Background(), "sync", "source", "ckan"); err != nil {
		t.Fatal(err)
	}
	if repo.observed[0].ID != "original-ckan-id" {
		t.Fatalf("resource ID = %q", repo.observed[0].ID)
	}
}
func TestJobsUseResourceIDAndScopeVersionsBySource(t *testing.T) {
	a := Job(Resource{ID: "original", SourceID: "a", Version: "v1", DatastoreActive: true})
	b := Job(Resource{ID: "original", SourceID: "b", Version: "v1"})
	if a.ResourceID != "original" || a.ExternalResourceID != "original" || a.JobID == b.JobID {
		t.Fatalf("jobs=%+v %+v", a, b)
	}
}
func TestCatalogIdentityIsUsedForDataStoreAccess(t *testing.T) {
	r := Resource{ID: "external", SourceID: "source", DatastoreActive: true}
	data, _ := json.Marshal(Job(r))
	var payload map[string]any
	json.Unmarshal(data, &payload)
	if payload["resource_id"] != "external" || payload["job_id"] != Job(r).JobID || payload["external_resource_id"] != "external" {
		t.Fatalf("payload=%s", data)
	}
}
func TestSyncRoutesAdaptersAndCompletesOnlyFullDiscovery(t *testing.T) {
	for _, fail := range []bool{false, true} {
		repo := &testRepo{source: Source{ID: "s", Type: "sicom"}}
		w := Worker{Catalog: repo, Adapters: map[string]Adapter{"sicom": testAdapter{fail}}, Publisher: &testPublisher{}}
		err := w.Sync(context.Background(), "sync", "s", "sicom")
		if (err != nil) != fail {
			t.Fatalf("fail=%v err=%v", fail, err)
		}
		expected := 1
		if fail {
			expected = 0
		}
		if repo.complete != expected {
			t.Fatal("partial discovery finalized snapshot")
		}
		if len(repo.observed) == 0 || repo.observed[0].SourceID != "s" {
			t.Fatal("observations lost source")
		}
	}
}

func TestSyncPersistsDiscoveredResourcesInOneBatch(t *testing.T) {
	repo := &testRepo{source: Source{ID: "s", Type: "sicom"}}
	adapter := testAdapter{}
	w := Worker{Catalog: repo, Adapters: map[string]Adapter{"sicom": adapter}, Publisher: &testPublisher{}}
	if err := w.Sync(context.Background(), "sync", "s", "sicom"); err != nil {
		t.Fatal(err)
	}
	if repo.observeCalls != 1 {
		t.Fatalf("persistence calls=%d, want one batch", repo.observeCalls)
	}
}

func TestSyncFlushesAtTwoHundredResources(t *testing.T) {
	repo := &testRepo{source: Source{ID: "s", Type: "sicom"}}
	adapter := make(resourceListAdapter, 201)
	for i := range adapter {
		adapter[i] = Resource{ID: ResourceID("s", fmt.Sprint(i))}
	}
	w := Worker{Catalog: repo, Adapters: map[string]Adapter{"sicom": adapter}, Publisher: &testPublisher{}}
	if err := w.Sync(context.Background(), "sync", "s", "sicom"); err != nil {
		t.Fatal(err)
	}
	if repo.observeCalls != 2 {
		t.Fatalf("persistence calls=%d, want two bounded batches", repo.observeCalls)
	}
}
func TestSyncPublishesWithoutPersistingDeliveryAcknowledgements(t *testing.T) {
	repo := &testRepo{source: Source{ID: "s", Type: "ckan"}, outgoing: []Outgoing{{ID: "job", Topic: "jobs", Key: "resource"}}}
	pub := &testPublisher{}
	w := Worker{Catalog: repo, Publisher: pub, Adapters: map[string]Adapter{"ckan": testAdapter{}}}
	if err := w.Sync(context.Background(), "sync", "s", "ckan"); err != nil {
		t.Fatal(err)
	}
	if len(repo.ack) != 0 {
		t.Fatalf("delivery caused database writes: %v", repo.ack)
	}
}

type partialPublisher struct {
	attempts []Outgoing
	failAt   int
}

func (p *partialPublisher) Send(_ context.Context, topic, key string, payload []byte) (int, error) {
	p.attempts = append(p.attempts, Outgoing{Topic: topic, Key: key, Payload: payload})
	if len(p.attempts) == p.failAt {
		return 0, errors.New("publication failed")
	}
	return 0, nil
}
func TestPublicationFailureReplaysStableJobsAndCompletesLast(t *testing.T) {
	repo := &testRepo{source: Source{ID: "s", Type: "ckan"}, outgoing: []Outgoing{
		{ID: "pending", Topic: "results", Key: "r", Payload: []byte(`{"job_id":"stable"}`)},
		{ID: "job", Topic: "jobs", Key: "r", Payload: []byte(`{"job_id":"stable"}`)},
		{ID: "complete", Topic: "sync-results", Key: "s", Payload: []byte(`{"status":"success"}`)},
	}}
	pub := &partialPublisher{failAt: 2}
	w := Worker{Catalog: repo, Publisher: pub, Adapters: map[string]Adapter{"ckan": testAdapter{}}}
	if err := w.Sync(context.Background(), "sync", "s", "ckan"); err == nil {
		t.Fatal("failed publication accepted")
	}
	if len(pub.attempts) != 2 {
		t.Fatal("completion published before all jobs")
	}
	first := string(pub.attempts[1].Payload)
	// Losing the completion acknowledgement must also keep the command retryable.
	pub.attempts = nil
	pub.failAt = 3
	if err := w.Sync(context.Background(), "sync", "s", "ckan"); err == nil {
		t.Fatal("failed completion publication accepted")
	}
	pub.attempts = nil
	pub.failAt = 0
	if err := w.Sync(context.Background(), "sync", "s", "ckan"); err != nil {
		t.Fatal(err)
	}
	if len(pub.attempts) != 3 || pub.attempts[2].Topic != "sync-results" || string(pub.attempts[1].Payload) != first {
		t.Fatalf("replay=%v", pub.attempts)
	}
	if len(repo.ack) != 0 {
		t.Fatal("delivery state written")
	}
}
