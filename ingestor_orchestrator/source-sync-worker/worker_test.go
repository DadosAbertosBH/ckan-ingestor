// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sourcesyncworker

import (
	"context"
	catalog "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog"
	"testing"
)

type fakeRepository struct {
	source catalog.Source
	calls  int
}

func (r *fakeRepository) Source(context.Context, string) (catalog.Source, error) {
	return r.source, nil
}
func (r *fakeRepository) Lock(context.Context, string) (func(), error)                   { return func() {}, nil }
func (r *fakeRepository) ObserveBatch(context.Context, string, []catalog.Resource) error { return nil }
func (r *fakeRepository) Complete(context.Context, string, catalog.Source) (catalog.Counts, []catalog.Outgoing, error) {
	r.calls++
	return catalog.Counts{}, nil, nil
}

type fakeAdapter struct{}

func (fakeAdapter) Discover(context.Context, catalog.Source, func(catalog.Resource) error) error {
	return nil
}

type fakePublisher struct{}

func (fakePublisher) Send(context.Context, string, string, []byte) (int, error) { return 0, nil }
func TestRunnerRoutesLegacyCommandToCKAN(t *testing.T) {
	repo := &fakeRepository{source: catalog.Source{ID: "source", Type: "ckan"}}
	runner := Runner{Worker: &catalog.Worker{Catalog: repo, Publisher: fakePublisher{}, Adapters: map[string]catalog.Adapter{"ckan": fakeAdapter{}}}}
	if err := runner.Handle(context.Background(), []byte(`{"sync_id":"sync","instance_id":"source","instance_url":"https://example.test"}`)); err != nil {
		t.Fatal(err)
	}
	if repo.calls != 1 {
		t.Fatal("CKAN command was not completed")
	}
}
