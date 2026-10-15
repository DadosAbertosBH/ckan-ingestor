// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package resourcecatalog

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	jobmessage "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts"
	repositories "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

type Source = repositories.Source
type Resource = repositories.Resource
type Counts = repositories.Counts
type Outgoing = repositories.Outgoing

type Adapter interface {
	Discover(context.Context, Source, func(Resource) error) error
}
type Publisher interface {
	Send(context.Context, string, string, []byte) (int, error)
}
type Worker struct {
	Catalog          SourceResourceCatalogRepository
	Adapters         map[string]Adapter
	Publisher        Publisher
	SyncResultsTopic string
}

const maximumResourcesPerObservationBatch = 200

// ResourceID builds a deterministic identifier for synthetic resources and messages.
func ResourceID(source, key string) string {
	return repositories.ResourceID(source, key)
}
func Job(r Resource) jobmessage.WorkerJob {
	return repositories.JobForResource(r)
}
func (w *Worker) Sync(ctx context.Context, syncID, sourceID, sourceType string) error {
	slog.InfoContext(ctx, "source sync started", "sync_id", syncID, "source_id", sourceID, "source_type", sourceType)
	source, err := w.Catalog.Source(ctx, sourceID)
	if err != nil {
		return err
	}
	unlock, err := w.Catalog.Lock(ctx, source.ID)
	if err != nil {
		return err
	}
	defer unlock()
	if source.Type != sourceType {
		return fmt.Errorf("source type mismatch: configured %s, requested %s", source.Type, sourceType)
	}
	adapter, ok := w.Adapters[source.Type]
	if !ok {
		return fmt.Errorf("unsupported source type %s", source.Type)
	}
	batch := make([]Resource, 0, maximumResourcesPerObservationBatch)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := w.Catalog.ObserveBatch(ctx, syncID, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	err = adapter.Discover(ctx, source, func(r Resource) error {
		r.SourceID = source.ID
		if r.ID == "" {
			return fmt.Errorf("resource identity is required")
		}
		if len(batch) == maximumResourcesPerObservationBatch {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, r)
		return nil
	})
	if flushErr := flush(); flushErr != nil {
		return flushErr
	}
	if err != nil {
		if w.SyncResultsTopic != "" {
			payload, encodeErr := json.Marshal(map[string]any{"sync_id": syncID, "instance_id": source.ID, "instance_name": source.Name, "status": "failure", "error_message": err.Error()})
			if encodeErr != nil {
				return encodeErr
			}
			_, publishErr := w.Publisher.Send(ctx, w.SyncResultsTopic, source.ID, payload)
			return publishErr
		}
		return err
	}
	_, messages, err := w.Catalog.Complete(ctx, syncID, source)
	if err != nil {
		return err
	}
	for _, m := range messages {
		if _, err := w.Publisher.Send(ctx, m.Topic, m.Key, m.Payload); err != nil {
			return err
		}
	}
	return nil
}
