// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package resourcecatalog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

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

// SnapshotSink persists an immutable, complete JSONL discovery for lake ingestion.
type SnapshotSink interface {
	Publish(context.Context, string, Source, time.Time, io.ReadSeeker) error
}
type Worker struct {
	Catalog          SourceResourceCatalogRepository
	Adapters         map[string]Adapter
	Publisher        Publisher
	SyncResultsTopic string
	Snapshots        SnapshotSink
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
	collectedAt := time.Now().UTC()
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
	// Spool discovery to disk; validate the complete tree before updating current rows.
	spool, err := os.CreateTemp("", "source-catalog-*.jsonl")
	if err != nil {
		return err
	}
	defer os.Remove(spool.Name())
	defer spool.Close()
	encoder := json.NewEncoder(spool)
	parents := make(map[string]string)
	err = adapter.Discover(ctx, source, func(r Resource) error {
		if r.SourceID != "" && r.SourceID != source.ID {
			return fmt.Errorf("resource %s belongs to another source", r.ID)
		}
		r.SourceID = source.ID
		if r.ID == "" {
			return fmt.Errorf("resource identity is required")
		}
		if _, exists := parents[r.ID]; exists {
			return fmt.Errorf("duplicate resource %s", r.ID)
		}
		parents[r.ID] = r.ParentID
		return encoder.Encode(r)
	})
	if err == nil {
		err = validateTree(parents)
	}
	if err != nil {
		if w.SyncResultsTopic != "" {
			payload, encodeErr := json.Marshal(map[string]any{"sync_id": syncID, "instance_id": source.ID, "instance_name": source.Name, "status": "failure", "error_message": err.Error()})
			if encodeErr != nil {
				return encodeErr
			}
			_, publishErr := w.Publisher.Send(ctx, w.SyncResultsTopic, source.ID, payload)
			if publishErr != nil {
				return publishErr
			}
			return nil
		}
		return err
	}
	if _, err = spool.Seek(0, io.SeekStart); err != nil {
		return err
	}
	decoder := json.NewDecoder(spool)
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
	for {
		var r Resource
		if err = decoder.Decode(&r); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if len(batch) == maximumResourcesPerObservationBatch {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, r)
	}
	if flushErr := flush(); flushErr != nil {
		return flushErr
	}
	_, messages, err := w.Catalog.Complete(ctx, syncID, source)
	if err != nil {
		return err
	}
	if w.Snapshots != nil {
		if _, err = spool.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if err = w.Snapshots.Publish(ctx, syncID, source, collectedAt, spool); err != nil {
			return err
		}
	}
	for _, m := range messages {
		if _, err := w.Publisher.Send(ctx, m.Topic, m.Key, m.Payload); err != nil {
			return err
		}
	}
	return nil
}

func validateTree(parents map[string]string) error {
	visited := make(map[string]bool, len(parents))
	for id := range parents {
		path := make(map[string]bool)
		for node := id; node != "" && !visited[node]; node = parents[node] {
			if _, exists := parents[node]; !exists {
				return fmt.Errorf("resource %s has missing parent %s", id, node)
			}
			if path[node] {
				return fmt.Errorf("resource cycle at %s", node)
			}
			path[node] = true
		}
		for node := range path {
			visited[node] = true
		}
	}
	return nil
}
