// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package repositories

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	jobmessage "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts"
)

var ErrNotFound = errors.New("not found")

type Source struct {
	ID   string `json:"id"`
	Name string `json:"title"`
	URL  string `json:"url"`
	Type string `json:"type"`
}
type Resource struct {
	ID                 string          `json:"id"`
	SourceID           string          `json:"source_id"`
	ParentID           string          `json:"parent_resource_id,omitempty"`
	Kind               string          `json:"kind"`
	Title              string          `json:"title"`
	Description        string          `json:"description"`
	PageURL            string          `json:"page_url"`
	URL                string          `json:"content_url"`
	Format             string          `json:"format"`
	Version            string          `json:"source_version"`
	DatasetName        string          `json:"dataset_name"`
	DatastoreActive    bool            `json:"datastore_active"`
	Metadata           json.RawMessage `json:"metadata,omitempty"`
	MetadataCollection string          `json:"metadata_collection,omitempty"`
}
type Counts struct{ NewDatasets, NewResources, UpdatedDatasets, UpdatedResources, DeletedDatasets, DeletedResources, DatasetCount, ResourceCount, Planned int64 }
type Outgoing struct {
	ID, Topic, Key string
	Payload        []byte
}
type SourceResourceCatalogRepository interface {
	Source(context.Context, string) (Source, error)
	// Lock serializes syncs for one source and returns its release function.
	Lock(context.Context, string) (func(), error)
	ObserveBatch(context.Context, string, []Resource) error
	Complete(context.Context, string, Source) (Counts, []Outgoing, error)
}

func ResourceID(source, key string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(source+":"+key)).String()
}
func JobForResource(r Resource) jobmessage.WorkerJob {
	jobID := ResourceID(r.SourceID, r.ID+":"+r.Version)
	return jobmessage.WorkerJob{JobID: jobID, ResourceID: r.ID, InstanceID: r.SourceID, PackageID: r.ParentID, DatasetName: r.DatasetName, ResourceName: r.Title, ResourceURL: r.URL, ResourceFormat: r.Format, SourceVersion: r.Version, DatastoreActive: r.DatastoreActive, ExternalResourceID: r.ID}
}
