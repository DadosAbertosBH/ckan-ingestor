// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	jobmessage "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts"
	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

// OracleSourceResourceCatalogRepository persists source snapshots and reads the API job ledger.
type OracleSourceResourceCatalogRepository struct {
	DB                                        *sql.DB
	JobsTopic, ResultsTopic, SyncResultsTopic string
}

const maximumResourcesPerCompletionBatch = 200

func (r *OracleSourceResourceCatalogRepository) Source(ctx context.Context, id string) (Source, error) {
	var s Source
	err := queryRowContext(ctx, r.DB, `SELECT id,name,url,source_type FROM ckan_instance WHERE id=?`, id).Scan(&s.ID, &s.Name, &s.URL, &s.Type)
	return s, err
}

func (r *OracleSourceResourceCatalogRepository) Lock(ctx context.Context, id string) (func(), error) {
	return acquireLock(ctx, r.DB, "source:"+id)
}
func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func (r *OracleSourceResourceCatalogRepository) versionStatuses(ctx context.Context, tx *sql.Tx, resources []Resource) (map[string]string, error) {
	statuses := make(map[string]string, len(resources))
	for start := 0; start < len(resources); start += maximumResourcesPerCompletionBatch {
		end := start + maximumResourcesPerCompletionBatch
		if end > len(resources) {
			end = len(resources)
		}
		var conditions []string
		var args []any
		identities := make(map[string]string)
		for _, v := range resources[start:end] {
			if v.URL == "" || v.Version == "" {
				continue
			}
			job := app.JobForResource(v)
			conditions = append(conditions, "(instance_id=? AND resource_id=? AND source_version=?)")
			args = append(args, v.SourceID, job.ResourceID, v.Version)
			identities[v.SourceID+"\x00"+job.ResourceID+"\x00"+v.Version] = app.ResourceID(v.ID, v.Version)
		}
		if len(conditions) == 0 {
			continue
		}
		rows, err := queryContext(ctx, tx, "SELECT instance_id,resource_id,source_version FROM ckan_data_job WHERE status='completed' AND ("+strings.Join(conditions, " OR ")+")", args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var source, resource, version string
			if err := rows.Scan(&source, &resource, &version); err != nil {
				rows.Close()
				return nil, err
			}
			statuses[identities[source+"\x00"+resource+"\x00"+version]] = "completed"
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return statuses, nil
}
func (r *OracleSourceResourceCatalogRepository) Complete(ctx context.Context, syncID string, s Source) (counts Counts, messages []Outgoing, err error) {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return counts, messages, err
	}
	defer tx.Rollback()
	err = queryRowContext(ctx, tx, `SELECT COALESCE(SUM(kind='collection' AND parent_resource_id IS NULL),0),COALESCE(SUM(kind<>'collection'),0),COALESCE(SUM(kind='collection' AND parent_resource_id IS NULL AND first_seen_sync=?),0),COALESCE(SUM(kind<>'collection' AND first_seen_sync=?),0),COALESCE(SUM(kind='collection' AND parent_resource_id IS NULL AND last_changed_sync=? AND first_seen_sync<>?),0),COALESCE(SUM(kind<>'collection' AND last_changed_sync=? AND first_seen_sync<>?),0) FROM source_resource WHERE source_id=? AND last_seen_sync=? AND deleted_at IS NULL`, syncID, syncID, syncID, syncID, syncID, syncID, s.ID, syncID).Scan(&counts.DatasetCount, &counts.ResourceCount, &counts.NewDatasets, &counts.NewResources, &counts.UpdatedDatasets, &counts.UpdatedResources)
	if err != nil {
		return counts, messages, err
	}
	rows, err := queryContext(ctx, tx, `SELECT id,COALESCE(parent_resource_id,''),kind,title,description,page_url,content_url,format,source_version,dataset_name,datastore_active FROM source_resource WHERE source_id=? AND last_seen_sync=? AND deleted_at IS NULL AND kind<>'collection' ORDER BY id`, s.ID, syncID)
	if err != nil {
		return counts, messages, err
	}
	var resources []Resource
	for rows.Next() {
		v := Resource{SourceID: s.ID}
		var parent, title, description, page, url, format, version, dataset sql.NullString
		if err = rows.Scan(&v.ID, &parent, &v.Kind, &title, &description, &page, &url, &format, &version, &dataset, &v.DatastoreActive); err != nil {
			rows.Close()
			return counts, messages, err
		}
		v.ParentID, v.Title, v.Description, v.PageURL, v.URL, v.Format, v.Version, v.DatasetName = parent.String, title.String, description.String, page.String, url.String, format.String, version.String, dataset.String
		resources = append(resources, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return counts, messages, err
	}
	statuses, err := r.versionStatuses(ctx, tx, resources)
	if err != nil {
		return counts, messages, err
	}
	for _, v := range resources {
		if v.URL == "" || v.Version == "" {
			continue
		}
		job := app.JobForResource(v)
		job.CKANURL = s.URL
		// The source's original ID is preserved for DataStore access, while identity is namespaced.
		versionID := app.ResourceID(v.ID, v.Version)
		if statuses[versionID] == "completed" {
			continue
		}
		pending := jobmessage.Pending(job)
		if err = appendMessage(&messages, r.ResultsTopic, job.ResourceID, app.ResourceID(syncID, job.JobID+":pending"), pending); err != nil {
			return counts, messages, err
		}
		if err = appendMessage(&messages, r.JobsTopic, job.ResourceID, app.ResourceID(syncID, job.JobID+":job"), job); err != nil {
			return counts, messages, err
		}
		counts.Planned++
	}
	// Include this sync's tombstones so replay republishes deletion announcements.
	missing, err := queryContext(ctx, tx, `SELECT id,kind,dataset_name,title FROM source_resource WHERE source_id=? AND last_seen_sync<>? AND (deleted_at IS NULL OR last_changed_sync=?) ORDER BY id`, s.ID, syncID, syncID)
	if err != nil {
		return counts, messages, err
	}
	type absent struct{ id, kind, dataset, title string }
	var deleted []absent
	for missing.Next() {
		var a absent
		if err = missing.Scan(&a.id, &a.kind, &a.dataset, &a.title); err != nil {
			missing.Close()
			return counts, messages, err
		}
		deleted = append(deleted, a)
	}
	err = missing.Err()
	missing.Close()
	if err != nil {
		return counts, messages, err
	}
	for _, a := range deleted {
		if a.kind == "collection" {
			counts.DeletedDatasets++
			continue
		}
		counts.DeletedResources++
		j := jobmessage.WorkerJob{JobID: app.ResourceID(syncID, a.id+":deleted"), ResourceID: a.id, InstanceID: s.ID, DatasetName: a.dataset, ResourceName: a.title, CKANURL: s.URL}
		pending := jobmessage.Pending(j)
		if err = appendMessage(&messages, r.ResultsTopic, j.ResourceID, app.ResourceID(j.JobID, "pending"), pending); err != nil {
			return counts, messages, err
		}
		pending.Status = "DELETED"
		if err = appendMessage(&messages, r.ResultsTopic, j.ResourceID, app.ResourceID(j.JobID, "deleted"), pending); err != nil {
			return counts, messages, err
		}
	}
	_, err = execContext(ctx, tx, `UPDATE source_resource SET deleted_at=UTC_TIMESTAMP(),last_changed_sync=?,updated_at=UTC_TIMESTAMP() WHERE source_id=? AND last_seen_sync<>? AND deleted_at IS NULL`, syncID, s.ID, syncID)
	if err != nil {
		return counts, messages, err
	}
	result := map[string]any{"sync_id": syncID, "instance_id": s.ID, "instance_name": s.Name, "status": "success", "total_packages": counts.DatasetCount, "dataset_count": counts.DatasetCount, "resource_count": counts.ResourceCount, "new_datasets": counts.NewDatasets, "new_resources": counts.NewResources, "updated_datasets": counts.UpdatedDatasets, "updated_resources": counts.UpdatedResources, "deleted_datasets": counts.DeletedDatasets, "deleted_resources": counts.DeletedResources, "outdated_resources": counts.Planned}
	if err = appendMessage(&messages, r.SyncResultsTopic, s.ID, app.ResourceID(syncID, "success"), result); err != nil {
		return counts, messages, err
	}
	err = tx.Commit()
	return counts, messages, err
}
func appendMessage(messages *[]Outgoing, topic, key, id string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	*messages = append(*messages, Outgoing{ID: id, Topic: topic, Key: key, Payload: payload})
	return nil
}
