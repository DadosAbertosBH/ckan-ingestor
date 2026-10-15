// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package mysqlrepositories

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	jobmessage "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts"
	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

// MySQLSourceResourceCatalogRepository persists source snapshots and reads the API job ledger.
type MySQLSourceResourceCatalogRepository struct {
	DB                                        *sql.DB
	JobsTopic, ResultsTopic, SyncResultsTopic string
}

const maximumResourcesPerCompletionBatch = 200

func (r *MySQLSourceResourceCatalogRepository) Source(ctx context.Context, id string) (Source, error) {
	var s Source
	err := r.DB.QueryRowContext(ctx, `SELECT id,name,url,source_type FROM ckan_instance WHERE id=?`, id).Scan(&s.ID, &s.Name, &s.URL, &s.Type)
	return s, err
}

// Lock serializes snapshots of the same source, including redelivery across replicas.
func (r *MySQLSourceResourceCatalogRepository) Lock(ctx context.Context, id string) (func(), error) {
	conn, err := r.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	key := "source-sync:" + app.ResourceID(id, "lock")
	var acquired int
	if err = conn.QueryRowContext(ctx, `SELECT GET_LOCK(?,0)`, key).Scan(&acquired); err != nil || acquired != 1 {
		conn.Close()
		if err == nil {
			err = errors.New("source sync already active")
		}
		return nil, err
	}
	return func() { conn.ExecContext(context.Background(), `SELECT RELEASE_LOCK(?)`, key); conn.Close() }, nil
}
func (r *MySQLSourceResourceCatalogRepository) ObserveBatch(ctx context.Context, syncID string, values []Resource) error {
	if len(values) == 0 {
		return nil
	}
	args := make([]any, 0, len(values)*15)
	rows := make([]string, 0, len(values))
	for _, v := range values {
		rows = append(rows, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP(),UTC_TIMESTAMP())")
		args = append(args, v.ID, v.SourceID, nullable(v.ParentID), v.Kind, v.Title, v.Description, v.PageURL, v.URL, v.Format, v.Version, v.DatasetName, v.DatastoreActive, syncID, syncID, syncID)
	}
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Compare versions and restoration state before assigning the new fields.
	query := `INSERT INTO source_resource (id,source_id,parent_resource_id,kind,title,description,page_url,content_url,format,source_version,dataset_name,datastore_active,first_seen_sync,last_changed_sync,last_seen_sync,created_at,updated_at) VALUES ` + strings.Join(rows, ",") + ` ON DUPLICATE KEY UPDATE last_changed_sync=IF(NOT(BINARY source_version<=>BINARY VALUES(source_version)) OR deleted_at IS NOT NULL,VALUES(last_changed_sync),last_changed_sync),parent_resource_id=VALUES(parent_resource_id),kind=VALUES(kind),title=VALUES(title),description=VALUES(description),page_url=VALUES(page_url),content_url=VALUES(content_url),format=VALUES(format),source_version=VALUES(source_version),dataset_name=VALUES(dataset_name),datastore_active=VALUES(datastore_active),last_seen_sync=VALUES(last_seen_sync),deleted_at=NULL,updated_at=UTC_TIMESTAMP()`
	if _, err = tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	return tx.Commit()
}
func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func (r *MySQLSourceResourceCatalogRepository) versionStatuses(ctx context.Context, tx *sql.Tx, resources []Resource) (map[string]string, error) {
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
		rows, err := tx.QueryContext(ctx, "SELECT instance_id,resource_id,source_version FROM ckan_data_job WHERE status='completed' AND ("+strings.Join(conditions, " OR ")+")", args...)
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
func (r *MySQLSourceResourceCatalogRepository) Complete(ctx context.Context, syncID string, s Source) (counts Counts, messages []Outgoing, err error) {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return counts, messages, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(kind='collection' AND parent_resource_id IS NULL),0),COALESCE(SUM(kind<>'collection'),0),COALESCE(SUM(kind='collection' AND parent_resource_id IS NULL AND first_seen_sync=?),0),COALESCE(SUM(kind<>'collection' AND first_seen_sync=?),0),COALESCE(SUM(kind='collection' AND parent_resource_id IS NULL AND last_changed_sync=? AND first_seen_sync<>?),0),COALESCE(SUM(kind<>'collection' AND last_changed_sync=? AND first_seen_sync<>?),0) FROM source_resource WHERE source_id=? AND last_seen_sync=? AND deleted_at IS NULL`, syncID, syncID, syncID, syncID, syncID, syncID, s.ID, syncID).Scan(&counts.DatasetCount, &counts.ResourceCount, &counts.NewDatasets, &counts.NewResources, &counts.UpdatedDatasets, &counts.UpdatedResources)
	if err != nil {
		return counts, messages, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,COALESCE(parent_resource_id,''),kind,title,description,page_url,content_url,format,source_version,dataset_name,datastore_active FROM source_resource WHERE source_id=? AND last_seen_sync=? AND deleted_at IS NULL AND kind<>'collection' ORDER BY id`, s.ID, syncID)
	if err != nil {
		return counts, messages, err
	}
	var resources []Resource
	for rows.Next() {
		v := Resource{SourceID: s.ID}
		if err = rows.Scan(&v.ID, &v.ParentID, &v.Kind, &v.Title, &v.Description, &v.PageURL, &v.URL, &v.Format, &v.Version, &v.DatasetName, &v.DatastoreActive); err != nil {
			rows.Close()
			return counts, messages, err
		}
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
	missing, err := tx.QueryContext(ctx, `SELECT id,kind,dataset_name,title FROM source_resource WHERE source_id=? AND last_seen_sync<>? AND (deleted_at IS NULL OR last_changed_sync=?) ORDER BY id`, s.ID, syncID, syncID)
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
	_, err = tx.ExecContext(ctx, `UPDATE source_resource SET deleted_at=UTC_TIMESTAMP(),last_changed_sync=?,updated_at=UTC_TIMESTAMP() WHERE source_id=? AND last_seen_sync<>? AND deleted_at IS NULL`, syncID, s.ID, syncID)
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
