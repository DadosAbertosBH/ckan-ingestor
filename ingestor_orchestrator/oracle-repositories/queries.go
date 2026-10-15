// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package oraclerepositories

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

const instanceColumns = `id,name,url,CASE WHEN source_type='sicom' THEN (SELECT COUNT(DISTINCT dataset_name) FROM latest_resource_job WHERE instance_id=ckan_instance.id) ELSE dataset_count END,CASE WHEN source_type='sicom' THEN (SELECT COUNT(*) FROM latest_resource_job WHERE instance_id=ckan_instance.id) ELSE resource_count END,last_metadata_synced,created_at,updated_at,source_type`

func (s *Store) ListInstances(ctx context.Context) ([]app.Instance, error) {
	rows, err := queryContext(ctx, s.DB, `SELECT `+instanceColumns+` FROM ckan_instance ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []app.Instance{}
	for rows.Next() {
		var v app.Instance
		var synced sql.NullTime
		if err := rows.Scan(&v.ID, &v.Name, &v.URL, &v.DatasetCount, &v.ResourceCount, &synced, &v.CreatedAt, &v.UpdatedAt, &v.SourceType); err != nil {
			return nil, err
		}
		v.LastMetadataSynced = nullTime(synced)
		values = append(values, v)
	}
	return values, rows.Err()
}

func (s *Store) GetInstance(ctx context.Context, id string) (*app.Instance, error) {
	var v app.Instance
	var synced sql.NullTime
	err := queryRowContext(ctx, s.DB, `SELECT `+instanceColumns+` FROM ckan_instance WHERE id=?`, id).Scan(&v.ID, &v.Name, &v.URL, &v.DatasetCount, &v.ResourceCount, &synced, &v.CreatedAt, &v.UpdatedAt, &v.SourceType)
	v.LastMetadataSynced = nullTime(synced)
	return &v, translateNotFound(err)
}
func (s *Store) CreateInstance(ctx context.Context, v *app.Instance) error {
	if v.SourceType == "" {
		v.SourceType = "ckan"
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = execContext(ctx, tx, `INSERT INTO ckan_instance (id,name,url,dataset_count,resource_count,created_at,updated_at,source_type) VALUES (?,?,?,0,0,?,?,?)`, v.ID, v.Name, v.URL, v.CreatedAt, v.UpdatedAt, v.SourceType); err != nil {
		return err
	}
	if _, err = execContext(ctx, tx, "INSERT INTO repository_lock(lock_key) VALUES (?)", "source:"+v.ID); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) DeleteInstance(ctx context.Context, id string) error {
	result, err := execContext(ctx, s.DB, `DELETE FROM ckan_instance WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n == 0 {
		return app.ErrNotFound
	}
	return err
}

const jobColumns = `j.id,j.resource_id,j.resource_name,j.resource_url,j.resource_format,j.dataset_name,j.status,j.idempotency_key,j.instance_id,i.name,i.url,j.ckan_url,j.datastore_active,j.created_at,j.updated_at,j.started_at,j.completed_at,j.broker_type,j.message_stream,j.message_topic,j.message_partition,j.message_offset,j.parent_job_id,j.parent_resource_id,j.child_index,j.total_child_count,j.source_version`

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (app.JobView, error) {
	var v app.JobView
	var name, url, format, instanceID, instanceName, instanceURL, broker, stream, topic, parentJob, parentResource, sourceVersion sql.NullString
	var started, completed sql.NullTime
	var partition, offset, childIndex, totalChildren sql.NullInt64
	err := row.Scan(&v.ID, &v.ResourceID, &name, &url, &format, &v.DatasetName, &v.Status, &v.IdempotencyKey, &instanceID, &instanceName, &instanceURL, &v.CKANURL, &v.DatastoreActive, &v.CreatedAt, &v.UpdatedAt, &started, &completed, &broker, &stream, &topic, &partition, &offset, &parentJob, &parentResource, &childIndex, &totalChildren, &sourceVersion)
	if err != nil {
		return v, translateNotFound(err)
	}
	v.ResourceName, v.ResourceURL, v.ResourceFormat = nullString(name), nullString(url), nullString(format)
	if instanceID.Valid {
		v.InstanceID = instanceID.String
	}
	v.InstanceName = nullString(instanceName)
	if instanceURL.Valid {
		v.InstanceURL = instanceURL.String
	}
	v.StartedAt, v.CompletedAt = nullTime(started), nullTime(completed)
	v.BrokerType, v.MessageStream, v.MessageTopic = nullString(broker), nullString(stream), nullString(topic)
	if partition.Valid {
		p := int(partition.Int64)
		v.MessagePartition = &p
	}
	v.MessageOffset = nullInt64(offset)
	v.ParentJobID, v.ParentResourceID = nullString(parentJob), nullString(parentResource)
	v.SourceVersion = nullString(sourceVersion)
	if childIndex.Valid {
		index := int(childIndex.Int64)
		v.ChildIndex = &index
	}
	if totalChildren.Valid {
		count := int(totalChildren.Int64)
		v.TotalChildCount = &count
	}
	ckanResourceID := v.ResourceID
	if v.ParentResourceID != nil {
		ckanResourceID = *v.ParentResourceID
	}
	v.CKANResourceURL = app.SourceResourceURL(v.InstanceURL, v.DatasetName, ckanResourceID)
	v.Labels = []string{}
	return v, nil
}
func resourceURL(base, dataset, resource string) string {
	return app.SourceResourceURL(base, dataset, resource)
}

func (s *Store) ListJobs(ctx context.Context, q app.JobQuery) ([]app.JobView, error) {
	where, args := []string{"j.parent_job_id IS NULL"}, []any{}
	add := func(column, value string) {
		if value != "" {
			where = append(where, column+"=?")
			args = append(args, value)
		}
	}
	add("j.status", q.Status)
	add("j.resource_id", q.ResourceID)
	add("j.instance_id", q.InstanceID)
	if q.Tags != "" {
		tags := splitTags(q.Tags)
		if len(tags) > 0 {
			marks := make([]string, len(tags))
			for i, tag := range tags {
				marks[i] = "?"
				args = append(args, tag)
			}
			where = append(where, "EXISTS (SELECT 1 FROM resource_metadata_label f WHERE f.resource_id=j.resource_id AND f.label IN ("+strings.Join(marks, ",")+"))")
		}
	}
	order := "j.created_at DESC"
	if q.OrderBy == "created_at" && q.OrderDir == "asc" {
		order = "j.created_at ASC"
	}
	if q.OrderBy == "duration" {
		direction := "DESC"
		if q.OrderDir == "asc" {
			direction = "ASC"
		}
		order = "CASE WHEN j.started_at IS NULL THEN 1 ELSE 0 END, TIMESTAMPDIFF(MICROSECOND,j.started_at,COALESCE(j.completed_at,UTC_TIMESTAMP())) " + direction
	}
	args = append(args, q.Limit, q.Offset)
	rows, err := queryContext(ctx, s.DB, `SELECT `+jobColumns+` FROM ckan_data_job j LEFT JOIN ckan_instance i ON i.id=j.instance_id WHERE `+strings.Join(where, " AND ")+` ORDER BY `+order+` LIMIT ? OFFSET ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []app.JobView{}
	for rows.Next() {
		v, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		v.Labels, err = s.labels(ctx, v.ResourceID)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, rows.Err()
}
func splitTags(raw string) []string {
	var out []string
	for _, v := range strings.Split(raw, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
func (s *Store) labels(ctx context.Context, id string) ([]string, error) {
	rows, err := queryContext(ctx, s.DB, `SELECT label FROM resource_metadata_label WHERE resource_id=? ORDER BY label`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) GetJob(ctx context.Context, id string) (*app.JobView, error) {
	v, err := scanJob(queryRowContext(ctx, s.DB, `SELECT `+jobColumns+` FROM ckan_data_job j LEFT JOIN ckan_instance i ON i.id=j.instance_id WHERE j.id=?`, id))
	if err != nil {
		return nil, err
	}
	v.Labels, err = s.labels(ctx, v.ResourceID)
	if err != nil {
		return nil, err
	}
	results, resultErr := s.results(ctx, v.ID)
	v.Results, err = &results, resultErr
	if err != nil {
		return &v, err
	}
	v.Children, err = s.childJobs(ctx, v.ID)
	return &v, err
}

func (s *Store) childJobs(ctx context.Context, parentID string) ([]app.JobView, error) {
	rows, err := queryContext(ctx, s.DB, `SELECT `+jobColumns+` FROM ckan_data_job j LEFT JOIN ckan_instance i ON i.id=j.instance_id WHERE j.parent_job_id=? ORDER BY j.child_index`, parentID)
	if err != nil {
		return nil, err
	}
	children := []app.JobView{}
	for rows.Next() {
		child, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		children = append(children, child)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for index := range children {
		results, err := s.results(ctx, children[index].ID)
		if err != nil {
			return nil, err
		}
		children[index].Results = &results
	}
	return children, nil
}
func (s *Store) results(ctx context.Context, jobID string) ([]app.JobResult, error) {
	rows, err := queryContext(ctx, s.DB, `SELECT id,job_id,success,status,error_message,error_trace,dataset_preview,rows_processed,expected_rows,resource_size,encoding,created_at FROM ckan_data_job_result WHERE job_id=? ORDER BY created_at`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []app.JobResult{}
	for rows.Next() {
		var v app.JobResult
		var message, trace, preview, encoding sql.NullString
		var rowsDone, expected, size sql.NullInt64
		if err := rows.Scan(&v.ID, &v.JobID, &v.Success, &v.Status, &message, &trace, &preview, &rowsDone, &expected, &size, &encoding, &v.CreatedAt); err != nil {
			return nil, err
		}
		v.ErrorMessage, v.ErrorTrace, v.Encoding = nullString(message), nullString(trace), nullString(encoding)
		v.RowsProcessed, v.ExpectedRows, v.ResourceSize = nullInt64(rowsDone), nullInt64(expected), nullInt64(size)
		if preview.Valid {
			v.DatasetPreview = json.RawMessage(preview.String)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

const resourceColumns = `l.resource_id,l.latest_job_id,l.resource_name,l.resource_url,l.resource_format,l.dataset_name,l.status,l.instance_id,i.url,l.created_at,l.updated_at,(SELECT COUNT(*) FROM ckan_data_job c WHERE c.resource_id=l.resource_id)`

func scanResource(row scanner) (app.ResourceView, error) {
	var v app.ResourceView
	var name, url, format, instanceURL sql.NullString
	err := row.Scan(&v.ResourceID, &v.LatestJobID, &name, &url, &format, &v.DatasetName, &v.Status, &v.InstanceID, &instanceURL, &v.CreatedAt, &v.UpdatedAt, &v.JobCount)
	if err != nil {
		return v, translateNotFound(err)
	}
	v.ResourceName, v.ResourceURL, v.ResourceFormat = nullString(name), nullString(url), nullString(format)
	if instanceURL.Valid {
		v.CKANResourceURL = resourceURL(instanceURL.String, v.DatasetName, v.ResourceID)
	}
	v.Labels = []string{}
	return v, nil
}
func (s *Store) ListResources(ctx context.Context, q app.ResourceQuery) ([]app.ResourceView, error) {
	where, args := []string{"1=1"}, []any{}
	if q.Status != "" {
		where = append(where, "l.status=?")
		args = append(args, q.Status)
	}
	if q.InstanceID != "" {
		where = append(where, "l.instance_id=?")
		args = append(args, q.InstanceID)
	}
	if q.Search != "" {
		where = append(where, "(l.resource_name LIKE ? OR l.dataset_name LIKE ? OR l.resource_id LIKE ?)")
		like := "%" + q.Search + "%"
		args = append(args, like, like, like)
	}
	args = append(args, q.Limit, q.Offset)
	rows, err := queryContext(ctx, s.DB, `SELECT `+resourceColumns+` FROM latest_resource_job l LEFT JOIN ckan_instance i ON i.id=l.instance_id WHERE `+strings.Join(where, " AND ")+` ORDER BY l.updated_at DESC LIMIT ? OFFSET ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []app.ResourceView{}
	for rows.Next() {
		v, err := scanResource(rows)
		if err != nil {
			return nil, err
		}
		v.Labels, err = s.labels(ctx, v.ResourceID)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) GetResource(ctx context.Context, id string) (*app.ResourceView, error) {
	v, err := scanResource(queryRowContext(ctx, s.DB, `SELECT `+resourceColumns+` FROM latest_resource_job l LEFT JOIN ckan_instance i ON i.id=l.instance_id WHERE l.resource_id=?`, id))
	if err != nil {
		return nil, err
	}
	v.Labels, err = s.labels(ctx, id)
	if err != nil {
		return nil, err
	}
	jobs, err := s.ListJobs(ctx, app.JobQuery{ResourceID: id, Limit: 500})
	if err != nil {
		return nil, err
	}
	v.Jobs = jobs
	if len(jobs) > 0 {
		latest := jobs[0]
		latest.Children, err = s.childJobs(ctx, latest.ID)
		if err != nil {
			return nil, err
		}
		results, resultErr := s.results(ctx, latest.ID)
		latest.Results, err = &results, resultErr
		if err != nil {
			return nil, err
		}
		v.LatestJob = &latest
		v.Preview = []any{}
		for _, result := range *latest.Results {
			if result.Success && len(result.DatasetPreview) > 0 {
				_ = json.Unmarshal(result.DatasetPreview, &v.Preview)
				break
			}
		}
	}
	return &v, nil
}

func (s *Store) ListDatasets(ctx context.Context, q app.ResourceQuery) ([]app.DatasetView, error) {
	where, args := []string{"1=1"}, []any{}
	if q.InstanceID != "" {
		where = append(where, "l.instance_id=?")
		args = append(args, q.InstanceID)
	}
	if q.Search != "" {
		where = append(where, "l.dataset_name LIKE ?")
		args = append(args, "%"+q.Search+"%")
	}
	args = append(args, q.Limit, q.Offset)
	query := `SELECT l.instance_id,i.name,i.url,l.dataset_name,COUNT(*),SUM(l.status='pending'),SUM(l.status='processing'),SUM(l.status='completed'),SUM(l.status='failed'),SUM(l.status='outdated'),SUM(l.status='deleted'),SUM(EXISTS(SELECT 1 FROM resource_metadata_label e WHERE e.resource_id=l.resource_id AND e.label='empty')),MAX(l.updated_at),i.last_metadata_synced FROM latest_resource_job l LEFT JOIN ckan_instance i ON i.id=l.instance_id WHERE ` + strings.Join(where, " AND ") + ` GROUP BY l.instance_id,i.name,i.url,i.last_metadata_synced,l.dataset_name ORDER BY MAX(l.updated_at) DESC LIMIT ? OFFSET ?`
	rows, err := queryContext(ctx, s.DB, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []app.DatasetView{}
	for rows.Next() {
		var v app.DatasetView
		var name, url sql.NullString
		var updated, synced sql.NullTime
		if err := rows.Scan(&v.InstanceID, &name, &url, &v.DatasetName, &v.TotalResources, &v.PendingResources, &v.ProcessingResources, &v.CompletedResources, &v.FailedResources, &v.OutdatedResources, &v.DeletedResources, &v.EmptyResources, &updated, &synced); err != nil {
			return nil, err
		}
		v.InstanceName = nullString(name)
		v.UpdatedAt, v.InstanceLastSyncedAt = nullTime(updated), nullTime(synced)
		if url.Valid {
			v.CKANDatasetURL = app.SourceDatasetURL(url.String, v.DatasetName)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) Dashboard(ctx context.Context) ([]app.InstanceStats, error) {
	instances, err := s.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]app.InstanceStats, 0, len(instances))
	for _, instance := range instances {
		v := app.InstanceStats{Instance: instance}
		err = queryRowContext(ctx, s.DB, `SELECT COALESCE(SUM(status='pending'),0),COALESCE(SUM(status='processing'),0),COALESCE(SUM(status='completed'),0),COALESCE(SUM(status='failed'),0),COALESCE(SUM(status='deleted'),0),COALESCE(SUM(status='outdated'),0),COALESCE(SUM(EXISTS(SELECT 1 FROM resource_metadata_label e WHERE e.resource_id=l.resource_id AND e.label='empty')),0) FROM latest_resource_job l WHERE instance_id=?`, instance.ID).Scan(&v.Pending, &v.Processing, &v.Completed, &v.Failed, &v.Deleted, &v.Outdated, &v.Empty)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

const syncColumns = `s.id,s.instance_id,i.name,s.start_time,s.end_time,s.status,s.error_message,s.total_packages,s.new_datasets,s.new_resources,s.updated_datasets,s.updated_resources,s.outdated_resources,s.deleted_datasets,s.deleted_resources`

func scanSync(row scanner) (app.MetadataSync, error) {
	var v app.MetadataSync
	var name, status, message sql.NullString
	var end sql.NullTime
	err := row.Scan(&v.ID, &v.InstanceID, &name, &v.StartTime, &end, &status, &message, &v.TotalPackages, &v.NewDatasets, &v.NewResources, &v.UpdatedDatasets, &v.UpdatedResources, &v.OutdatedResources, &v.DeletedDatasets, &v.DeletedResources)
	if err != nil {
		return v, translateNotFound(err)
	}
	v.InstanceName, v.EndTime, v.ErrorMessage = nullString(name), nullTime(end), nullString(message)
	if status.Valid {
		v.Status = status.String
	}
	return v, nil
}
func (s *Store) ListSyncs(ctx context.Context, q app.SyncQuery) ([]app.MetadataSync, error) {
	where, args := "", []any{}
	if q.InstanceID != "" {
		where = " WHERE s.instance_id=?"
		args = append(args, q.InstanceID)
	}
	args = append(args, q.Limit, q.Offset)
	rows, err := queryContext(ctx, s.DB, `SELECT `+syncColumns+` FROM metadata_sync s LEFT JOIN ckan_instance i ON i.id=s.instance_id`+where+` ORDER BY s.start_time DESC LIMIT ? OFFSET ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []app.MetadataSync{}
	for rows.Next() {
		v, err := scanSync(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) GetSync(ctx context.Context, id string) (*app.MetadataSync, error) {
	v, err := scanSync(queryRowContext(ctx, s.DB, `SELECT `+syncColumns+` FROM metadata_sync s LEFT JOIN ckan_instance i ON i.id=s.instance_id WHERE s.id=?`, id))
	return &v, err
}

var _ app.QueryStore = (*Store)(nil)
