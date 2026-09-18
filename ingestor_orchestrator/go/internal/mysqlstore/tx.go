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

package mysqlstore

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/go/internal/app"
)

func (s *txStore) Job(ctx context.Context, id string) (*app.Job, error) {
	row := s.tx.QueryRowContext(ctx, `SELECT j.id,j.resource_id,j.resource_name,j.resource_url,j.resource_format,j.dataset_name,j.status,j.idempotency_key,j.instance_id,i.name,i.url,j.ckan_url,j.datastore_active,j.created_at,j.updated_at,j.started_at,j.completed_at,j.broker_type,j.message_stream,j.message_topic,j.message_partition,j.message_offset FROM ckan_data_job j LEFT JOIN ckan_instance i ON i.id=j.instance_id WHERE j.id=? FOR UPDATE`, id)
	var v app.Job
	var name, url, format, instanceName, instanceURL, broker, stream, topic sql.NullString
	var started, completed sql.NullTime
	var partition, offset sql.NullInt64
	if err := row.Scan(&v.ID, &v.ResourceID, &name, &url, &format, &v.DatasetName, &v.Status, &v.IdempotencyKey, &v.InstanceID, &instanceName, &instanceURL, &v.CKANURL, &v.DatastoreActive, &v.CreatedAt, &v.UpdatedAt, &started, &completed, &broker, &stream, &topic, &partition, &offset); err != nil {
		return nil, translateNotFound(err)
	}
	v.ResourceName, v.ResourceURL, v.ResourceFormat = nullString(name), nullString(url), nullString(format)
	v.InstanceName, v.StartedAt, v.CompletedAt = nullString(instanceName), nullTime(started), nullTime(completed)
	if instanceURL.Valid {
		v.InstanceURL = instanceURL.String
	}
	v.BrokerType, v.MessageStream, v.MessageTopic = nullString(broker), nullString(stream), nullString(topic)
	if partition.Valid {
		p := int(partition.Int64)
		v.MessagePartition = &p
	}
	v.MessageOffset = nullInt64(offset)
	return &v, nil
}

func (s *txStore) InsertJob(ctx context.Context, v *app.Job) error {
	_, err := s.tx.ExecContext(ctx, `INSERT INTO ckan_data_job (id,resource_id,resource_name,resource_url,resource_format,dataset_name,status,idempotency_key,instance_id,ckan_url,datastore_active,created_at,updated_at,started_at,completed_at,broker_type,message_stream,message_topic,message_partition,message_offset) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, v.ID, v.ResourceID, v.ResourceName, v.ResourceURL, v.ResourceFormat, v.DatasetName, v.Status, v.IdempotencyKey, v.InstanceID, v.CKANURL, v.DatastoreActive, v.CreatedAt, v.UpdatedAt, v.StartedAt, v.CompletedAt, v.BrokerType, v.MessageStream, v.MessageTopic, v.MessagePartition, v.MessageOffset)
	return err
}
func (s *txStore) UpdateJob(ctx context.Context, v *app.Job) error {
	_, err := s.tx.ExecContext(ctx, `UPDATE ckan_data_job SET status=?,started_at=?,completed_at=?,updated_at=? WHERE id=?`, v.Status, v.StartedAt, v.CompletedAt, v.UpdatedAt, v.ID)
	return err
}
func (s *txStore) InsertResult(ctx context.Context, v *app.JobResult) error {
	_, err := s.tx.ExecContext(ctx, `INSERT INTO ckan_data_job_result (id,job_id,success,status,error_message,error_trace,dataset_preview,rows_processed,expected_rows,resource_size,encoding,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, v.ID, v.JobID, v.Success, v.Status, v.ErrorMessage, v.ErrorTrace, nullableJSON(v.DatasetPreview), v.RowsProcessed, v.ExpectedRows, v.ResourceSize, v.Encoding, v.CreatedAt)
	return err
}
func (s *txStore) Latest(ctx context.Context, id string) (*app.LatestResource, error) {
	row := s.tx.QueryRowContext(ctx, `SELECT resource_id,latest_job_id,instance_id,resource_name,resource_url,resource_format,dataset_name,status,created_at,updated_at FROM latest_resource_job WHERE resource_id=? FOR UPDATE`, id)
	var v app.LatestResource
	var name, url, format sql.NullString
	if err := row.Scan(&v.ResourceID, &v.LatestJobID, &v.InstanceID, &name, &url, &format, &v.DatasetName, &v.Status, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, translateNotFound(err)
	}
	v.ResourceName, v.ResourceURL, v.ResourceFormat = nullString(name), nullString(url), nullString(format)
	return &v, nil
}
func (s *txStore) PutLatest(ctx context.Context, v *app.LatestResource) error {
	_, err := s.tx.ExecContext(ctx, `INSERT INTO latest_resource_job (resource_id,latest_job_id,instance_id,resource_name,resource_url,resource_format,dataset_name,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE latest_job_id=VALUES(latest_job_id),instance_id=VALUES(instance_id),resource_name=VALUES(resource_name),resource_url=VALUES(resource_url),resource_format=VALUES(resource_format),dataset_name=VALUES(dataset_name),status=VALUES(status),updated_at=VALUES(updated_at)`, v.ResourceID, v.LatestJobID, v.InstanceID, v.ResourceName, v.ResourceURL, v.ResourceFormat, v.DatasetName, v.Status, v.CreatedAt, v.UpdatedAt)
	return err
}
func (s *txStore) Terminal(ctx context.Context, id string) (*app.TerminalState, error) {
	row := s.tx.QueryRowContext(ctx, `SELECT resource_id,last_terminal_job_id,last_terminal_status,last_terminal_at,last_successful_job_id FROM last_terminal_status WHERE resource_id=? FOR UPDATE`, id)
	var v app.TerminalState
	var success sql.NullString
	if err := row.Scan(&v.ResourceID, &v.LastTerminalJobID, &v.LastTerminalStatus, &v.LastTerminalAt, &success); err != nil {
		return nil, translateNotFound(err)
	}
	v.LastSuccessfulJobID = nullString(success)
	return &v, nil
}
func (s *txStore) PutTerminal(ctx context.Context, v *app.TerminalState) error {
	_, err := s.tx.ExecContext(ctx, `INSERT INTO last_terminal_status (resource_id,last_terminal_job_id,last_terminal_status,last_terminal_at,last_successful_job_id) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE last_terminal_job_id=VALUES(last_terminal_job_id),last_terminal_status=VALUES(last_terminal_status),last_terminal_at=VALUES(last_terminal_at),last_successful_job_id=VALUES(last_successful_job_id)`, v.ResourceID, v.LastTerminalJobID, v.LastTerminalStatus, v.LastTerminalAt, v.LastSuccessfulJobID)
	return err
}
func (s *txStore) AddLabel(ctx context.Context, resourceID, label string) error {
	_, err := s.tx.ExecContext(ctx, `INSERT IGNORE INTO resource_metadata_label (id,resource_id,label,created_at) VALUES (?,?,?,UTC_TIMESTAMP())`, uuid.NewString(), resourceID, label)
	return err
}
func (s *txStore) RemoveLabel(ctx context.Context, resourceID, label string) error {
	_, err := s.tx.ExecContext(ctx, `DELETE FROM resource_metadata_label WHERE resource_id=? AND label=?`, resourceID, label)
	return err
}
func (s *txStore) PutCSVHint(ctx context.Context, resourceID, delimiter string) error {
	_, err := s.tx.ExecContext(ctx, `INSERT INTO csv_hint (resource_id,delimiter) VALUES (?,?) ON DUPLICATE KEY UPDATE delimiter=VALUES(delimiter)`, resourceID, delimiter)
	return err
}
func (s *txStore) CSVHint(ctx context.Context, resourceID string) (*string, error) {
	var v string
	if err := s.tx.QueryRowContext(ctx, `SELECT delimiter FROM csv_hint WHERE resource_id=?`, resourceID).Scan(&v); err != nil {
		return nil, translateNotFound(err)
	}
	return &v, nil
}
func (s *txStore) Sync(ctx context.Context, id string) (*app.MetadataSync, error) {
	row := s.tx.QueryRowContext(ctx, `SELECT id,instance_id,start_time,end_time,status,error_message,total_packages,new_datasets,new_resources,updated_datasets,updated_resources,deleted_datasets,deleted_resources FROM metadata_sync WHERE id=? FOR UPDATE`, id)
	var v app.MetadataSync
	var end sql.NullTime
	var status, message sql.NullString
	if err := row.Scan(&v.ID, &v.InstanceID, &v.StartTime, &end, &status, &message, &v.TotalPackages, &v.NewDatasets, &v.NewResources, &v.UpdatedDatasets, &v.UpdatedResources, &v.DeletedDatasets, &v.DeletedResources); err != nil {
		return nil, translateNotFound(err)
	}
	v.EndTime, v.ErrorMessage = nullTime(end), nullString(message)
	if status.Valid {
		v.Status = status.String
	}
	return &v, nil
}
func (s *txStore) InsertSync(ctx context.Context, v *app.MetadataSync) error {
	_, err := s.tx.ExecContext(ctx, `INSERT INTO metadata_sync (id,instance_id,start_time,status,total_packages,new_datasets,new_resources,updated_datasets,updated_resources,deleted_datasets,deleted_resources) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, v.ID, v.InstanceID, v.StartTime, v.Status, 0, 0, 0, 0, 0, 0, 0)
	return err
}
func (s *txStore) UpdateSync(ctx context.Context, v *app.MetadataSync) error {
	_, err := s.tx.ExecContext(ctx, `UPDATE metadata_sync SET end_time=?,status=?,error_message=?,total_packages=?,new_datasets=?,new_resources=?,updated_datasets=?,updated_resources=?,deleted_datasets=?,deleted_resources=? WHERE id=?`, v.EndTime, v.Status, v.ErrorMessage, v.TotalPackages, v.NewDatasets, v.NewResources, v.UpdatedDatasets, v.UpdatedResources, v.DeletedDatasets, v.DeletedResources, v.ID)
	return err
}
func (s *txStore) Instance(ctx context.Context, id string) (*app.Instance, error) {
	row := s.tx.QueryRowContext(ctx, `SELECT id,name,url,dataset_count,resource_count,last_metadata_synced FROM ckan_instance WHERE id=? FOR UPDATE`, id)
	var v app.Instance
	var synced sql.NullTime
	if err := row.Scan(&v.ID, &v.Name, &v.URL, &v.DatasetCount, &v.ResourceCount, &synced); err != nil {
		return nil, translateNotFound(err)
	}
	v.LastMetadataSynced = nullTime(synced)
	return &v, nil
}
func (s *txStore) UpdateInstance(ctx context.Context, v *app.Instance) error {
	_, err := s.tx.ExecContext(ctx, `UPDATE ckan_instance SET dataset_count=?,resource_count=?,last_metadata_synced=? WHERE id=?`, v.DatasetCount, v.ResourceCount, v.LastMetadataSynced, v.ID)
	return err
}
func nullableJSON(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

var _ app.Tx = (*txStore)(nil)
