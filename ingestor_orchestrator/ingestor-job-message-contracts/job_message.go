// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

// Package jobmessage defines the Iggy wire messages consumed by ingestion workers and the orchestrator.
package jobmessage

import "encoding/json"

// WorkerJob is the wire format published to the Iggy jobs topic.
type WorkerJob struct {
	JobID           string `json:"job_id"`
	ResourceID      string `json:"resource_id"`
	PackageID       string `json:"package_id,omitempty"`
	CKANURL         string `json:"ckan_url"`
	InstanceID      string `json:"instance_id"`
	DatasetName     string `json:"dataset_name"`
	ResourceName    string `json:"resource_name"`
	ResourceURL     string `json:"resource_url"`
	ResourceFormat  string `json:"resource_format"`
	SourceVersion   string `json:"source_version"`
	DatastoreActive bool   `json:"datastore_active"`
}

// JobResult is the wire format accepted from the Iggy job-results topic.
type JobResult struct {
	JobID            string          `json:"job_id"`
	SourceVersion    *string         `json:"source_version,omitempty"`
	ParentJobID      *string         `json:"parent_job_id,omitempty"`
	ParentResourceID *string         `json:"parent_resource_id,omitempty"`
	ChildIndex       *int            `json:"child_index,omitempty"`
	TotalChildCount  *int            `json:"total_child_count,omitempty"`
	Status           string          `json:"status"`
	ResourceID       string          `json:"resource_id"`
	DatasetName      *string         `json:"dataset_name,omitempty"`
	ResourceName     *string         `json:"resource_name,omitempty"`
	ResourceURL      *string         `json:"resource_url,omitempty"`
	ResourceFormat   *string         `json:"resource_format,omitempty"`
	InstanceID       *string         `json:"instance_id,omitempty"`
	CKANURL          *string         `json:"ckan_url,omitempty"`
	DatastoreActive  *bool           `json:"datastore_active,omitempty"`
	Reader           *string         `json:"reader,omitempty"`
	RowsProcessed    *int64          `json:"rows_processed,omitempty"`
	ExpectedRows     *int64          `json:"expected_rows,omitempty"`
	ResourceSize     *int64          `json:"resource_size,omitempty"`
	Encoding         *string         `json:"encoding,omitempty"`
	CSVStrictMode    *bool           `json:"csv_strict_mode,omitempty"`
	CSVDelimiter     *string         `json:"csv_delimiter,omitempty"`
	CSVSamples       *string         `json:"csv_samples,omitempty"`
	ExpectedColumns  *int64          `json:"expected_columns,omitempty"`
	HTTPStatus       *int64          `json:"http_status,omitempty"`
	ErrorMessage     *string         `json:"error_message,omitempty"`
	Preview          json.RawMessage `json:"preview,omitempty"`
}

// Pending converts a worker job to the corresponding persisted-job announcement.
func Pending(job WorkerJob) JobResult {
	return JobResult{
		JobID: job.JobID, SourceVersion: ptr(job.SourceVersion), Status: "PENDING", ResourceID: job.ResourceID,
		DatasetName: ptr(job.DatasetName), ResourceName: ptr(job.ResourceName), ResourceURL: ptr(job.ResourceURL), ResourceFormat: ptr(job.ResourceFormat),
		InstanceID: ptr(job.InstanceID), CKANURL: ptr(job.CKANURL), DatastoreActive: ptr(job.DatastoreActive),
	}
}

func ptr[T any](value T) *T { return &value }
