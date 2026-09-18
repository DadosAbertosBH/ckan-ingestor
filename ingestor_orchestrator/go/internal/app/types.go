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

package app

import (
	"encoding/json"
	"time"
)

type JobStatus string

const (
	JobPending    JobStatus = "pending"
	JobProcessing JobStatus = "processing"
	JobCompleted  JobStatus = "completed"
	JobFailed     JobStatus = "failed"
	JobDeleted    JobStatus = "deleted"
)

type Job struct {
	ID               string     `json:"id"`
	ResourceID       string     `json:"resource_id"`
	ResourceName     *string    `json:"resource_name"`
	ResourceURL      *string    `json:"resource_url"`
	ResourceFormat   *string    `json:"resource_format"`
	DatasetName      string     `json:"dataset_name"`
	Status           JobStatus  `json:"status"`
	IdempotencyKey   string     `json:"idempotency_key"`
	InstanceID       string     `json:"instance_id"`
	InstanceName     *string    `json:"instance_name,omitempty"`
	InstanceURL      string     `json:"-"`
	CKANURL          string     `json:"-"`
	DatastoreActive  bool       `json:"-"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	StartedAt        *time.Time `json:"started_at"`
	CompletedAt      *time.Time `json:"completed_at"`
	BrokerType       *string    `json:"broker_type"`
	MessageStream    *string    `json:"message_stream"`
	MessageTopic     *string    `json:"message_topic"`
	MessagePartition *int       `json:"message_partition"`
	MessageOffset    *int64     `json:"message_offset"`
}

type JobResult struct {
	ID             string          `json:"id"`
	JobID          string          `json:"job_id"`
	Success        bool            `json:"success"`
	Status         JobStatus       `json:"status"`
	ErrorMessage   *string         `json:"error_message"`
	ErrorTrace     *string         `json:"error_trace"`
	DatasetPreview json.RawMessage `json:"dataset_preview"`
	RowsProcessed  *int64          `json:"rows_processed"`
	ExpectedRows   *int64          `json:"expected_rows"`
	ResourceSize   *int64          `json:"resource_size"`
	Encoding       *string         `json:"encoding"`
	CreatedAt      time.Time       `json:"created_at"`
}

type LatestResource struct {
	ResourceID     string
	LatestJobID    string
	InstanceID     string
	ResourceName   *string
	ResourceURL    *string
	ResourceFormat *string
	DatasetName    string
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type TerminalState struct {
	ResourceID          string
	LastTerminalJobID   string
	LastTerminalStatus  JobStatus
	LastTerminalAt      time.Time
	LastSuccessfulJobID *string
}

type Instance struct {
	ID                 string     `json:"id"`
	Name               string     `json:"name"`
	URL                string     `json:"url"`
	DatasetCount       int64      `json:"dataset_count"`
	ResourceCount      int64      `json:"resource_count"`
	LastMetadataSynced *time.Time `json:"last_metadata_synced"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

type MetadataSync struct {
	ID               string     `json:"id"`
	InstanceID       string     `json:"instance_id"`
	InstanceName     *string    `json:"instance_name"`
	StartTime        time.Time  `json:"start_time"`
	EndTime          *time.Time `json:"end_time"`
	Status           string     `json:"status"`
	ErrorMessage     *string    `json:"error_message"`
	TotalPackages    int64      `json:"total_packages"`
	NewDatasets      int64      `json:"new_datasets"`
	NewResources     int64      `json:"new_resources"`
	UpdatedDatasets  int64      `json:"updated_datasets"`
	UpdatedResources int64      `json:"updated_resources"`
	DeletedDatasets  int64      `json:"deleted_datasets"`
	DeletedResources int64      `json:"deleted_resources"`
}

type JobResultMessage struct {
	JobID           string          `json:"job_id"`
	Status          string          `json:"status"`
	ResourceID      string          `json:"resource_id"`
	DatasetName     *string         `json:"dataset_name,omitempty"`
	ResourceName    *string         `json:"resource_name,omitempty"`
	ResourceURL     *string         `json:"resource_url,omitempty"`
	ResourceFormat  *string         `json:"resource_format,omitempty"`
	InstanceID      *string         `json:"instance_id,omitempty"`
	CKANURL         *string         `json:"ckan_url,omitempty"`
	DatastoreActive *bool           `json:"datastore_active,omitempty"`
	Reader          *string         `json:"reader,omitempty"`
	RowsProcessed   *int64          `json:"rows_processed,omitempty"`
	ExpectedRows    *int64          `json:"expected_rows,omitempty"`
	ResourceSize    *int64          `json:"resource_size,omitempty"`
	Encoding        *string         `json:"encoding,omitempty"`
	CSVStrictMode   *bool           `json:"csv_strict_mode,omitempty"`
	CSVDelimiter    *string         `json:"csv_delimiter,omitempty"`
	CSVSamples      *string         `json:"csv_samples,omitempty"`
	ExpectedColumns *int64          `json:"expected_columns,omitempty"`
	ErrorMessage    *string         `json:"error_message,omitempty"`
	Preview         json.RawMessage `json:"preview,omitempty"`
}

type MetadataSyncResultMessage struct {
	SyncID           string  `json:"sync_id"`
	InstanceID       string  `json:"instance_id"`
	InstanceName     string  `json:"instance_name"`
	Status           string  `json:"status"`
	TotalPackages    int64   `json:"total_packages"`
	NewDatasets      int64   `json:"new_datasets"`
	NewResources     int64   `json:"new_resources"`
	UpdatedDatasets  int64   `json:"updated_datasets"`
	UpdatedResources int64   `json:"updated_resources"`
	DeletedDatasets  int64   `json:"deleted_datasets"`
	DeletedResources int64   `json:"deleted_resources"`
	DatasetCount     int64   `json:"dataset_count"`
	ResourceCount    int64   `json:"resource_count"`
	ErrorMessage     *string `json:"error_message,omitempty"`
}

type Routing struct {
	BrokerType string `json:"broker_type"`
	Stream     string `json:"stream"`
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	Offset     *int64 `json:"offset"`
}

type JobQuery struct {
	Status, ResourceID, InstanceID, Tags, OrderBy, OrderDir string
	Limit, Offset                                           int
}

type ResourceQuery struct {
	Status, InstanceID, Search string
	Limit, Offset              int
}

type SyncQuery struct {
	InstanceID    string
	Limit, Offset int
}

type JobView struct {
	Job
	CKANResourceURL string       `json:"ckan_resource_url"`
	Labels          []string     `json:"labels"`
	Results         *[]JobResult `json:"results,omitempty"`
}

type ResourceView struct {
	ResourceID      string    `json:"resource_id"`
	DatasetName     string    `json:"dataset_name"`
	Status          string    `json:"status"`
	InstanceID      string    `json:"instance_id"`
	CKANResourceURL string    `json:"ckan_resource_url"`
	ResourceName    *string   `json:"resource_name"`
	ResourceURL     *string   `json:"resource_url"`
	ResourceFormat  *string   `json:"resource_format"`
	Labels          []string  `json:"labels"`
	JobCount        int64     `json:"job_count"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	LatestJob       *JobView  `json:"latest_job,omitempty"`
	Jobs            []JobView `json:"jobs,omitempty"`
	Preview         any       `json:"preview,omitempty"`
}

type DatasetView struct {
	InstanceID           string     `json:"instance_id"`
	InstanceName         *string    `json:"instance_name"`
	DatasetName          string     `json:"dataset_name"`
	CKANDatasetURL       string     `json:"ckan_dataset_url"`
	TotalResources       int64      `json:"total_resources"`
	PendingResources     int64      `json:"pending_resources"`
	ProcessingResources  int64      `json:"processing_resources"`
	CompletedResources   int64      `json:"completed_resources"`
	FailedResources      int64      `json:"failed_resources"`
	OutdatedResources    int64      `json:"outdated_resources"`
	DeletedResources     int64      `json:"deleted_resources"`
	EmptyResources       int64      `json:"empty_resources"`
	UpdatedAt            *time.Time `json:"updated_at"`
	InstanceLastSyncedAt *time.Time `json:"instance_last_synced_at"`
}

type InstanceStats struct {
	Instance   Instance `json:"instance"`
	Pending    int64    `json:"pending"`
	Processing int64    `json:"processing"`
	Completed  int64    `json:"completed"`
	Failed     int64    `json:"failed"`
	Deleted    int64    `json:"deleted"`
	Outdated   int64    `json:"outdated"`
	Empty      int64    `json:"empty"`
}
