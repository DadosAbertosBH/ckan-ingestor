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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var ErrNotFound = errors.New("not found")
var ErrInvalidMessage = errors.New("invalid message")

type Processor struct {
	Store Store
	Now   func() time.Time
	NewID func() string
}

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now().UTC()
	}
	return time.Now().UTC()
}

func (p *Processor) newID() string {
	if p.NewID != nil {
		return p.NewID()
	}
	return fmt.Sprintf("%d", p.now().UnixNano())
}

func (p *Processor) ApplyJobResult(ctx context.Context, message JobResultMessage) error {
	switch message.Status {
	case "PENDING", "PROCESSING", "SUCCESS", "FAILED", "DELETED":
	default:
		return fmt.Errorf("%w: unknown job status %q", ErrInvalidMessage, message.Status)
	}
	if message.Status == "SUCCESS" && message.JobID == "" && message.ResourceID != "" {
		return nil
	}
	if message.JobID == "" || message.ResourceID == "" {
		return fmt.Errorf("%w: job_id and resource_id are required", ErrInvalidMessage)
	}
	return p.Store.InTx(ctx, func(tx Tx) error {
		if message.Status == "PENDING" {
			return p.applyPending(ctx, tx, message)
		}
		job, err := tx.Job(ctx, message.JobID)
		if err != nil {
			return err
		}
		switch message.Status {
		case "PROCESSING":
			return p.applyProcessing(ctx, tx, job)
		case "FAILED":
			return p.applyFailed(ctx, tx, job, message)
		case "DELETED":
			return p.applyDeleted(ctx, tx, job)
		default:
			return p.applySuccess(ctx, tx, job, message)
		}
	})
}

func (p *Processor) applyPending(ctx context.Context, tx Tx, message JobResultMessage) error {
	if _, err := tx.Job(ctx, message.JobID); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	if message.DatasetName == nil || message.InstanceID == nil {
		return fmt.Errorf("%w: pending result requires dataset_name and instance_id", ErrInvalidMessage)
	}
	now := p.now()
	job := &Job{
		ID: message.JobID, ResourceID: message.ResourceID, ResourceName: message.ResourceName,
		ResourceURL: message.ResourceURL, ResourceFormat: message.ResourceFormat,
		DatasetName: *message.DatasetName, Status: JobPending, IdempotencyKey: message.ResourceID,
		InstanceID: *message.InstanceID, CreatedAt: now, UpdatedAt: now,
	}
	if message.CKANURL != nil {
		job.CKANURL = *message.CKANURL
	}
	if message.DatastoreActive != nil {
		job.DatastoreActive = *message.DatastoreActive
	}
	if err := tx.InsertJob(ctx, job); err != nil {
		return err
	}
	status, err := p.resourceStatus(ctx, tx, job.ResourceID, JobPending)
	if err != nil {
		return err
	}
	return tx.PutLatest(ctx, latestFromJob(job, status, now))
}

func (p *Processor) applyProcessing(ctx context.Context, tx Tx, job *Job) error {
	if job.Status == JobCompleted || job.Status == JobFailed || job.Status == JobDeleted {
		return nil
	}
	now := p.now()
	job.Status, job.StartedAt, job.CompletedAt, job.UpdatedAt = JobProcessing, &now, nil, now
	if err := tx.UpdateJob(ctx, job); err != nil {
		return err
	}
	return p.updateLatest(ctx, tx, job, string(JobProcessing))
}

func (p *Processor) applyFailed(ctx context.Context, tx Tx, job *Job, message JobResultMessage) error {
	if job.Status == JobFailed || job.Status == JobCompleted || job.Status == JobDeleted {
		return nil
	}
	now := p.now()
	errorMessage := ""
	if message.ErrorMessage != nil {
		errorMessage = truncate(*message.ErrorMessage, 16000)
	}
	emptyTrace := ""
	if err := tx.InsertResult(ctx, &JobResult{ID: p.newID(), JobID: job.ID, Success: false, Status: JobFailed, ErrorMessage: &errorMessage, ErrorTrace: &emptyTrace, CreatedAt: now}); err != nil {
		return err
	}
	job.Status, job.CompletedAt, job.UpdatedAt = JobFailed, &now, now
	if err := tx.UpdateJob(ctx, job); err != nil {
		return err
	}
	if err := p.putTerminal(ctx, tx, job, JobFailed, now); err != nil {
		return err
	}
	return p.updateLatest(ctx, tx, job, string(JobFailed))
}

func (p *Processor) applyDeleted(ctx context.Context, tx Tx, job *Job) error {
	if job.Status == JobDeleted || job.Status == JobCompleted || job.Status == JobFailed {
		return nil
	}
	now := p.now()
	if err := tx.InsertResult(ctx, &JobResult{ID: p.newID(), JobID: job.ID, Success: true, Status: JobDeleted, CreatedAt: now}); err != nil {
		return err
	}
	job.Status, job.CompletedAt, job.UpdatedAt = JobDeleted, &now, now
	if err := tx.UpdateJob(ctx, job); err != nil {
		return err
	}
	if err := p.putTerminal(ctx, tx, job, JobDeleted, now); err != nil {
		return err
	}
	return p.updateLatest(ctx, tx, job, string(JobDeleted))
}

func (p *Processor) applySuccess(ctx context.Context, tx Tx, job *Job, message JobResultMessage) error {
	if job.Status == JobCompleted || job.Status == JobFailed || job.Status == JobDeleted {
		return nil
	}
	now := p.now()
	rows := int64(0)
	if message.RowsProcessed != nil {
		rows = *message.RowsProcessed
	}
	result := &JobResult{ID: p.newID(), JobID: job.ID, Success: true, Status: JobCompleted, RowsProcessed: &rows, ExpectedRows: message.ExpectedRows, ResourceSize: message.ResourceSize, Encoding: message.Encoding, CreatedAt: now}
	if len(message.Preview) > 0 && string(message.Preview) != "null" {
		if !json.Valid(message.Preview) {
			return fmt.Errorf("%w: invalid preview", ErrInvalidMessage)
		}
		result.DatasetPreview = append(json.RawMessage(nil), message.Preview...)
	}
	if err := tx.InsertResult(ctx, result); err != nil {
		return err
	}
	if message.CSVDelimiter != nil && *message.CSVDelimiter != "" {
		if err := tx.PutCSVHint(ctx, job.ResourceID, *message.CSVDelimiter); err != nil {
			return err
		}
	}
	job.Status, job.CompletedAt, job.UpdatedAt = JobCompleted, &now, now
	if err := tx.UpdateJob(ctx, job); err != nil {
		return err
	}
	if err := p.putTerminal(ctx, tx, job, JobCompleted, now); err != nil {
		return err
	}
	if rows == 0 {
		if err := tx.AddLabel(ctx, job.ResourceID, "empty"); err != nil {
			return err
		}
	} else if err := p.applyLabels(ctx, tx, job.ResourceID, message, rows); err != nil {
		return err
	}
	return p.updateLatest(ctx, tx, job, string(JobCompleted))
}

func (p *Processor) applyLabels(ctx context.Context, tx Tx, resourceID string, message JobResultMessage, rows int64) error {
	if err := tx.RemoveLabel(ctx, resourceID, "empty"); err != nil {
		return err
	}
	labels := make([]string, 0, 12)
	if rows == 1 {
		labels = append(labels, "single-row")
	}
	columns := previewColumns(message.Preview)
	if columns == 1 {
		labels = append(labels, "single-column")
	}
	if message.ExpectedRows != nil && rows != *message.ExpectedRows {
		labels = append(labels, "row-count-mismatch")
	}
	if message.ExpectedColumns != nil && columns > 0 && int64(columns) != *message.ExpectedColumns {
		labels = append(labels, "column-count-mismatch")
	}
	if message.ResourceSize != nil {
		switch {
		case *message.ResourceSize < 1_000_000:
			labels = append(labels, "size:small")
		case *message.ResourceSize < 1_000_000_000:
			labels = append(labels, "size:medium")
		default:
			labels = append(labels, "size:large")
		}
	}
	if message.Encoding != nil && *message.Encoding != "" && *message.Encoding != "utf-8" {
		labels = append(labels, "encoding:"+*message.Encoding)
	}
	if message.CSVStrictMode != nil {
		labels = append(labels, "csv-strict-mode:"+strconv.FormatBool(*message.CSVStrictMode))
	}
	for _, value := range []struct {
		prefix string
		value  *string
	}{{"csv-delimiter:", message.CSVDelimiter}, {"csv-samples:", message.CSVSamples}, {"reader:", message.Reader}} {
		if value.value != nil && *value.value != "" {
			labels = append(labels, value.prefix+*value.value)
		}
	}
	if message.DatastoreActive != nil && *message.DatastoreActive {
		labels = append(labels, "datastore")
	}
	for _, label := range labels {
		if err := tx.AddLabel(ctx, resourceID, label); err != nil {
			return err
		}
	}
	return nil
}

func previewColumns(raw json.RawMessage) int {
	var rows []map[string]any
	if len(raw) == 0 || json.Unmarshal(raw, &rows) != nil || len(rows) == 0 {
		return 0
	}
	return len(rows[0])
}

func (p *Processor) putTerminal(ctx context.Context, tx Tx, job *Job, status JobStatus, now time.Time) error {
	terminal, err := tx.Terminal(ctx, job.ResourceID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if terminal == nil {
		terminal = &TerminalState{ResourceID: job.ResourceID}
	}
	terminal.LastTerminalJobID, terminal.LastTerminalStatus, terminal.LastTerminalAt = job.ID, status, now
	if status == JobCompleted {
		id := job.ID
		terminal.LastSuccessfulJobID = &id
	}
	return tx.PutTerminal(ctx, terminal)
}

func (p *Processor) resourceStatus(ctx context.Context, tx Tx, resourceID string, current JobStatus) (string, error) {
	if current != JobPending {
		return string(current), nil
	}
	terminal, err := tx.Terminal(ctx, resourceID)
	if errors.Is(err, ErrNotFound) {
		return string(JobPending), nil
	}
	if err != nil {
		return "", err
	}
	if terminal.LastTerminalStatus == JobFailed {
		return string(JobFailed), nil
	}
	if terminal.LastTerminalStatus == JobCompleted {
		return "outdated", nil
	}
	return string(JobPending), nil
}

func (p *Processor) updateLatest(ctx context.Context, tx Tx, job *Job, status string) error {
	latest, err := tx.Latest(ctx, job.ResourceID)
	if errors.Is(err, ErrNotFound) || (err == nil && latest.LatestJobID != job.ID) {
		return nil
	}
	if err != nil {
		return err
	}
	latest.Status, latest.UpdatedAt = status, p.now()
	latest.ResourceName, latest.ResourceURL, latest.ResourceFormat, latest.DatasetName = job.ResourceName, job.ResourceURL, job.ResourceFormat, job.DatasetName
	return tx.PutLatest(ctx, latest)
}

func latestFromJob(job *Job, status string, now time.Time) *LatestResource {
	return &LatestResource{ResourceID: job.ResourceID, LatestJobID: job.ID, InstanceID: job.InstanceID, ResourceName: job.ResourceName, ResourceURL: job.ResourceURL, ResourceFormat: job.ResourceFormat, DatasetName: job.DatasetName, Status: status, CreatedAt: now, UpdatedAt: now}
}

func truncate(value string, length int) string {
	if len(value) <= length {
		return value
	}
	return value[:length]
}

func (p *Processor) ApplyMetadataResult(ctx context.Context, message MetadataSyncResultMessage) error {
	if message.SyncID == "" || (message.Status != "success" && message.Status != "failure") {
		return fmt.Errorf("%w: invalid metadata sync result", ErrInvalidMessage)
	}
	return p.Store.InTx(ctx, func(tx Tx) error {
		sync, err := tx.Sync(ctx, message.SyncID)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if sync.Status == "success" || sync.Status == "failure" {
			return nil
		}
		now := p.now()
		sync.EndTime = &now
		sync.TotalPackages, sync.NewDatasets, sync.NewResources = message.TotalPackages, message.NewDatasets, message.NewResources
		sync.UpdatedDatasets, sync.UpdatedResources = message.UpdatedDatasets, message.UpdatedResources
		sync.OutdatedResources = message.OutdatedResources
		sync.DeletedDatasets, sync.DeletedResources = message.DeletedDatasets, message.DeletedResources
		if message.Status == "failure" {
			sync.Status = "failure"
			if message.ErrorMessage != nil {
				value := truncate(*message.ErrorMessage, 16000)
				sync.ErrorMessage = &value
			}
			return tx.UpdateSync(ctx, sync)
		}
		instance, err := tx.Instance(ctx, sync.InstanceID)
		if errors.Is(err, ErrNotFound) {
			sync.Status = "failure"
			return tx.UpdateSync(ctx, sync)
		}
		if err != nil {
			return err
		}
		instance.DatasetCount, instance.ResourceCount, instance.LastMetadataSynced = message.DatasetCount, message.ResourceCount, &now
		if err := tx.UpdateInstance(ctx, instance); err != nil {
			return err
		}
		sync.Status, sync.ErrorMessage = "success", nil
		return tx.UpdateSync(ctx, sync)
	})
}
