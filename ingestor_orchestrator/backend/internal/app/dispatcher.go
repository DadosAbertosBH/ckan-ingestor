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
	"time"
)

var ErrConflict = errors.New("conflict")

type Dispatcher struct {
	SICOMEnabled  bool // Include SICOM in bulk and scheduled syncs; individual syncs are always available.
	Store         Store
	Publisher     Publisher
	RetryTopic    string
	MetadataTopic string
	Now           func() time.Time
	NewID         func() string
}

func (d *Dispatcher) now() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}

func (d *Dispatcher) newID() string {
	if d.NewID != nil {
		return d.NewID()
	}
	return fmt.Sprintf("%d", d.now().UnixNano())
}

func (d *Dispatcher) RetryJob(ctx context.Context, jobID string) (*Job, error) {
	var retried *Job
	err := d.Store.InTx(ctx, func(tx Tx) error {
		job, err := tx.Job(ctx, jobID)
		if err != nil {
			return err
		}
		if job.ParentJobID != nil {
			return fmt.Errorf("%w: retry the parent job %s", ErrInvalidMessage, *job.ParentJobID)
		}
		retried = job
		payload := map[string]any{"job_id": job.ID, "resource_id": job.ResourceID, "ckan_url": job.CKANURL, "resource_url": stringValue(job.ResourceURL), "resource_format": stringValue(job.ResourceFormat), "datastore_active": job.DatastoreActive, "dataset_name": job.DatasetName, "instance_id": job.InstanceID, "resource_name": job.ResourceName}
		if job.SourceVersion != nil {
			payload["source_version"] = *job.SourceVersion
		}
		if hint, hintErr := tx.CSVHint(ctx, job.ResourceID); hintErr == nil {
			payload["csv_delimiter"] = *hint
		} else if !errors.Is(hintErr, ErrNotFound) {
			return hintErr
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		_, err = d.Publisher.Publish(ctx, d.RetryTopic, encoded, job.ResourceID)
		return err
	})
	return retried, err
}

func (d *Dispatcher) SyncInstance(ctx context.Context, instanceID string) (*MetadataSync, error) {
	var created *MetadataSync
	var publishErr error
	err := d.Store.InTx(ctx, func(tx Tx) error {
		instance, err := tx.Instance(ctx, instanceID)
		if err != nil {
			return err
		}
		now := d.now()
		created = &MetadataSync{ID: d.newID(), InstanceID: instance.ID, StartTime: now, Status: "pending"}
		if err := tx.InsertSync(ctx, created); err != nil {
			return err
		}
		payload, err := json.Marshal(map[string]string{"sync_id": created.ID, "instance_id": instance.ID, "instance_name": instance.Name, "instance_url": instance.URL, "source_type": instance.SourceType})
		if err != nil {
			return err
		}
		if _, err = d.Publisher.Publish(ctx, d.MetadataTopic, payload, created.ID); err != nil {
			publishErr = err
			message := truncate(err.Error(), 16000)
			created.Status, created.EndTime, created.ErrorMessage = "failure", &now, &message
			return tx.UpdateSync(ctx, created)
		}
		return nil
	})
	if err != nil {
		return created, err
	}
	return created, publishErr
}

type SyncDispatchResult struct {
	SyncID     string `json:"sync_id,omitempty"`
	InstanceID string `json:"instance_id"`
	Status     string `json:"status,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (d *Dispatcher) SyncAll(ctx context.Context) []SyncDispatchResult {
	instances, err := d.Store.Instances(ctx)
	if err != nil {
		return []SyncDispatchResult{{Error: err.Error()}}
	}
	results := make([]SyncDispatchResult, 0, len(instances))
	for _, instance := range instances {
		if instance.SourceType == "sicom" && !d.SICOMEnabled {
			continue
		}
		sync, syncErr := d.SyncInstance(ctx, instance.ID)
		result := SyncDispatchResult{InstanceID: instance.ID}
		if sync != nil {
			result.SyncID, result.Status = sync.ID, sync.Status
		}
		if syncErr != nil {
			result.Error = syncErr.Error()
		}
		results = append(results, result)
	}
	return results
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
