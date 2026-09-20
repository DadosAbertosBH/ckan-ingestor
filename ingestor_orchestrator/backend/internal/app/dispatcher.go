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
	var created *Job
	err := d.Store.InTx(ctx, func(tx Tx) error {
		job, err := tx.Job(ctx, jobID)
		if err != nil {
			return err
		}
		now := d.now()
		created = &Job{ID: d.newID(), ResourceID: job.ResourceID, ResourceName: job.ResourceName, ResourceURL: job.ResourceURL, ResourceFormat: job.ResourceFormat, DatasetName: job.DatasetName, Status: JobPending, IdempotencyKey: job.IdempotencyKey, InstanceID: job.InstanceID, InstanceName: job.InstanceName, InstanceURL: job.InstanceURL, CKANURL: job.CKANURL, DatastoreActive: job.DatastoreActive, CreatedAt: now, UpdatedAt: now}
		payload := map[string]any{"job_id": created.ID, "resource_id": created.ResourceID, "ckan_url": created.CKANURL, "resource_url": stringValue(created.ResourceURL), "resource_format": stringValue(created.ResourceFormat), "datastore_active": created.DatastoreActive}
		if hint, hintErr := tx.CSVHint(ctx, job.ResourceID); hintErr == nil {
			payload["csv_delimiter"] = *hint
		} else if !errors.Is(hintErr, ErrNotFound) {
			return hintErr
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		routing, err := d.Publisher.Publish(ctx, d.RetryTopic, encoded, created.ResourceID)
		if err != nil {
			return err
		}
		created.BrokerType, created.MessageStream, created.MessageTopic = new(routing.BrokerType), new(routing.Stream), new(routing.Topic)
		created.MessagePartition, created.MessageOffset = &routing.Partition, routing.Offset
		if err := tx.InsertJob(ctx, created); err != nil {
			return err
		}
		processor := Processor{Now: d.Now}
		status, err := processor.resourceStatus(ctx, tx, created.ResourceID, JobPending)
		if err != nil {
			return err
		}
		return tx.PutLatest(ctx, latestFromJob(created, status, now))
	})
	return created, err
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
		payload, err := json.Marshal(map[string]string{"sync_id": created.ID, "instance_id": instance.ID, "instance_name": instance.Name, "instance_url": instance.URL})
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
