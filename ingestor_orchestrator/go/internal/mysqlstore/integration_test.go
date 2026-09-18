// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package mysqlstore

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/go/internal/app"
)

func TestQueryStoreAgainstMySQL(t *testing.T) {
	dsn := os.Getenv("MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("MYSQL_INTEGRATION_DSN is not set")
	}
	store, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	cleanup := func() {
		for _, statement := range []string{
			"DELETE FROM resource_metadata_label WHERE resource_id='integration-resource'",
			"DELETE FROM last_terminal_status WHERE resource_id='integration-resource'",
			"DELETE FROM latest_resource_job WHERE resource_id='integration-resource'",
			"DELETE FROM ckan_data_job_result WHERE job_id='integration-job'",
			"DELETE FROM metadata_sync WHERE instance_id='integration-instance'",
			"DELETE FROM ckan_data_job WHERE id='integration-job'",
			"DELETE FROM ckan_instance WHERE id='integration-instance'",
		} {
			_, _ = store.DB.ExecContext(ctx, statement)
		}
	}
	cleanup()
	defer cleanup()
	instance := &app.Instance{ID: "integration-instance", Name: "Integration", URL: "https://data.test", CreatedAt: now, UpdatedAt: now}
	if err := store.CreateInstance(ctx, instance); err != nil {
		t.Fatal(err)
	}
	job := &app.Job{ID: "integration-job", ResourceID: "integration-resource", DatasetName: "dataset", Status: app.JobCompleted, IdempotencyKey: "integration-key", InstanceID: instance.ID, CKANURL: instance.URL, CreatedAt: now, UpdatedAt: now}
	result := &app.JobResult{ID: "integration-result", JobID: job.ID, Success: true, Status: app.JobCompleted, DatasetPreview: json.RawMessage(`[{"a":1}]`), CreatedAt: now}
	sync := &app.MetadataSync{ID: "integration-sync", InstanceID: instance.ID, StartTime: now, Status: "pending"}
	err = store.InTx(ctx, func(tx app.Tx) error {
		if err := tx.InsertJob(ctx, job); err != nil {
			return err
		}
		if err := tx.InsertResult(ctx, result); err != nil {
			return err
		}
		if err := tx.PutLatest(ctx, &app.LatestResource{ResourceID: job.ResourceID, LatestJobID: job.ID, InstanceID: instance.ID, DatasetName: job.DatasetName, Status: "completed", CreatedAt: now, UpdatedAt: now}); err != nil {
			return err
		}
		if err := tx.AddLabel(ctx, job.ResourceID, "empty"); err != nil {
			return err
		}
		return tx.InsertSync(ctx, sync)
	})
	if err != nil {
		t.Fatal(err)
	}
	instances, err := store.ListInstances(ctx)
	if err != nil || len(instances) != 1 {
		t.Fatalf("instances=%+v err=%v", instances, err)
	}
	jobs, err := store.ListJobs(ctx, app.JobQuery{Tags: "empty", Limit: 50})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%+v err=%v", jobs, err)
	}
	detail, err := store.GetJob(ctx, job.ID)
	if err != nil || detail.Results == nil || len(*detail.Results) != 1 {
		t.Fatalf("job=%+v err=%v", detail, err)
	}
	resources, err := store.ListResources(ctx, app.ResourceQuery{Limit: 50})
	if err != nil || len(resources) != 1 {
		t.Fatalf("resources=%+v err=%v", resources, err)
	}
	resource, err := store.GetResource(ctx, job.ResourceID)
	if err != nil || resource.Preview == nil {
		t.Fatalf("resource=%+v err=%v", resource, err)
	}
	datasets, err := store.ListDatasets(ctx, app.ResourceQuery{Limit: 50})
	if err != nil || len(datasets) != 1 || datasets[0].CompletedResources != 1 {
		t.Fatalf("datasets=%+v err=%v", datasets, err)
	}
	stats, err := store.Dashboard(ctx)
	if err != nil || len(stats) != 1 || stats[0].Completed != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	syncs, err := store.ListSyncs(ctx, app.SyncQuery{InstanceID: instance.ID, Limit: 50})
	if err != nil || len(syncs) < 1 {
		t.Fatalf("syncs=%+v err=%v", syncs, err)
	}
	if err := store.InTx(ctx, func(tx app.Tx) error {
		sync.OutdatedResources = 6
		return tx.UpdateSync(ctx, sync)
	}); err != nil {
		t.Fatal(err)
	}
	syncs, err = store.ListSyncs(ctx, app.SyncQuery{InstanceID: instance.ID, Limit: 50})
	if err != nil || len(syncs) < 1 || syncs[0].OutdatedResources != 6 {
		t.Fatalf("syncs=%+v err=%v", syncs, err)
	}
}
