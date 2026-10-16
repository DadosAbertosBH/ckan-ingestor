// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package mysqlrepositories

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
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
	if err != nil {
		t.Fatalf("instances=%+v err=%v", instances, err)
	}
	foundInstance := false
	for _, listed := range instances {
		if listed.ID == instance.ID {
			foundInstance = listed.SourceType == "ckan"
		}
	}
	if !foundInstance {
		t.Fatal("integration instance missing among seeded CKAN/SICOM sources")
	}
	jobs, err := store.ListJobs(ctx, app.JobQuery{Tags: "empty", Limit: 50})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%+v err=%v", jobs, err)
	}
	detail, err := store.GetJob(ctx, job.ID)
	if err != nil || detail.Results == nil || len(*detail.Results) != 1 {
		t.Fatalf("job=%+v err=%v", detail, err)
	}
	resources, err := store.ListResources(ctx, app.ResourceQuery{InstanceID: instance.ID, Limit: 50})
	if err != nil || len(resources) != 1 {
		t.Fatalf("resources=%+v err=%v", resources, err)
	}
	resource, err := store.GetResource(ctx, job.ResourceID)
	if err != nil || resource.Preview == nil {
		t.Fatalf("resource=%+v err=%v", resource, err)
	}
	datasets, err := store.ListDatasets(ctx, app.ResourceQuery{InstanceID: instance.ID, Limit: 50})
	if err != nil || len(datasets) != 1 || datasets[0].CompletedResources != 1 {
		t.Fatalf("datasets=%+v err=%v", datasets, err)
	}
	stats, err := store.Dashboard(ctx)
	if err != nil {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	foundStats := false
	for _, stat := range stats {
		if stat.Instance.ID == instance.ID {
			foundStats = stat.Completed == 1
		}
	}
	if !foundStats {
		t.Fatalf("integration stats missing: %+v", stats)
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

func TestChildJobsAppearOnlyUnderTheirParentAgainstMySQL(t *testing.T) {
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
	instanceID, parentID, childID := "child-projection-instance", "child-projection-parent", "child-projection-child"
	defer func() {
		store.DB.ExecContext(ctx, `DELETE FROM latest_resource_job WHERE resource_id='child-projection-resource'`)
		store.DB.ExecContext(ctx, `DELETE FROM ckan_data_job WHERE id=?`, childID)
		store.DB.ExecContext(ctx, `DELETE FROM ckan_data_job WHERE id=?`, parentID)
		store.DB.ExecContext(ctx, `DELETE FROM ckan_instance WHERE id=?`, instanceID)
	}()
	if err := store.CreateInstance(ctx, &app.Instance{ID: instanceID, Name: "Child test", URL: "https://data.test", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	version := "2026-09-01T00:00:00Z"
	parent := &app.Job{ID: parentID, ResourceID: "child-projection-resource", DatasetName: "dataset", Status: app.JobProcessing, IdempotencyKey: "parent-key", InstanceID: instanceID, SourceVersion: &version, CreatedAt: now, UpdatedAt: now}
	index, total := 0, 2
	child := &app.Job{ID: childID, ResourceID: "child-projection-file", DatasetName: "dataset", Status: app.JobCompleted, IdempotencyKey: "child-key", InstanceID: instanceID, ParentJobID: &parentID, ParentResourceID: &parent.ResourceID, ChildIndex: &index, TotalChildCount: &total, SourceVersion: parent.SourceVersion, CreatedAt: now, UpdatedAt: now}
	if err := store.InTx(ctx, func(tx app.Tx) error {
		if err := tx.InsertJob(ctx, parent); err != nil {
			return err
		}
		if err := tx.InsertJob(ctx, child); err != nil {
			return err
		}
		return tx.PutLatest(ctx, &app.LatestResource{ResourceID: parent.ResourceID, LatestJobID: parent.ID, InstanceID: instanceID, DatasetName: parent.DatasetName, Status: "processing", CreatedAt: now, UpdatedAt: now})
	}); err != nil {
		t.Fatal(err)
	}
	listed, err := store.ListJobs(ctx, app.JobQuery{InstanceID: instanceID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ID != parentID {
		t.Fatalf("top-level jobs = %#v", listed)
	}
	detail, err := store.GetJob(ctx, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Children) != 1 || detail.Children[0].ID != childID || detail.Children[0].SourceVersion == nil {
		t.Fatalf("parent children = %#v", detail.Children)
	}
}
