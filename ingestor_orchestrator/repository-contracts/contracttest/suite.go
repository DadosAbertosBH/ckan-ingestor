// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
// Package contracttest provides driver-independent integration assertions.
package contracttest

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	r "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
	"strings"
	"testing"
	"time"
)

func Run(t *testing.T, db r.Database, catalog r.SourceResourceCatalogRepository) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	id := uuid.NewString()
	source := r.Instance{ID: id, Name: "contract-" + id, URL: "https://contract.test", SourceType: "ckan", CreatedAt: now, UpdatedAt: now}
	if err := db.CreateInstance(ctx, &source); err != nil {
		t.Fatal(err)
	}
	t.Run("instance_and_missing", func(t *testing.T) {
		got, err := db.GetInstance(ctx, id)
		if err != nil || got.ID != id || got.SourceType != "ckan" {
			t.Fatalf("instance=%+v error=%v", got, err)
		}
		if _, err = db.GetInstance(ctx, uuid.NewString()); !errors.Is(err, r.ErrNotFound) {
			t.Fatalf("missing=%v", err)
		}
		found, err := catalog.Source(ctx, id)
		if err != nil || found.ID != id {
			t.Fatalf("source=%v error=%v", found, err)
		}
	})
	job := r.Job{ID: uuid.NewString(), ResourceID: uuid.NewString(), DatasetName: "Dataset α", InstanceID: id, CKANURL: source.URL, Status: r.JobCompleted, IdempotencyKey: uuid.NewString(), CreatedAt: now, UpdatedAt: now, CompletedAt: &now}
	t.Run("transaction_rollback", func(t *testing.T) {
		sentinel := errors.New("rollback")
		err := db.InTx(ctx, func(tx r.Tx) error {
			if err := tx.InsertJob(ctx, &job); err != nil {
				return err
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
		if _, err := db.GetJob(ctx, job.ID); !errors.Is(err, r.ErrNotFound) {
			t.Fatalf("rolled back job: %v", err)
		}
	})
	t.Run("ledger_and_queries", func(t *testing.T) {
		version := "v1"
		job.SourceVersion = &version
		if err := db.InTx(ctx, func(tx r.Tx) error {
			if err := tx.InsertJob(ctx, &job); err != nil {
				return err
			}
			if err := tx.InsertResult(ctx, &r.JobResult{ID: uuid.NewString(), JobID: job.ID, Success: true, Status: r.JobCompleted, DatasetPreview: json.RawMessage(`[{"id":1}]`), CreatedAt: now}); err != nil {
				return err
			}
			if err := tx.PutLatest(ctx, &r.LatestResource{ResourceID: job.ResourceID, LatestJobID: job.ID, InstanceID: id, DatasetName: job.DatasetName, Status: "completed", CreatedAt: now, UpdatedAt: now}); err != nil {
				return err
			}
			if err := tx.AddLabel(ctx, job.ResourceID, "verified"); err != nil {
				return err
			}
			return tx.PutCSVHint(ctx, job.ResourceID, ";")
		}); err != nil {
			t.Fatal(err)
		}
		jobs, err := db.ListJobs(ctx, r.JobQuery{InstanceID: id, Tags: "verified", Limit: 10})
		if err != nil || len(jobs) != 1 {
			t.Fatalf("jobs=%v error=%v", jobs, err)
		}
		detail, err := db.GetJob(ctx, job.ID)
		if err != nil || detail.Results == nil || len(*detail.Results) != 1 {
			t.Fatalf("detail=%v error=%v", detail, err)
		}
		resources, err := db.ListResources(ctx, r.ResourceQuery{InstanceID: id, Search: "α", Limit: 10})
		if err != nil || len(resources) != 1 {
			t.Fatalf("resources=%v error=%v", resources, err)
		}
		if _, err := db.GetResource(ctx, job.ResourceID); err != nil {
			t.Fatal(err)
		}
		datasets, err := db.ListDatasets(ctx, r.ResourceQuery{InstanceID: id, Limit: 10})
		if err != nil || len(datasets) != 1 || datasets[0].CompletedResources != 1 {
			t.Fatalf("datasets=%v error=%v", datasets, err)
		}
		if _, err := db.Dashboard(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("exclusive_locks", func(t *testing.T) {
		release, err := catalog.Lock(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		other, err := catalog.Lock(ctx, id)
		if err == nil {
			other()
			release()
			t.Fatal("second source lock acquired")
		}
		release()
		release, err = catalog.Lock(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		release()
		nested := false
		if err := db.WithSchedulerLock(ctx, func() error { return db.WithSchedulerLock(ctx, func() error { nested = true; return nil }) }); err != nil || nested {
			t.Fatalf("scheduler nested=%v error=%v", nested, err)
		}
	})
	t.Run("catalog_replay_change_delete_restore", func(t *testing.T) {
		root := r.Resource{ID: uuid.NewString(), SourceID: id, Kind: "collection", Title: "Dataset α", DatasetName: "Dataset α"}
		file := r.Resource{ID: uuid.NewString(), SourceID: id, ParentID: root.ID, Kind: "file", Title: "a.csv", Description: strings.Repeat("á", 24000), URL: "https://contract.test/a.csv", Format: "CSV", Version: "v1", DatasetName: "Dataset α"}
		source := r.Source{ID: id, Name: source.Name, URL: source.URL, Type: "ckan"}
		first := uuid.NewString()
		if err := catalog.ObserveBatch(ctx, first, []r.Resource{root, file}); err != nil {
			t.Fatal(err)
		}
		counts, messages, err := catalog.Complete(ctx, first, source)
		if err != nil || counts.ResourceCount != 1 || counts.NewResources != 1 || counts.Planned != 1 {
			t.Fatalf("counts=%v messages=%d error=%v", counts, len(messages), err)
		}
		replay := uuid.NewString()
		if err := catalog.ObserveBatch(ctx, replay, []r.Resource{root, file}); err != nil {
			t.Fatal(err)
		}
		counts, _, err = catalog.Complete(ctx, replay, source)
		if err != nil || counts.NewResources != 0 || counts.UpdatedResources != 0 {
			t.Fatalf("replay=%v error=%v", counts, err)
		}
		file.Version = "v2"
		changed := uuid.NewString()
		if err := catalog.ObserveBatch(ctx, changed, []r.Resource{root, file}); err != nil {
			t.Fatal(err)
		}
		counts, _, err = catalog.Complete(ctx, changed, source)
		if err != nil || counts.UpdatedResources != 1 {
			t.Fatalf("change=%v error=%v", counts, err)
		}
		missing := uuid.NewString()
		if err := catalog.ObserveBatch(ctx, missing, []r.Resource{root}); err != nil {
			t.Fatal(err)
		}
		counts, _, err = catalog.Complete(ctx, missing, source)
		if err != nil || counts.DeletedResources != 1 {
			t.Fatalf("deletion=%v error=%v", counts, err)
		}
		counts, _, err = catalog.Complete(ctx, missing, source)
		if err != nil || counts.DeletedResources != 1 {
			t.Fatalf("deletion replay=%v error=%v", counts, err)
		}
		restored := uuid.NewString()
		if err := catalog.ObserveBatch(ctx, restored, []r.Resource{root, file}); err != nil {
			t.Fatal(err)
		}
		counts, _, err = catalog.Complete(ctx, restored, source)
		if err != nil || counts.UpdatedResources != 1 {
			t.Fatalf("restore=%v error=%v", counts, err)
		}
	})
}
