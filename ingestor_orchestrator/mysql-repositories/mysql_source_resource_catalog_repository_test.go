// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package mysqlrepositories

import (
	"context"
	"encoding/json"
	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSnapshotReplayPreparesJobsAndDeletionsWithoutPersistingMessages(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := MySQLSourceResourceCatalogRepository{DB: db, JobsTopic: "jobs", ResultsTopic: "results", SyncResultsTopic: "sync-results"}
	for attempt := 0; attempt < 2; attempt++ {
		m.ExpectBegin()
		m.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"datasets", "resources", "new_datasets", "new_resources", "updated_datasets", "updated_resources"}).AddRow(1, 1, 0, 1, 0, 0))
		m.ExpectQuery("SELECT id,COALESCE").WillReturnRows(sqlmock.NewRows([]string{"id", "parent", "kind", "title", "description", "page", "url", "format", "version", "dataset", "datastore"}).AddRow("r", "p", "file", "title", "", "", "https://file", "CSV", "v1", "dataset", false))
		m.ExpectQuery("SELECT instance_id,resource_id,source_version FROM ckan_data_job WHERE status='completed'").WillReturnRows(sqlmock.NewRows([]string{"source", "resource", "version"}))
		m.ExpectQuery("SELECT id,kind,dataset_name,title.*last_changed_sync").WithArgs("source", "sync", "sync").WillReturnRows(sqlmock.NewRows([]string{"id", "kind", "dataset", "title"}).AddRow("deleted", "file", "dataset", "old"))
		m.ExpectExec("UPDATE source_resource SET deleted_at=.*last_changed_sync").WithArgs("sync", "source", "sync").WillReturnResult(sqlmock.NewResult(0, 1))
		m.ExpectCommit()
		counts, messages, err := repo.Complete(context.Background(), "sync", Source{ID: "source", URL: "https://portal"})
		if err != nil {
			t.Fatal(err)
		}
		if counts.Planned != 1 || counts.DeletedResources != 1 || len(messages) != 5 {
			t.Fatalf("counts=%v messages=%v", counts, messages)
		}
		if messages[1].Topic != "jobs" || messages[4].Topic != "sync-results" {
			t.Fatal("completion must follow all job messages")
		}
		var payload map[string]any
		if err := json.Unmarshal(messages[1].Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["job_id"] != app.JobForResource(Resource{ID: "r", SourceID: "source", Version: "v1"}).JobID {
			t.Fatalf("job identity changed: %v", payload)
		}
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

var _ app.SourceResourceCatalogRepository = (*MySQLSourceResourceCatalogRepository)(nil)

func TestMySQLSourceUsesRegisteredConfiguration(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m.ExpectQuery("SELECT id,name,url,source_type FROM ckan_instance WHERE id").WithArgs("source").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "url", "type"}).AddRow("source", "SICOM", "https://portal.test", "sicom"))
	repo := MySQLSourceResourceCatalogRepository{DB: db}
	s, err := repo.Source(context.Background(), "source")
	if err != nil || s.Type != "sicom" {
		t.Fatalf("source=%v error=%v", s, err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestObserveBatchUsesOneTransactionalMultiRowUpsert(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := MySQLSourceResourceCatalogRepository{DB: db}
	m.ExpectBegin()
	// Metadata changes with the same version preserve the change marker;
	// a new version or a restored resource advances it before fields are assigned.
	changeMarker := regexp.QuoteMeta("last_changed_sync=IF(NOT(BINARY source_version<=>BINARY VALUES(source_version)) OR deleted_at IS NOT NULL,VALUES(last_changed_sync),last_changed_sync),parent_resource_id=VALUES(parent_resource_id)")
	m.ExpectExec(`INSERT INTO source_resource .* VALUES \(.+\),\(.+\) ON DUPLICATE KEY UPDATE ` + changeMarker).WillReturnResult(sqlmock.NewResult(0, 2))
	m.ExpectCommit()
	resources := []Resource{
		{ID: "one", SourceID: "source"},
		{ID: "two", SourceID: "source"},
	}
	if err := repo.ObserveBatch(context.Background(), "sync", resources); err != nil {
		t.Fatal(err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestVersionStatusesReadsOnlyCompletedVersionsFromAPIJobLedger(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	m.ExpectQuery("SELECT instance_id,resource_id,source_version FROM ckan_data_job WHERE status='completed'").WithArgs("source", "one", "v1", "source", "two", "v1").WillReturnRows(sqlmock.NewRows([]string{"instance_id", "resource_id", "source_version"}).AddRow("source", "two", "v1"))
	statuses, err := (&MySQLSourceResourceCatalogRepository{}).versionStatuses(context.Background(), tx, []Resource{{ID: "one", SourceID: "source", Version: "v1", URL: "https://one"}, {ID: "two", SourceID: "source", Version: "v1", URL: "https://two"}})
	if err != nil {
		t.Fatal(err)
	}
	if statuses[app.ResourceID("one", "v1")] != "" || statuses[app.ResourceID("two", "v1")] != "completed" {
		t.Fatalf("statuses=%v", statuses)
	}
	m.ExpectCommit()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
