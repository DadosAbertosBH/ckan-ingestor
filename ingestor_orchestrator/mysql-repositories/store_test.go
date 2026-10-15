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

package mysqlrepositories

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

type interfaceTx = app.Tx

func TestInTxCommitsSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectCommit()
	store := &Store{DB: db}
	called := false
	if err := store.InTx(context.Background(), func(_ interfaceTx) error { called = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("callback was not called")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSICOMInstanceCountsComeFromPublishedResources(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// SICOM does not run CKAN metadata sync, so its counters are derived from jobs.
	mock.ExpectQuery("SELECT .*CASE WHEN source_type='sicom'.*COUNT\\(DISTINCT dataset_name\\)").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "url", "dataset_count", "resource_count", "synced", "created", "updated", "source_type"}).AddRow("sicom", "SICOM", "https://dadosabertos.tce.mg.gov.br/", 13, 853, nil, time.Now(), time.Now(), "sicom"))
	rows, err := (&Store{DB: db}).ListInstances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].SourceType != "sicom" || rows[0].ResourceCount != 853 || rows[0].DatasetCount != 13 {
		t.Fatalf("instances=%+v", rows)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInTxRollsBackError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectRollback()
	want := errors.New("boom")
	store := &Store{DB: db}
	if err := store.InTx(context.Background(), func(_ interfaceTx) error { return want }); !errors.Is(err, want) {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestJobStatePersistsOnlyInAPIJobLedger(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	mock.ExpectExec("UPDATE ckan_data_job SET status").WithArgs(app.JobCompleted, nil, &now, now, nil, "job").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := (&txStore{tx: tx}).UpdateJob(context.Background(), &app.Job{ID: "job", Status: app.JobCompleted, CompletedAt: &now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
