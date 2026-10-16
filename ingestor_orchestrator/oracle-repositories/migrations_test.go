// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestOracleRetainsAllCSVHints(t *testing.T) {
	var hints [][2]string
	if err := json.Unmarshal(csvHints, &hints); err != nil {
		t.Fatal(err)
	}
	if len(hints) != 4064 {
		t.Fatalf("hints=%d want=4064", len(hints))
	}
}

func TestMigrationSeedsDisableParallelDML(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT NVL").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(0))
	for _, statement := range strings.Split(schema, ";") {
		if strings.TrimSpace(statement) != "" {
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		}
	}
	mock.ExpectBegin()
	for range 3 {
		mock.ExpectExec(`MERGE /\*\+ DISABLE_PARALLEL_DML \*/ INTO ckan_instance`).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec(`MERGE /\*\+ DISABLE_PARALLEL_DML \*/ INTO repository_lock`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	var hints [][2]string
	if err := json.Unmarshal(csvHints, &hints); err != nil {
		t.Fatal(err)
	}
	for start := 0; start < len(hints); start += 200 {
		mock.ExpectBegin()
		for _, hint := range hints[start:min(start+200, len(hints))] {
			mock.ExpectExec(`MERGE /\*\+ DISABLE_PARALLEL_DML \*/ INTO csv_hint`).WithArgs(hint[0], hint[1]).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		mock.ExpectCommit()
	}
	mock.ExpectExec("INSERT INTO repository_schema_version").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := (&Store{DB: db}).Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCSVHintBatchRollsBackOnFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	want := errors.New("seed failed")
	mock.ExpectBegin()
	mock.ExpectExec(`MERGE /\*\+ DISABLE_PARALLEL_DML \*/ INTO csv_hint`).WithArgs("resource", ",").WillReturnError(want)
	mock.ExpectRollback()
	if err := migrateCSVHintBatch(context.Background(), db, [][2]string{{"resource", ","}}); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOracleMigrationCommitsEachCSVHintBatch(t *testing.T) {
	data, err := os.ReadFile("migrations.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "func migrateCSVHintBatch") || !strings.Contains(string(data), "return tx.Commit()") {
		t.Fatal("CSV hint batches must commit independently to avoid Oracle parallel DML transaction errors")
	}
	if !strings.Contains(string(data), "VALUES(s.resource_id,s.delimiter)") {
		t.Fatal("CSV hint merge must use the source delimiter in its INSERT clause")
	}
	if strings.Contains(string(data), "sql.Named(\"ids\"") {
		t.Fatal("CSV hint migration must avoid vectorized Oracle binds")
	}
}
