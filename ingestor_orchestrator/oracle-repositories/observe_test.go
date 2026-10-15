// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"database/sql/driver"
	"github.com/DATA-DOG/go-sqlmock"
	"testing"
)

type arrayConverter struct{}

func (arrayConverter) ConvertValue(v any) (driver.Value, error) { return v, nil }
func TestObservationDetectsTransitionFromEmptyVersion(t *testing.T) {
	db, m, err := sqlmock.New(sqlmock.ValueConverterOption(arrayConverter{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m.ExpectBegin()
	m.ExpectExec(`MERGE INTO source_resource.*DBMS_LOB.GETLENGTH`).WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	repo := OracleSourceResourceCatalogRepository{DB: db}
	if err := repo.ObserveBatch(context.Background(), "sync", []Resource{{ID: "resource", SourceID: "source", Kind: "file", Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	if err = m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
