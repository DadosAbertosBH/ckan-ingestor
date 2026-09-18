// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package migrations

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"strings"
	"testing"
)

func TestCSVHintMigrationContainsAllLegacySeeds(t *testing.T) {
	data, err := files.ReadFile("sql/00002_csv_hints.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)
	if count := strings.Count(sql, "\n('"); count != 4064 {
		t.Fatalf("CSV hint count = %d", count)
	}
	if strings.Contains(sql, "('','')") {
		t.Fatal("migration contains an empty CSV hint")
	}
	if strings.Count(sql, "CHAR(9)") != 1 {
		t.Fatal("migration must contain exactly one tab-delimited hint")
	}
}

func TestValidateAlembicAcceptsCleanAndHeadDatabases(t *testing.T) {
	for _, item := range []struct {
		exists  int
		version string
	}{{0, ""}, {1, "020"}} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("information_schema.tables").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(item.exists))
		if item.exists == 1 {
			mock.ExpectQuery("SELECT version_num").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(item.version))
		}
		if err := validateAlembic(context.Background(), db); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

func TestValidateAlembicRejectsOlderRevision(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("information_schema.tables").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT version_num").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("019"))
	if err := validateAlembic(context.Background(), db); err == nil {
		t.Fatal("expected revision error")
	}
}
