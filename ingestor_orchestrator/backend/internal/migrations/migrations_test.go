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
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"
	"os"
	"strings"
	"testing"
)

func TestChildJobsMigrationAgainstMySQL(t *testing.T) {
	dsn := os.Getenv("MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("MYSQL_INTEGRATION_DSN is not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := Up(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"parent_job_id", "parent_resource_id", "child_index", "total_child_count", "source_version"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='ckan_data_job' AND column_name=?`, column).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("migration did not create %s", column)
		}
	}
}

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

func TestDefaultCKANInstancesAreSeeded(t *testing.T) {
	data, err := files.ReadFile("sql/00005_default_ckan_instances.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)
	for _, value := range []string{
		"INSERT IGNORE INTO ckan_instance",
		"PBH",
		"https://dados.pbh.gov.br",
		"Minas Gerais",
		"https://dados.mg.gov.br/",
	} {
		if !strings.Contains(sql, value) {
			t.Fatalf("default instance migration is missing %q", value)
		}
	}
}

func TestSICOMMigrationPreservesCKANDefaultAndSeedsDedicatedSource(t *testing.T) {
	data, err := files.ReadFile("sql/00006_sicom_source.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"DEFAULT 'ckan'", "TCE-MG SICOM", "https://dadosabertos.tce.mg.gov.br/", "'sicom'", "-- +goose Up", "-- +goose Down"} {
		if !strings.Contains(string(data), required) {
			t.Fatalf("migration missing %q", required)
		}
	}
}

func TestSICOMMigrationExecutesThroughGoose(t *testing.T) {
	goose.SetBaseFS(files)
	if err := goose.SetDialect("mysql"); err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec("ALTER TABLE ckan_instance ADD COLUMN source_type").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT IGNORE INTO ckan_instance").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO goose_db_version").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	migration := &goose.Migration{Version: 6, Source: "sql/00006_sicom_source.sql"}
	if err := migration.UpContext(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChildJobsMigrationHasGooseUpAndExpectedColumns(t *testing.T) {
	data, err := files.ReadFile("sql/00004_child_jobs.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)
	if !strings.Contains(sql, "-- +goose Up") {
		t.Fatal("child jobs migration must declare its Goose Up section")
	}
	for _, column := range []string{"parent_job_id", "parent_resource_id", "child_index", "total_child_count", "source_version"} {
		if !strings.Contains(sql, column) {
			t.Fatalf("missing %s column", column)
		}
	}
}

func TestChildJobsMigrationExecutesThroughGoose(t *testing.T) {
	goose.SetBaseFS(files)
	if err := goose.SetDialect("mysql"); err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec("ALTER TABLE ckan_data_job").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO goose_db_version").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	migration := &goose.Migration{Version: 4, Source: "sql/00004_child_jobs.sql"}
	if err := migration.UpContext(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
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
