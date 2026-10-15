// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	r "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
	"testing"
	"time"
)

func TestCreateInstanceSeedsLockInSameTransaction(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m.ExpectBegin()
	m.ExpectExec("INSERT INTO ckan_instance").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec("INSERT INTO repository_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	now := time.Now()
	err = (&Store{DB: db}).CreateInstance(context.Background(), &r.Instance{ID: "source", Name: "Portal", URL: "https://test", CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
