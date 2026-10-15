// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package mysqlrepositories

import (
	"context"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts/contracttest"
	"os"
	"testing"
)

func TestMySQLRepositoryContracts(t *testing.T) {
	dsn := os.Getenv("MYSQL_INTEGRATION_DSN")
	if dsn == "" {
		t.Skip("set MYSQL_INTEGRATION_DSN for real MySQL contracts")
	}
	store, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	contracttest.Run(t, store, &MySQLSourceResourceCatalogRepository{DB: store.DB, JobsTopic: "jobs", ResultsTopic: "results", SyncResultsTopic: "sync-results"})
}
