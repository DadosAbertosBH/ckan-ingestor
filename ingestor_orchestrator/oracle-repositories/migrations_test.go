// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
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
