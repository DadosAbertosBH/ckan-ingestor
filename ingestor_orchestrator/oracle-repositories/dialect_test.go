// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"database/sql"
	"testing"

	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

func TestOracleBindsAndPagination(t *testing.T) {
	q := oracleSQL("SELECT '?' FROM ckan_data_job WHERE id=? LIMIT ? OFFSET ?")
	want := "SELECT '?' FROM ckan_data_job WHERE id=:p1 OFFSET :p3 ROWS FETCH NEXT :p2 ROWS ONLY"
	if q != want {
		t.Fatalf("query=%s want=%s", q, want)
	}
}
func TestOracleBooleanAggregation(t *testing.T) {
	q := oracleSQL("SELECT SUM(status='pending'),UTC_TIMESTAMP() FROM latest_resource_job")
	want := "SELECT SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),SYS_EXTRACT_UTC(SYSTIMESTAMP) FROM latest_resource_job"
	if q != want {
		t.Fatalf("query=%s want=%s", q, want)
	}
}

func TestOracleBindsJobStatusAsString(t *testing.T) {
	bound := bindArgs([]any{app.JobCompleted})
	arg, ok := bound[0].(sql.NamedArg)
	if !ok {
		t.Fatalf("argument=%T", bound[0])
	}
	if value, ok := arg.Value.(string); !ok || value != "completed" {
		t.Fatalf("value=%T(%v)", arg.Value, arg.Value)
	}
}
