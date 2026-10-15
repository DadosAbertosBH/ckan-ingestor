// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package config

import (
	"reflect"
	"testing"
)

func TestDefaultProviderIsOracle(t *testing.T) {
	t.Setenv("INGEST_ORCH_DB_PROVIDER", "")
	if got := reflect.ValueOf(Load()).FieldByName("DBProvider"); !got.IsValid() || got.String() != "oracle" {
		t.Fatal("default provider must be oracle")
	}
}
