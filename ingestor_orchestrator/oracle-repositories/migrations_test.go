// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"encoding/json"
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
