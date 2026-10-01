// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package app

import "testing"

func TestSourceLinksUseTheSICOMPortalAndPreserveCKANPaths(t *testing.T) {
	if got := SourceResourceURL("https://dadosabertos.tce.mg.gov.br/", "SICOM 2026", "id"); got != "https://dadosabertos.tce.mg.gov.br/#/public/detalhar-categoria/jurisdicionado/download" {
		t.Fatalf("SICOM URL=%s", got)
	}
	if got := SourceDatasetURL("https://ckan.test/", "dataset"); got != "https://ckan.test/dataset/dataset" {
		t.Fatalf("CKAN URL=%s", got)
	}
}
