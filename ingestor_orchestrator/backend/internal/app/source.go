// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package app

import (
	"strings"

	sicomidentity "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity"
)

func SourceDatasetURL(base, dataset string) string {
	if base == "" {
		return ""
	}
	if sicomidentity.IsPortalURL(base) {
		return sicomidentity.DownloadPageURL
	}
	return strings.TrimRight(base, "/") + "/dataset/" + dataset
}
func SourceResourceURL(base, dataset, resource string) string {
	datasetURL := SourceDatasetURL(base, dataset)
	if base == "" || strings.Contains(datasetURL, "/#/") {
		return datasetURL
	}
	return datasetURL + "/resource/" + resource
}
