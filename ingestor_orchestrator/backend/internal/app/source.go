// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package app

import repositories "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"

func SourceDatasetURL(base, dataset string) string {
	return repositories.SourceDatasetURL(base, dataset)
}
func SourceResourceURL(base, dataset, resource string) string {
	return repositories.SourceResourceURL(base, dataset, resource)
}
