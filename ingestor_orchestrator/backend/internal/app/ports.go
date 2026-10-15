// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package app

import (
	"context"
	repositories "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

type Tx = repositories.Tx
type Store = repositories.Store
type QueryStore = repositories.QueryStore
type Publisher interface {
	Publish(context.Context, string, []byte, string) (Routing, error)
}
