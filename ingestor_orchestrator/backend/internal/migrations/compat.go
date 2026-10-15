// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
// Deprecated: migrations are owned by mysql-repositories.
package migrations

import (
	"context"
	"database/sql"
	m "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories/migrations"
)

func Up(ctx context.Context, db *sql.DB) error { return m.Up(ctx, db) }
