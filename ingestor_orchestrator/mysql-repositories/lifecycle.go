// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package mysqlrepositories

import (
	"context"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories/migrations"
	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

func (s *Store) Close() error                      { return s.DB.Close() }
func (s *Store) Migrate(ctx context.Context) error { return migrations.Up(ctx, s.DB) }

var _ app.Database = (*Store)(nil)
