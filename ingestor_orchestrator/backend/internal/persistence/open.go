// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package persistence

import (
	"fmt"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
	mysql "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories"
	oracle "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/oracle-repositories"
	r "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

func Open(cfg config.Config) (r.Database, error) {
	switch cfg.DBProvider {
	case "mysql":
		return mysql.Open(cfg.MySQLDSN)
	case "oracle":
		if cfg.OracleDSN == "" {
			return nil, fmt.Errorf("INGEST_ORCH_ORACLE_DSN is required")
		}
		return oracle.Open(cfg.OracleDSN)
	default:
		return nil, fmt.Errorf("unknown database provider %q", cfg.DBProvider)
	}
}
