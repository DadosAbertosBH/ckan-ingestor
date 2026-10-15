// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
// Deprecated: use the mysql-repositories module.
package mysqlstore

import repositories "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories"

type Store = repositories.Store

var Open = repositories.Open
