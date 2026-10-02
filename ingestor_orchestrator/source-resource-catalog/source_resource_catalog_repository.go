// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package resourcecatalog

import "context"

// SourceResourceCatalogRepository persists snapshots and prepares messages for publication.
type SourceResourceCatalogRepository interface {
	Source(context.Context, string) (Source, error)
	// Lock serializes syncs for one source and returns its release function.
	Lock(context.Context, string) (func(), error)
	ObserveBatch(context.Context, string, []Resource) error
	Complete(context.Context, string, Source) (Counts, []Outgoing, error)
}
