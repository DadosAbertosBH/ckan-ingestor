// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"database/sql"
	"fmt"
	go_ora "github.com/sijms/go-ora/v2"
	"strings"
)

func (r *OracleSourceResourceCatalogRepository) ObserveBatch(ctx context.Context, syncID string, values []Resource) error {
	if len(values) == 0 {
		return nil
	}
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	columns := []string{"id", "source_id", "parent_resource_id", "kind", "title", "description", "page_url", "content_url", "format", "source_version", "dataset_name", "datastore_active", "first_seen_sync", "last_changed_sync", "last_seen_sync"}
	selects := make([]string, len(columns))
	for i, c := range columns {
		selects[i] = fmt.Sprintf(":p%d AS %s", i+1, c)
	}
	query := "MERGE INTO source_resource t USING (SELECT " + strings.Join(selects, ",") + " FROM dual) s ON (t.source_id=s.source_id AND t.id=s.id) WHEN MATCHED THEN UPDATE SET t.last_changed_sync=CASE WHEN NVL(DBMS_LOB.GETLENGTH(t.source_version),0)<>NVL(DBMS_LOB.GETLENGTH(s.source_version),0) OR NVL(DBMS_LOB.COMPARE(t.source_version,s.source_version),0)<>0 OR t.deleted_at IS NOT NULL THEN s.last_changed_sync ELSE t.last_changed_sync END"
	for _, c := range columns[2:12] {
		query += ",t." + c + "=s." + c
	}
	query += ",t.last_seen_sync=s.last_seen_sync,t.deleted_at=NULL,t.updated_at=SYS_EXTRACT_UTC(SYSTIMESTAMP) WHEN NOT MATCHED THEN INSERT (" + strings.Join(columns, ",") + ",created_at,updated_at) VALUES ("
	refs := make([]string, len(columns))
	for i, c := range columns {
		refs[i] = "s." + c
	}
	query += strings.Join(refs, ",") + ",SYS_EXTRACT_UTC(SYSTIMESTAMP),SYS_EXTRACT_UTC(SYSTIMESTAMP))"
	for start := 0; start < len(values); start += 200 {
		batch := values[start:min(start+200, len(values))]
		stringsByColumn := make([][]string, len(columns))
		for i := range stringsByColumn {
			stringsByColumn[i] = make([]string, len(batch))
		}
		bools := make([]int, len(batch))
		for i, v := range batch {
			row := []string{v.ID, v.SourceID, v.ParentID, v.Kind, v.Title, v.Description, v.PageURL, v.URL, v.Format, v.Version, v.DatasetName, "", syncID, syncID, syncID}
			for j, s := range row {
				stringsByColumn[j][i] = s
			}
			if v.DatastoreActive {
				bools[i] = 1
			}
		}
		args := make([]any, len(columns))
		for j := range columns {
			var val any = stringsByColumn[j]
			if j == 11 {
				val = bools
			} else if j >= 4 && j <= 7 || j == 9 || j == 10 {
				lobs := make([]go_ora.Clob, len(batch))
				for i, s := range stringsByColumn[j] {
					lobs[i] = go_ora.Clob{String: s, Valid: true}
				}
				val = lobs
			}
			args[j] = sql.Named(fmt.Sprintf("p%d", j+1), val)
		}
		if _, err = tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}
