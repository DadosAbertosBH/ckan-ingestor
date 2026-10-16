// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
	"strings"
)

//go:embed schema.sql
var schema string

//go:embed csv_hints.json
var csvHints []byte

func (s *Store) Migrate(ctx context.Context) error {
	// Oracle DDL commits implicitly. Each object is checked independently so a
	// partially applied initial migration resumes on the next invocation.
	if err := ensureObject(ctx, s.DB, "CREATE TABLE repository_schema_version (version_number NUMBER PRIMARY KEY)"); err != nil {
		return err
	}
	var version int
	if err := s.DB.QueryRowContext(ctx, "SELECT NVL(MAX(version_number),0) FROM repository_schema_version").Scan(&version); err != nil {
		return err
	}
	if version > 1 {
		return fmt.Errorf("unsupported Oracle schema version %d", version)
	}
	if version == 1 {
		return nil
	}
	for _, q := range strings.Split(schema, ";") {
		if strings.TrimSpace(q) == "" {
			continue
		}
		if err := ensureObject(ctx, s.DB, strings.TrimSpace(q)); err != nil {
			return err
		}
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, source := range []struct{ id, name, url, kind string }{
		{app.ResourceID("seed", "pbh"), "PBH", "https://dados.pbh.gov.br", "ckan"},
		{app.ResourceID("seed", "mg"), "Minas Gerais", "https://dados.mg.gov.br/", "ckan"},
		{"4be9dd37-1f28-540d-a7c3-104e4f5a54e8", "TCE-MG SICOM", "https://dadosabertos.tce.mg.gov.br/", "sicom"},
	} {
		_, err = tx.ExecContext(ctx, "MERGE /*+ DISABLE_PARALLEL_DML */ INTO ckan_instance t USING (SELECT :id id,:name name,:url url,:kind source_type FROM dual) s ON(t.name=s.name) WHEN NOT MATCHED THEN INSERT(id,name,url,source_type,created_at,updated_at) VALUES(s.id,s.name,s.url,s.source_type,SYS_EXTRACT_UTC(SYSTIMESTAMP),SYS_EXTRACT_UTC(SYSTIMESTAMP))", sql.Named("id", source.id), sql.Named("name", source.name), sql.Named("url", source.url), sql.Named("kind", source.kind))
		if err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "MERGE /*+ DISABLE_PARALLEL_DML */ INTO repository_lock t USING (SELECT 'source:'||id lock_key FROM ckan_instance UNION ALL SELECT 'scheduler' FROM dual) s ON(t.lock_key=s.lock_key) WHEN NOT MATCHED THEN INSERT(lock_key) VALUES(s.lock_key)"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	var hints [][2]string
	if err = json.Unmarshal(csvHints, &hints); err != nil {
		return err
	}
	for start := 0; start < len(hints); start += 200 {
		if err = migrateCSVHintBatch(ctx, s.DB, hints[start:min(start+200, len(hints))]); err != nil {
			return err
		}
	}
	if _, err = s.DB.ExecContext(ctx, "INSERT INTO repository_schema_version(version_number) VALUES(1)"); err != nil {
		return err
	}
	return nil
}

func migrateCSVHintBatch(ctx context.Context, db *sql.DB, batch [][2]string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, hint := range batch {
		if _, err = tx.ExecContext(ctx, "MERGE /*+ DISABLE_PARALLEL_DML */ INTO csv_hint t USING(SELECT :resource_id resource_id,:delimiter delimiter FROM dual) s ON(t.resource_id=s.resource_id) WHEN MATCHED THEN UPDATE SET t.delimiter=s.delimiter WHEN NOT MATCHED THEN INSERT(resource_id,delimiter) VALUES(s.resource_id,s.delimiter)", sql.Named("resource_id", hint[0]), sql.Named("delimiter", hint[1])); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func ensureObject(ctx context.Context, db *sql.DB, q string) error {
	fields := strings.Fields(q)
	kind, name := "TABLE", fields[2]
	if fields[1] == "UNIQUE" {
		kind, name = "INDEX", fields[3]
	} else if fields[1] == "INDEX" {
		kind = "INDEX"
	}
	catalog := "USER_TABLES"
	column := "TABLE_NAME"
	if kind == "INDEX" {
		catalog, column = "USER_INDEXES", "INDEX_NAME"
	}
	var exists int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+catalog+" WHERE "+column+"=:name", sql.Named("name", strings.ToUpper(name))).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	_, err := db.ExecContext(ctx, q)
	if err != nil {
		return fmt.Errorf("create %s %s: %w", kind, name, err)
	}
	return nil
}
