// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed sql/*.sql
var files embed.FS

func Up(ctx context.Context, db *sql.DB) error {
	if err := validateAlembic(ctx, db); err != nil {
		return err
	}
	goose.SetBaseFS(files)
	if err := goose.SetDialect("mysql"); err != nil {
		return err
	}
	return goose.UpContext(ctx, db, "sql")
}

func validateAlembic(ctx context.Context, db *sql.DB) error {
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='alembic_version'`).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return nil
	}
	var version string
	if err := db.QueryRowContext(ctx, `SELECT version_num FROM alembic_version LIMIT 1`).Scan(&version); err != nil {
		return err
	}
	if version != "020" {
		return fmt.Errorf("database Alembic revision is %q; expected 020", version)
	}
	return nil
}
