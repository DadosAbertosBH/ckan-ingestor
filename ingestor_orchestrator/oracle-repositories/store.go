// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"database/sql"
	"errors"
	go_ora "github.com/sijms/go-ora/v2"
	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
	"time"
)

type Store struct{ DB *sql.DB }
type txStore struct{ tx *sql.Tx }

func Open(dsn string) (*Store, error) {
	tlsConfig, err := containerTLS(dsn)
	if err != nil {
		return nil, err
	}
	connector := go_ora.NewConnector(dsn).(*go_ora.OracleConnector)
	if tlsConfig != nil {
		connector.WithTLSConfig(tlsConfig)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)
	return &Store{DB: db}, nil
}
func (s *Store) Close() error                   { return s.DB.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.DB.PingContext(ctx) }
func (s *Store) InTx(ctx context.Context, fn func(app.Tx) error) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(&txStore{tx: tx}); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) Instances(ctx context.Context) ([]app.Instance, error) { return s.ListInstances(ctx) }
func (s *Store) WithSchedulerLock(ctx context.Context, fn func() error) error {
	release, err := acquireLock(ctx, s.DB, "scheduler")
	if errors.Is(err, ErrLockBusy) {
		return nil
	}
	if err != nil {
		return err
	}
	defer release()
	return fn()
}
func translateNotFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return app.ErrNotFound
	}
	return err
}
func nullString(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	return &v.String
}
func nullTime(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	return &v.Time
}
func nullInt64(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	return &v.Int64
}

var _ app.Database = (*Store)(nil)
