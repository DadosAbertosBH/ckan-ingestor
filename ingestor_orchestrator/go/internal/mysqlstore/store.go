// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/go/internal/app"
)

type Store struct{ DB *sql.DB }
type txStore struct{ tx *sql.Tx }

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	return &Store{DB: db}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.DB.PingContext(ctx) }

func (s *Store) InTx(ctx context.Context, fn func(app.Tx) error) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(&txStore{tx: tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Store) Instances(ctx context.Context) ([]app.Instance, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id,name,url,dataset_count,resource_count,last_metadata_synced FROM ckan_instance ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []app.Instance
	for rows.Next() {
		var value app.Instance
		var synced sql.NullTime
		if err := rows.Scan(&value.ID, &value.Name, &value.URL, &value.DatasetCount, &value.ResourceCount, &synced); err != nil {
			return nil, err
		}
		value.LastMetadataSynced = nullTime(synced)
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Store) WithSchedulerLock(ctx context.Context, fn func() error) error {
	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK('ckan-ingestor-metadata-scheduler', 0)`).Scan(&acquired); err != nil {
		return err
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return nil
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT RELEASE_LOCK('ckan-ingestor-metadata-scheduler')`)
	}()
	return fn()
}

func translateNotFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return app.ErrNotFound
	}
	return err
}
func nullString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}
func nullTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	return &value.Time
}
func nullInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	return &value.Int64
}
