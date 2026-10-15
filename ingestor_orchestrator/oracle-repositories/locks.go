// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var ErrLockBusy = errors.New("repository lock already held")

func acquireLock(ctx context.Context, db *sql.DB, key string) (func(), error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		conn.Close()
		return nil, err
	}
	var found string
	err = tx.QueryRowContext(ctx, "SELECT lock_key FROM repository_lock WHERE lock_key=:p1 FOR UPDATE NOWAIT", sql.Named("p1", key)).Scan(&found)
	if err != nil {
		tx.Rollback()
		conn.Close()
		if strings.Contains(err.Error(), "ORA-00054") {
			return nil, ErrLockBusy
		}
		return nil, fmt.Errorf("acquire %s: %w", key, err)
	}
	var once sync.Once
	return func() { once.Do(func() { tx.Rollback(); conn.Close() }) }, nil
}
