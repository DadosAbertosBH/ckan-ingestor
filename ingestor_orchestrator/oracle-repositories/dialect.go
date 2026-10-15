// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	go_ora "github.com/sijms/go-ora/v2"
	app "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts"
)

var pagination = regexp.MustCompile(`LIMIT (:p[0-9]+) OFFSET (:p[0-9]+)`)
var sums = regexp.MustCompile(`SUM\(([^()]*(?:=|<>)[^()]*)\)`)

// Adapt the repository's fixed SQL statements, keeping bound values separate.
func oracleSQL(query string) string {
	query = mergeSQL(query)
	query = strings.ReplaceAll(query, "UTC_TIMESTAMP()", "SYS_EXTRACT_UTC(SYSTIMESTAMP)")
	query = sums.ReplaceAllString(query, "SUM(CASE WHEN $1 THEN 1 ELSE 0 END)")
	query = strings.ReplaceAll(query, "SUM(EXISTS(", "SUM(CASE WHEN EXISTS(")
	query = strings.ReplaceAll(query, "AND e.label='empty')),0)", "AND e.label='empty') THEN 1 ELSE 0 END),0)")
	if strings.Contains(query, "TIMESTAMPDIFF(MICROSECOND") {
		interval := "(CAST(COALESCE(j.completed_at,SYS_EXTRACT_UTC(SYSTIMESTAMP)) AS TIMESTAMP)-CAST(j.started_at AS TIMESTAMP))"
		duration := "(EXTRACT(DAY FROM " + interval + ")*86400000000+EXTRACT(HOUR FROM " + interval + ")*3600000000+EXTRACT(MINUTE FROM " + interval + ")*60000000+EXTRACT(SECOND FROM " + interval + ")*1000000)"
		query = strings.ReplaceAll(query, "TIMESTAMPDIFF(MICROSECOND,j.started_at,COALESCE(j.completed_at,SYS_EXTRACT_UTC(SYSTIMESTAMP)))", duration)
	}
	var b strings.Builder
	quoted, bind := false, 0
	for i := 0; i < len(query); i++ {
		c := query[i]
		if c == '\'' {
			if quoted && i+1 < len(query) && query[i+1] == '\'' {
				b.WriteString("''")
				i++
				continue
			}
			quoted = !quoted
		}
		if c == '?' && !quoted {
			bind++
			fmt.Fprintf(&b, ":p%d", bind)
		} else {
			b.WriteByte(c)
		}
	}
	return pagination.ReplaceAllString(b.String(), "OFFSET $2 ROWS FETCH NEXT $1 ROWS ONLY")
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func bindArgs(args []any) []any {
	bound := make([]any, len(args))
	for i, v := range args {
		if status, ok := v.(app.JobStatus); ok {
			v = string(status)
		}
		if boolean, ok := v.(bool); ok {
			if boolean {
				v = 1
			} else {
				v = 0
			}
		}
		if text, ok := v.(string); ok && len(text) > 4000 {
			v = go_ora.Clob{String: text, Valid: true}
		}
		bound[i] = sql.Named(fmt.Sprintf("p%d", i+1), v)
	}
	return bound
}
func queryContext(ctx context.Context, db executor, q string, args ...any) (*sql.Rows, error) {
	return db.QueryContext(ctx, oracleSQL(q), bindArgs(args)...)
}
func queryRowContext(ctx context.Context, db executor, q string, args ...any) *sql.Row {
	return db.QueryRowContext(ctx, oracleSQL(q), bindArgs(args)...)
}
func execContext(ctx context.Context, db executor, q string, args ...any) (sql.Result, error) {
	return db.ExecContext(ctx, oracleSQL(q), bindArgs(args)...)
}
