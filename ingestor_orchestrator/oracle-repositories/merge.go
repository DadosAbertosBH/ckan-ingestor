// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import "strings"

// The supported upserts are fixed repository statements, never caller-provided SQL.
func mergeSQL(q string) string {
	parts := strings.SplitN(q, " ON DUPLICATE KEY UPDATE ", 2)
	ignore := strings.HasPrefix(parts[0], "INSERT IGNORE INTO ")
	if len(parts) == 1 && !ignore {
		return q
	}
	base := strings.Replace(parts[0], "INSERT IGNORE INTO ", "INSERT INTO ", 1)
	start := strings.Index(base, " (")
	values := strings.Index(base, ") VALUES (")
	if start < 0 || values < 0 {
		return q
	}
	table := strings.TrimPrefix(base[:start], "INSERT INTO ")
	columns := strings.Split(base[start+2:values], ",")
	rawValues := strings.TrimSuffix(base[values+len(") VALUES ("):], ")")
	expressions := splitExpressions(rawValues)
	if len(expressions) != len(columns) {
		return q
	}
	keys := map[string][]string{"latest_resource_job": {"resource_id"}, "last_terminal_status": {"resource_id"}, "csv_hint": {"resource_id"}, "resource_metadata_label": {"resource_id", "label"}}[table]
	if len(keys) == 0 {
		return q
	}
	selectValues := make([]string, len(columns))
	for i, c := range columns {
		columns[i] = strings.TrimSpace(c)
		selectValues[i] = expressions[i] + " AS " + columns[i]
	}
	conditions := make([]string, len(keys))
	for i, k := range keys {
		conditions[i] = "t." + k + "=s." + k
	}
	result := "MERGE INTO " + table + " t USING (SELECT " + strings.Join(selectValues, ",") + " FROM dual) s ON (" + strings.Join(conditions, " AND ") + ")"
	if len(parts) == 2 {
		assignments := splitExpressions(parts[1])
		for i, a := range assignments {
			pair := strings.SplitN(a, "=", 2)
			rhs := pair[1]
			for _, c := range columns {
				rhs = strings.ReplaceAll(rhs, "VALUES("+c+")", "s."+c)
			}
			assignments[i] = "t." + strings.TrimSpace(pair[0]) + "=" + rhs
		}
		result += " WHEN MATCHED THEN UPDATE SET " + strings.Join(assignments, ",")
	}
	refs := make([]string, len(columns))
	for i, c := range columns {
		refs[i] = "s." + c
	}
	return result + " WHEN NOT MATCHED THEN INSERT (" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(refs, ",") + ")"
}
func splitExpressions(s string) []string {
	var out []string
	depth, start := 0, 0
	quoted := false
	for i, c := range s {
		if c == '\'' {
			quoted = !quoted
		}
		if quoted {
			continue
		}
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(s[start:]))
}
