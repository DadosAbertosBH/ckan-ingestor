// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"fmt"
	"regexp"
)

func walletService(tns []byte) (string, error) {
	match := regexp.MustCompile(`(?i)SERVICE_NAME\s*=\s*([^\s)]*_tp\.adb\.oraclecloud\.com)`).FindSubmatch(tns)
	if len(match) < 2 {
		return "", fmt.Errorf("wallet has no service name")
	}
	return string(match[1]), nil
}
