// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import "testing"

func TestWalletUsesTransactionalService(t *testing.T) {
	tns := []byte("myatp_high=(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=myatp_high.adb.oraclecloud.com)))\nmyatp_tp=(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=myatp_tp.adb.oraclecloud.com)))")
	service, err := walletService(tns)
	if err != nil || service != "myatp_tp.adb.oraclecloud.com" {
		t.Fatalf("service=%s error=%v", service, err)
	}
}
