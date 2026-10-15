// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"testing"
	"time"
)

func TestContainerCertificatePin(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	certificate := func(serial int64, until time.Time) *x509.Certificate {
		template := x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: until, KeyUsage: x509.KeyUsageDigitalSignature}
		der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		c, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	trusted := certificate(1, time.Now().Add(time.Hour))
	if err := verifyPinnedCertificate(trusted.Raw, tls.ConnectionState{PeerCertificates: []*x509.Certificate{trusted}}); err != nil {
		t.Fatal(err)
	}
	other := certificate(2, time.Now().Add(time.Hour))
	if err := verifyPinnedCertificate(trusted.Raw, tls.ConnectionState{PeerCertificates: []*x509.Certificate{other}}); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
	expired := certificate(3, time.Now().Add(-time.Minute))
	if err := verifyPinnedCertificate(expired.Raw, tls.ConnectionState{PeerCertificates: []*x509.Certificate{expired}}); err == nil {
		t.Fatal("expired certificate accepted")
	}
	if err := verifyPinnedCertificate(trusted.Raw, tls.ConnectionState{}); err == nil {
		t.Fatal("missing certificate accepted")
	}
}
