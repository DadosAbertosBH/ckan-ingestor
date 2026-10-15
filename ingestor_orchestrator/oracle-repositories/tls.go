// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// SPDX-License-Identifier: AGPL-3.0-or-later
// Licensed under the GNU Affero General Public License, version 3 or later.
package oraclerepositories

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/sijms/go-ora/v2/configurations"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Autonomous Free creates a CN-only self-signed certificate. Pin its exact
// certificate rather than weakening verification for arbitrary servers.
func verifyPinnedCertificate(pinned []byte, state tls.ConnectionState) error {
	if len(state.PeerCertificates) == 0 || !bytes.Equal(pinned, state.PeerCertificates[0].Raw) {
		return fmt.Errorf("Oracle container certificate does not match trusted pin")
	}
	cert := state.PeerCertificates[0]
	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return fmt.Errorf("Oracle container certificate is outside its validity interval")
	}
	return nil
}

func containerTLS(dsn string) (*tls.Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	walletPath := ""
	for key, values := range u.Query() {
		if strings.EqualFold(key, "WALLET") && len(values) > 0 {
			walletPath = values[0]
		}
	}
	if walletPath == "" {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(walletPath, "adb_container.cert"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("Oracle container certificate is not PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	wallet, err := configurations.NewWallet(filepath.Join(walletPath, "cwallet.sso"))
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error { return verifyPinnedCertificate(certificate.Raw, state) }}
	for _, rawCert := range wallet.Certificates {
		cert, err := x509.ParseCertificate(rawCert)
		if err != nil {
			return nil, err
		}
		for _, rawKey := range wallet.PrivateKeys {
			key, err := x509.ParsePKCS1PrivateKey(rawKey)
			if err != nil {
				return nil, err
			}
			if !key.PublicKey.Equal(cert.PublicKey) {
				continue
			}
			pair, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rawCert}), pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: rawKey}))
			if err != nil {
				return nil, err
			}
			cfg.Certificates = append(cfg.Certificates, pair)
		}
	}
	return cfg, nil
}
