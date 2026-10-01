// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package portal

import (
	"crypto/tls"
	"crypto/x509"
	"embed"
	"errors"
	"net/http"
	"os"
	"time"
)

//go:embed certificates/sectigo-chain.pem
var certificates embed.FS

func HTTPClient(extraCA string) (*http.Client, []byte, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, nil, err
	}
	// --ca-cert replaces Lightpanda's default store, so explicitly include the system PEM.
	var bundle []byte
	for _, name := range []string{"/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/cert.pem"} {
		if b, e := os.ReadFile(name); e == nil {
			bundle = b
			break
		}
	}
	if len(bundle) == 0 {
		return nil, nil, errors.New("system CA PEM bundle unavailable")
	}
	chain, err := certificates.ReadFile("certificates/sectigo-chain.pem")
	if err != nil {
		return nil, nil, err
	}
	if extraCA != "" {
		extra, e := os.ReadFile(extraCA)
		if e != nil {
			return nil, nil, errors.New("read SICOM_CA_BUNDLE failed")
		}
		if !roots.AppendCertsFromPEM(extra) {
			return nil, nil, errors.New("invalid SICOM_CA_BUNDLE")
		}
		chain = append(append(chain, '\n'), extra...)
	}
	if !roots.AppendCertsFromPEM(chain) {
		return nil, nil, errors.New("invalid embedded Sectigo certificate chain")
	}
	bundle = append(append(bundle, '\n'), chain...)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	transport.ResponseHeaderTimeout = 60 * time.Second
	return &http.Client{Transport: transport, Timeout: 30 * time.Minute}, bundle, nil
}
