// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package portal

import (
	"net/http"
	"testing"
)

func TestTrustExtendsSystemRootsAndChecksCertificateValidity(t *testing.T) {
	client, pem, e := HTTPClient("")
	if e != nil {
		t.Fatal(e)
	}
	if len(pem) == 0 {
		t.Fatal("Lightpanda requires system plus Sectigo PEM chain")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil || transport.TLSClientConfig.RootCAs == nil || transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("verified TLS chain required")
	}
	if _, _, e := HTTPClient("/missing/sicom-ca.pem"); e == nil {
		t.Fatal("expected missing CA configuration error")
	}
}
