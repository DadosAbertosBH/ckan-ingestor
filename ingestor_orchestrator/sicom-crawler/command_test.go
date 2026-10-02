// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sicomcrawler

import (
	"context"
	"fmt"
	sicomidentity "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPreflightRequiresDedicatedSICOMInstance(t *testing.T) {
	for _, source := range []string{"ckan", "sicom"} {
		t.Run(source, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/instances/instance" {
					t.Errorf("path=%s", r.URL.Path)
				}
				fmt.Fprintf(w, `{"id":"instance","url":%q,"source_type":%q}`, sicomidentity.PortalURL, source)
			}))
			defer s.Close()
			err := validateInstance(context.Background(), s.Client(), s.URL, "instance")
			if (source == "sicom") != (err == nil) {
				t.Fatalf("source=%s error=%v", source, err)
			}
		})
	}
}
func TestBrowserStartupReportsMissingBinaryWithoutLeakingArguments(t *testing.T) {
	_, close, e := startBrowser(context.Background(), "/missing/lightpanda", []byte("test-ca"))
	if e == nil {
		close()
		t.Fatal("expected missing executable diagnostic")
	}
}
