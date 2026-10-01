// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package portal

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLiveBrowserAuthenticationAnd2026Listing(t *testing.T) {
	endpoint := os.Getenv("SICOM_LIVE_CDP")
	if endpoint == "" {
		t.Skip("set SICOM_LIVE_CDP for read-only live portal verification")
	}
	client, _, err := HTTPClient(os.Getenv("SICOM_CA_BUNDLE"))
	if err != nil {
		t.Fatal(err)
	}
	b := &Browser{CDPURL: endpoint, HTTP: client, Timeout: 2 * time.Minute}
	c := &Client{HTTP: client, Authenticate: b.Authenticate}
	years, e := c.Years(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("published SICOM years: %v", years)
	categories, e := c.List(context.Background(), 2026)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, category := range categories {
		if strings.EqualFold(category.Name, "Despesas com pessoal") {
			found = true
		}
	}
	if !found {
		t.Fatalf("personnel expenses absent among %d categories", len(categories))
	}
	t.Logf("2026 categories: %d", len(categories))
}
