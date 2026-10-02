// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package ckandiscovery

import (
	"context"
	"fmt"
	catalog "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPagesPackagesAndChildrenWithContentVersionFallback(t *testing.T) {
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages++
		if r.URL.Query().Get("offset") == "0" {
			fmt.Fprint(w, `{"success":true,"result":[{"id":"p","name":"p","title":"Title","license_id":"lic","metadata_modified":"v2","resources":[{"id":"r","url":"https://files.test/x.csv","format":"csv","datastore_active":true}]}]}`)
		} else {
			fmt.Fprint(w, `{"success":true,"result":[]}`)
		}
	}))
	defer server.Close()
	var resources []catalog.Resource
	a := Adapter{HTTP: server.Client(), PageSize: 1}
	s := catalog.Source{ID: "source", URL: server.URL}
	err := a.Discover(context.Background(), s, func(r catalog.Resource) error { resources = append(resources, r); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if pages != 2 || len(resources) != 2 {
		t.Fatalf("pages=%d resources=%v", pages, resources)
	}
	if resources[0].ID != "p" || resources[1].ID != "r" || resources[1].ParentID != "p" || resources[1].Version != "v2" || !resources[1].DatastoreActive {
		t.Fatal("resource lost parent/version/access metadata")
	}
}
func TestPartialPageFailurePropagatesAndAPIRejectsFailure(t *testing.T) {
	for _, response := range []string{`{"success":false,"result":[]}`, `{"success":true,"result":{}}`, `broken`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, response) }))
		a := Adapter{HTTP: server.Client()}
		err := a.Discover(context.Background(), catalog.Source{URL: server.URL}, func(catalog.Resource) error { return nil })
		server.Close()
		if err == nil {
			t.Fatalf("accepted %s", response)
		}
	}
}

func TestDiscoveryIdentifiesTheCrawlerWithItsDefaultUserAgent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != FirefoxUserAgent {
			t.Fatalf("User-Agent = %q", got)
		}
		fmt.Fprint(w, `{"success":true,"result":[]}`)
	}))
	defer server.Close()
	a := Adapter{HTTP: server.Client()}
	if err := a.Discover(context.Background(), catalog.Source{URL: server.URL}, func(catalog.Resource) error { return nil }); err != nil {
		t.Fatal(err)
	}
}
