// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package ckandiscovery

import (
	"context"
	"encoding/json"
	"fmt"
	catalog "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// FirefoxUserAgent matches the protected-CKAN request profile used by the
// established Rust metadata fetcher.
const FirefoxUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15; rv:132.0) Gecko/20100101 Firefox/132.0"

type Adapter struct {
	HTTP     *http.Client
	PageSize int
}

func (a *Adapter) Discover(ctx context.Context, s catalog.Source, emit func(catalog.Resource) error) error {
	client := a.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	limit := a.PageSize
	if limit <= 0 {
		limit = 25
	}
	for offset := 0; ; offset += limit {
		endpoint := strings.TrimRight(s.URL, "/") + "/api/action/current_package_list_with_resources?" + url.Values{"limit": {strconv.Itoa(limit)}, "offset": {strconv.Itoa(offset)}}.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", FirefoxUserAgent)
		response, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("CKAN page offset %d: %w", offset, err)
		}
		var page struct {
			Success bool              `json:"success"`
			Result  []json.RawMessage `json:"result"`
		}
		err = json.NewDecoder(io.LimitReader(response.Body, 64<<20)).Decode(&page)
		response.Body.Close()
		if response.StatusCode != 200 {
			return fmt.Errorf("CKAN page HTTP %d", response.StatusCode)
		}
		if err != nil {
			return fmt.Errorf("CKAN page: %w", err)
		}
		if !page.Success {
			return fmt.Errorf("CKAN API rejected discovery")
		}
		if page.Result == nil {
			return fmt.Errorf("CKAN API result must be an array")
		}
		for _, raw := range page.Result {
			var p struct {
				ID, Name, Title, Notes string
				Modified               string            `json:"metadata_modified"`
				Resources              []json.RawMessage `json:"resources"`
			}
			if err := json.Unmarshal(raw, &p); err != nil {
				return err
			}
			if p.ID == "" {
				return fmt.Errorf("CKAN package identity is required")
			}
			key := p.ID
			pageURL := strings.TrimRight(s.URL, "/") + "/dataset/" + url.PathEscape(p.Name)
			title := p.Title
			if title == "" {
				title = p.Name
			}
			if err := emit(catalog.Resource{ID: key, Kind: "collection", Title: title, Description: p.Notes, PageURL: pageURL, DatasetName: p.Name, Version: p.Modified, Metadata: raw, MetadataCollection: "ckan_packages"}); err != nil {
				return err
			}
			for _, child := range p.Resources {
				var r struct {
					ID, Name, Description, URL, Format string
					Modified                           string `json:"last_modified"`
					MetadataModified                   string `json:"metadata_modified"`
					Datastore                          bool   `json:"datastore_active"`
				}
				if err := json.Unmarshal(child, &r); err != nil {
					return err
				}
				if r.ID == "" {
					return fmt.Errorf("CKAN resource identity is required")
				}
				version := r.Modified
				if version == "" {
					version = r.MetadataModified
				}
				if version == "" {
					version = p.Modified
				}
				kind := "file"
				if r.Datastore {
					kind = "endpoint"
				}
				if err := emit(catalog.Resource{ID: r.ID, ParentID: key, Kind: kind, Title: r.Name, Description: r.Description, PageURL: pageURL + "/resource/" + url.PathEscape(r.ID), URL: r.URL, Format: strings.ToUpper(r.Format), Version: version, DatasetName: p.Name, DatastoreActive: r.Datastore, Metadata: child, MetadataCollection: "ckan_resources"}); err != nil {
					return err
				}
			}
		}
		if len(page.Result) < limit {
			return nil
		}
	}
}
