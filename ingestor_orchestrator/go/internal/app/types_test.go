// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package app

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAPITypesUseSnakeCaseJSON(t *testing.T) {
	encoded, err := json.Marshal(struct {
		Instance Instance      `json:"instance"`
		Result   JobResult     `json:"result"`
		Resource ResourceView  `json:"resource"`
		Dataset  DatasetView   `json:"dataset"`
		Sync     MetadataSync  `json:"sync"`
		Stats    InstanceStats `json:"stats"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	body := string(encoded)
	for _, key := range []string{"resource_id", "dataset_preview", "ckan_resource_url", "total_resources", "start_time", "pending", "outdated_resources"} {
		if !strings.Contains(body, `"`+key+`"`) {
			t.Fatalf("JSON does not contain %q: %s", key, body)
		}
	}
}
