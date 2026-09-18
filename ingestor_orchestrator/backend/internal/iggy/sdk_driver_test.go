// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package iggy

import (
	"os"
	"testing"

	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend/internal/config"
)

func TestSDKDriverAgainstIggy(t *testing.T) {
	address := os.Getenv("IGGY_INTEGRATION_ADDRESS")
	if address == "" {
		t.Skip("set IGGY_INTEGRATION_ADDRESS to run against Iggy")
	}
	driver, err := NewSDKDriver(config.Config{IggyAddress: address, IggyUsername: "iggy", IggyPassword: "iggy", Stream: "ckan-ingestor"})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	if err := driver.EnsureStream("ckan-ingestor"); err != nil {
		t.Fatalf("stream: %v", err)
	}
	for _, topic := range []string{"jobs", "jobs-retry", "job-results", "ckan_metadata_sync", "ckan_metadata_sync_result"} {
		if err := driver.EnsureTopic(topic, 1); err != nil {
			t.Fatalf("topic %s: %v", topic, err)
		}
	}
}
