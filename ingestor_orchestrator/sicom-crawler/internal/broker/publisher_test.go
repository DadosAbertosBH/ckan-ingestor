// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler/internal/crawler"
	sicomidentity "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity"
	"testing"
)

func TestPendingConfirmedBeforeWorkerJob(t *testing.T) {
	topics := []string{}
	payloads := []map[string]any{}
	p := &Publisher{JobsTopic: "jobs", ResultsTopic: "job-results", Send: func(_ context.Context, topic, key string, b []byte) error {
		if key != "resource" {
			t.Fatalf("routing key=%s", key)
		}
		var v map[string]any
		if e := json.Unmarshal(b, &v); e != nil {
			t.Fatal(e)
		}
		topics = append(topics, topic)
		payloads = append(payloads, v)
		return nil
	}}
	job := crawler.Job{JobID: "job", ResourceID: "resource", DatasetName: "SICOM 2026", InstanceID: "sicom", CKANURL: sicomidentity.PortalURL, ResourceURL: "https://files.test/file.csv", ResourceFormat: "CSV", SourceVersion: "109822"}
	if e := p.Publish(context.Background(), job); e != nil {
		t.Fatal(e)
	}
	if len(topics) != 2 || topics[0] != "job-results" || topics[1] != "jobs" {
		t.Fatalf("topics=%v", topics)
	}
	if payloads[0]["status"] != "PENDING" || payloads[1]["datastore_active"] != false || payloads[1]["source_version"] != "109822" {
		t.Fatalf("payloads=%v", payloads)
	}
	for _, k := range []string{"job_id", "resource_id", "dataset_name", "instance_id", "resource_url", "resource_format", "ckan_url"} {
		if payloads[0][k] != payloads[1][k] {
			t.Fatalf("metadata mismatch: %s", k)
		}
	}
}
func TestPendingFailureStopsPublication(t *testing.T) {
	calls := 0
	p := &Publisher{Send: func(context.Context, string, string, []byte) error { calls++; return errors.New("no confirmation") }}
	if e := p.Publish(context.Background(), crawler.Job{}); e == nil {
		t.Fatal("expected broker error")
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
}
