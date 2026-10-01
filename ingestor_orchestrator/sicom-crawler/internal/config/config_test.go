// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package config

import "testing"

func TestDefaultsMatchExistingQueueAndStorage(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "account.r2.cloudflarestorage.com")
	t.Setenv("S3_BUCKET", "public-datasets")
	t.Setenv("S3_USE_SSL", "true")
	t.Setenv("S3_URL_STYLE", "path")
	cfg, e := Load()
	if e != nil {
		t.Fatal(e)
	}
	if cfg.Endpoint != "https://account.r2.cloudflarestorage.com" || cfg.PublicURL != "https://account.r2.cloudflarestorage.com/public-datasets" || !cfg.PathStyle || cfg.Region != "auto" {
		t.Fatalf("storage=%+v", cfg)
	}
	if cfg.Stream != "ckan-ingestor" || cfg.JobsTopic != "jobs" || cfg.ResultsTopic != "job-results" || cfg.ZIPBytes != 8<<30 || cfg.Depth != 4 {
		t.Fatalf("defaults=%+v", cfg)
	}
}
func TestInvalidLimitsFailBeforeRunning(t *testing.T) {
	t.Setenv("SICOM_MAX_ZIP_BYTES", "-1")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid limit")
	}
}
func TestCustomPublicURLAndExternalCDP(t *testing.T) {
	t.Setenv("S3_PUBLIC_URL", "https://files.test/")
	t.Setenv("SICOM_CDP_URL", "http://localhost:9223")
	c, e := Load()
	if e != nil {
		t.Fatal(e)
	}
	if c.PublicURL != "https://files.test" || c.CDPURL != "http://localhost:9223" {
		t.Fatalf("config=%+v", c)
	}
}
