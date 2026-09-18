// Noctcloud Desenvolvimento LTDA
// Copyright (C) 2026  Noctcloud Desenvolvimento LTDA
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("INGEST_ORCH_MYSQL_PASSWORD", "")
	cfg := Load()

	if cfg.HTTPAddress != ":8081" {
		t.Fatalf("HTTPAddress = %q", cfg.HTTPAddress)
	}
	if cfg.IggyAddress != "localhost:8090" {
		t.Fatalf("IggyAddress = %q", cfg.IggyAddress)
	}
	if cfg.Stream != "ckan-ingestor" || cfg.JobTopic != "jobs" || cfg.RetryTopic != "jobs-retry" {
		t.Fatalf("unexpected topology: %#v", cfg)
	}
	if cfg.SchedulerInterval != 480*time.Minute {
		t.Fatalf("SchedulerInterval = %s", cfg.SchedulerInterval)
	}
	if cfg.MySQLDSN != "root:@tcp(localhost:3306)/ingestor_orchestrator?parseTime=true&loc=UTC" {
		t.Fatalf("MySQLDSN = %q", cfg.MySQLDSN)
	}
}

func TestLoadEnvironment(t *testing.T) {
	t.Setenv("INGEST_ORCH_GO_ADDRESS", "127.0.0.1:9000")
	t.Setenv("INGEST_ORCH_MYSQL_HOST", "mysql")
	t.Setenv("INGEST_ORCH_MYSQL_PORT", "3307")
	t.Setenv("INGEST_ORCH_MYSQL_USER", "app")
	t.Setenv("INGEST_ORCH_MYSQL_PASSWORD", "p@ss")
	t.Setenv("INGEST_ORCH_MYSQL_DATABASE", "ckan")
	t.Setenv("INGEST_ORCH_IGGY_ADDRESS", "iggy:8090")
	t.Setenv("INGEST_ORCH_IGGY_JOB_PARTITIONS", "6")
	t.Setenv("INGEST_ORCH_SCHEDULER_INTERVAL_MINUTES", "12")

	cfg := Load()
	if cfg.HTTPAddress != "127.0.0.1:9000" || cfg.JobPartitions != 6 {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	if cfg.SchedulerInterval != 12*time.Minute {
		t.Fatalf("SchedulerInterval = %s", cfg.SchedulerInterval)
	}
	if cfg.MySQLDSN != "app:p@ss@tcp(mysql:3307)/ckan?parseTime=true&loc=UTC" {
		t.Fatalf("MySQLDSN = %q", cfg.MySQLDSN)
	}
}
