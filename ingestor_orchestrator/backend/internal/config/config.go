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
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddress         string
	StaticDir           string
	MySQLDSN            string
	IggyAddress         string
	IggyUsername        string
	IggyPassword        string
	Stream              string
	JobTopic            string
	RetryTopic          string
	ResultTopic         string
	MetadataTopic       string
	MetadataResultTopic string
	ResultGroup         string
	MetadataResultGroup string
	JobPartitions       int
	ResultPartitions    int
	PollInterval        time.Duration
	SchedulerInterval   time.Duration
	StartupTimeout      time.Duration
}

func Load() Config {
	host := env("INGEST_ORCH_MYSQL_HOST", "localhost")
	port := envInt("INGEST_ORCH_MYSQL_PORT", 3306)
	user := env("INGEST_ORCH_MYSQL_USER", "root")
	password := os.Getenv("INGEST_ORCH_MYSQL_PASSWORD")
	database := env("INGEST_ORCH_MYSQL_DATABASE", "ingestor_orchestrator")

	return Config{
		HTTPAddress:         env("INGEST_ORCH_GO_ADDRESS", ":8000"),
		StaticDir:           env("INGEST_ORCH_STATIC_DIR", "/app/frontend"),
		MySQLDSN:            fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC", user, password, host, port, database),
		IggyAddress:         env("INGEST_ORCH_IGGY_ADDRESS", "localhost:8090"),
		IggyUsername:        env("INGEST_ORCH_IGGY_USERNAME", "iggy"),
		IggyPassword:        env("INGEST_ORCH_IGGY_PASSWORD", "iggy"),
		Stream:              env("INGEST_ORCH_IGGY_STREAM", "ckan-ingestor"),
		JobTopic:            env("INGEST_ORCH_IGGY_TOPIC", "jobs"),
		RetryTopic:          env("INGEST_ORCH_IGGY_TOPIC_RETRY", "jobs-retry"),
		ResultTopic:         env("INGEST_ORCH_IGGY_TOPIC_RESULTS", "job-results"),
		MetadataTopic:       env("INGEST_ORCH_IGGY_METADATA_SYNC_TOPIC", "ckan_metadata_sync"),
		MetadataResultTopic: env("INGEST_ORCH_IGGY_METADATA_SYNC_RESULT_TOPIC", "ckan_metadata_sync_result"),
		ResultGroup:         env("INGEST_ORCH_IGGY_RESULT_GROUP_ID", "ckan-result-consumer"),
		MetadataResultGroup: env("INGEST_ORCH_IGGY_METADATA_SYNC_RESULT_GROUP_ID", "ckan-metadata-sync-result-consumer"),
		JobPartitions:       envInt("INGEST_ORCH_IGGY_JOB_PARTITIONS", 10),
		ResultPartitions:    envInt("INGEST_ORCH_IGGY_RESULT_PARTITIONS", 1),
		PollInterval:        time.Duration(envInt("INGEST_ORCH_IGGY_CONSUMER_POLL_INTERVAL_MS", 500)) * time.Millisecond,
		SchedulerInterval:   time.Duration(envInt("INGEST_ORCH_SCHEDULER_INTERVAL_MINUTES", 480)) * time.Minute,
		StartupTimeout:      time.Duration(envInt("INGEST_ORCH_GO_STARTUP_TIMEOUT_SECONDS", 60)) * time.Second,
	}
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
