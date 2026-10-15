// Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
// Licensed under the GNU Affero General Public License, version 3 or later.
package sourcesyncworker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	ckandiscovery "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ckan-source-discovery"
	iggytransport "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/iggy-message-transport"
	mysqlrepo "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories"
	oraclerepo "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/oracle-repositories"
	sicomcrawler "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler"
	catalog "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog"
	sourcesync "gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-sync-message-contracts"
	"os"
	"time"
)

type Config struct{ DBProvider, OracleDSN, MySQLDSN, IggyAddress, IggyUsername, IggyPassword, Stream, SyncTopic, SyncGroup, JobsTopic, ResultsTopic, SyncResultsTopic string }

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func LoadConfig() Config {
	host := env("INGEST_ORCH_MYSQL_HOST", "localhost")
	port := env("INGEST_ORCH_MYSQL_PORT", "3306")
	user := env("INGEST_ORCH_MYSQL_USER", "root")
	password := os.Getenv("INGEST_ORCH_MYSQL_PASSWORD")
	database := env("INGEST_ORCH_MYSQL_DATABASE", "ingestor_orchestrator")
	return Config{DBProvider: env("INGEST_ORCH_DB_PROVIDER", "oracle"), OracleDSN: os.Getenv("INGEST_ORCH_ORACLE_DSN"), MySQLDSN: fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4", user, password, host, port, database), IggyAddress: env("IGGY_ADDRESS", "iggy:8090"), IggyUsername: env("IGGY_USERNAME", "iggy"), IggyPassword: os.Getenv("IGGY_PASSWORD"), Stream: env("IGGY_STREAM", "ckan-ingestor"), SyncTopic: env("IGGY_METADATA_SYNC_TOPIC", "ckan_metadata_sync"), SyncGroup: env("IGGY_METADATA_SYNC_GROUP_ID", "source-sync-worker"), JobsTopic: env("IGGY_TOPIC", "jobs"), ResultsTopic: env("IGGY_TOPIC_RESULTS", "job-results"), SyncResultsTopic: env("IGGY_METADATA_SYNC_RESULT_TOPIC", "ckan_metadata_sync_result")}
}

type Runner struct{ Worker *catalog.Worker }

func (r Runner) Handle(ctx context.Context, payload []byte) error {
	command, err := sourcesync.Decode(payload)
	if err != nil {
		return err
	}
	return r.Worker.Sync(ctx, command.SyncID, command.SourceID, command.SourceType)
}
func Run(ctx context.Context, cfg Config) error {
	if cfg.IggyPassword == "" {
		return errors.New("IGGY_PASSWORD is required")
	}
	db, catalogStore, err := openRepositories(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.PingContext(ctx); err != nil {
		return err
	}
	queue, err := iggytransport.Connect(ctx, iggytransport.ConnectionConfig{Address: cfg.IggyAddress, Username: cfg.IggyUsername, Password: cfg.IggyPassword, Stream: cfg.Stream})
	if err != nil {
		return err
	}
	defer queue.Close()
	worker := &catalog.Worker{Catalog: catalogStore, Publisher: queue, SyncResultsTopic: cfg.SyncResultsTopic, Adapters: map[string]catalog.Adapter{"ckan": &ckandiscovery.Adapter{}, "sicom": sicomcrawler.Adapter{}}}
	consumer := iggytransport.Consumer{Driver: queue, PollInterval: time.Second}
	runner := Runner{Worker: worker}
	return consumer.Consume(ctx, cfg.SyncTopic, cfg.SyncGroup, func(payload []byte) error { return runner.Handle(ctx, payload) })
}

func openRepositories(cfg Config) (*sql.DB, catalog.SourceResourceCatalogRepository, error) {
	switch cfg.DBProvider {
	case "mysql":
		store, err := mysqlrepo.Open(cfg.MySQLDSN)
		if err != nil {
			return nil, nil, err
		}
		repo := &mysqlrepo.MySQLSourceResourceCatalogRepository{DB: store.DB, JobsTopic: cfg.JobsTopic, ResultsTopic: cfg.ResultsTopic, SyncResultsTopic: cfg.SyncResultsTopic}
		return store.DB, repo, nil
	case "oracle":
		if cfg.OracleDSN == "" {
			return nil, nil, errors.New("INGEST_ORCH_ORACLE_DSN is required")
		}
		store, err := oraclerepo.Open(cfg.OracleDSN)
		if err != nil {
			return nil, nil, err
		}
		store.DB.SetMaxOpenConns(3)
		repo := &oraclerepo.OracleSourceResourceCatalogRepository{DB: store.DB, JobsTopic: cfg.JobsTopic, ResultsTopic: cfg.ResultsTopic, SyncResultsTopic: cfg.SyncResultsTopic}
		return store.DB, repo, nil
	default:
		return nil, nil, fmt.Errorf("unknown database provider %q", cfg.DBProvider)
	}
}
