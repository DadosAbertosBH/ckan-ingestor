module gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/backend

go 1.26

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/apache/iggy/foreign/go v0.9.0
	github.com/go-chi/chi/v5 v5.2.3
	github.com/go-sql-driver/mysql v1.10.1
	github.com/google/uuid v1.6.0
	github.com/pressly/goose/v3 v3.26.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/iggy-message-transport v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity v0.0.0
)

require github.com/sijms/go-ora/v2 v2.9.0 // indirect

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/iggy-message-transport => ../iggy-message-transport

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts => ../ingestor-job-message-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity => ../sicom-source-identity

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/avast/retry-go/v5 v5.0.0 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/oracle-repositories v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts v0.0.0
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
)

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories => ../mysql-repositories

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts => ../repository-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/oracle-repositories => ../oracle-repositories
