module gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories

go 1.26

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/go-sql-driver/mysql v1.10.1
	github.com/google/uuid v1.6.0
	github.com/pressly/goose/v3 v3.26.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts v0.0.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity v0.0.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
)

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts => ../repository-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts => ../ingestor-job-message-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity => ../sicom-source-identity
