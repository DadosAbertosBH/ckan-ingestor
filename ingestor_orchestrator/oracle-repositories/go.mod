module gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/oracle-repositories

go 1.26

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts => ../repository-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts => ../ingestor-job-message-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity => ../sicom-source-identity

require (
	github.com/google/uuid v1.6.0
	github.com/sijms/go-ora/v2 v2.9.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts v0.0.0-00010101000000-000000000000
)

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2 // indirect
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity v0.0.0 // indirect
)
