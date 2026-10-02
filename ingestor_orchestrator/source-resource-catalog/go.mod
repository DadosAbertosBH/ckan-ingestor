module gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog

go 1.26

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/google/uuid v1.6.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts v0.0.0
)

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts => ../ingestor-job-message-contracts
