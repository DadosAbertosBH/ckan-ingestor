module gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog

go 1.26

require (
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts v0.0.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity v0.0.0 // indirect
)

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts => ../ingestor-job-message-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts => ../repository-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity => ../sicom-source-identity
