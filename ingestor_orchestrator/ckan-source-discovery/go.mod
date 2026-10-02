module gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ckan-source-discovery

go 1.26

require gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog v0.0.0

require (
	github.com/google/uuid v1.6.0 // indirect
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts v0.0.0 // indirect
)

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog => ../source-resource-catalog

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts => ../ingestor-job-message-contracts
