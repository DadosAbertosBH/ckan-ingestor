module gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-sync-worker

go 1.26

require (
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ckan-source-discovery v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/iggy-message-transport v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-sync-message-contracts v0.0.0
)

require (
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/pressly/goose/v3 v3.26.0 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	github.com/sijms/go-ora/v2 v2.9.0 // indirect
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts v0.0.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/apache/iggy/foreign/go v0.9.0 // indirect
	github.com/avast/retry-go/v5 v5.0.0 // indirect
	github.com/aws/aws-sdk-go-v2 v1.47.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.33.6 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.20.6 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.1 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.23.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.10.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.38.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.43.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.1 // indirect
	github.com/aws/smithy-go v1.28.1 // indirect
	github.com/chromedp/cdproto v0.0.0-20260714215040-dc233986426f // indirect
	github.com/chromedp/chromedp v0.16.0 // indirect
	github.com/chromedp/sysutil v1.1.0 // indirect
	github.com/go-json-experiment/json v0.0.0-20260623181947-01eb4420fa68 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.4.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts v0.0.0 // indirect
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/oracle-repositories v0.0.0
	gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity v0.0.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ckan-source-discovery => ../ckan-source-discovery

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/iggy-message-transport => ../iggy-message-transport

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-crawler => ../sicom-crawler

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-resource-catalog => ../source-resource-catalog

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/source-sync-message-contracts => ../source-sync-message-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/ingestor-job-message-contracts => ../ingestor-job-message-contracts

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/sicom-source-identity => ../sicom-source-identity

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/oracle-repositories => ../oracle-repositories

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/mysql-repositories => ../mysql-repositories

replace gitlab.com/pedalin/ckan-ingestor/ingestor_orchestrator/repository-contracts => ../repository-contracts
