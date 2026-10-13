# SICOM crawler

Independent Go service for TCE-MG SICOM. Authentication uses chromedp over CDP
with the official Lightpanda nightly. Downloads and R2 transfers use Go HTTP/S3
clients; no browser download directory is required.

## Pipeline

1. Validate the dedicated SICOM instance seeded by orchestrator migration 00006.
2. Authenticate on the portal, discover every published SICOM year from its
   loaded scripts, and list every category using `origem=SICOM`.
3. HEAD the immutable source ZIP in R2. Only `NotFound` authorizes a download
   from TCE-MG. An existing ZIP is always reused, without TTL or origin HEAD.
4. Validate CRCs and archive limits, store the original ZIP, recursively extract
   supported members, and store the completed member manifest.
5. Confirm `PENDING` on `job-results`, then send a worker job to `jobs` for each
   member. Each daily run republishes all jobs, including cached members.

Object identities use UUIDv5 of the canonical `baixarArquivoPct/{seqZip}` URL
and the complete nested member path. Object keys live in `crawled-files/`;
manifests live in `crawled-discovery/sicom/`. A missing/incomplete manifest is
rebuilt **from R2**, with existing members reused. Queue failures are recovered
by the next run; delivery is at-least-once and job/resource IDs remain stable.
ZIPs and members are immutable. If the source ever reuses `seqZip` for different
bytes, introduce a new identity/version rather than refreshing this cache.

Jobs are independent: datasets group by year/category but member schemas are
not automatically merged. Archives retain all original files; only configured
formats become extracted objects/jobs. Defaults: CSV, JSON, GEOJSON.

## Local read-only verification

```sh
cd ingestor_orchestrator/sicom-crawler
SICOM_LIGHTPANDA_BIN=/path/to/nightly/lightpanda \
  go run ./cmd/sicom-crawler --discover-only --year 2026 \
  --category 'Despesas com pessoal'
```

`--discover-only` uses neither R2 nor Iggy and requires no storage credentials.
Without year/category flags, discovery or crawling covers all published years
and categories. For CAPTCHA intervention, provide an existing visual browser
using `SICOM_CDP_URL=http://localhost:9222` and add `--interactive` (30-minute
authentication timeout). Lightpanda itself is headless; an interactive CAPTCHA
in the CronJob produces a failure diagnostic for manual follow-up.

The portal currently requires a missing Sectigo intermediate. The embedded
public chain extends system trust for both Go and Lightpanda; TLS verification
remains enabled. `SICOM_CA_BUNDLE` optionally extends trust further. Child
browser output is suppressed because redirect URLs can contain JWTs.
Public certificate fingerprints (SHA-256):

- OV R36: `6542D176BED50F193C0CE297AE44ECD8A0A86BEC2EDE682769344059B4E78530`
- Root R46: `7BB647A62AEEAC88BF257AA522D01FFEA395E0AB45C73F93F65654EC38F25A06`

## Configuration

| Environment | Default / purpose |
| --- | --- |
| `S3_ENDPOINT`, `S3_BUCKET` | `rustfs:9000`, `public-datasets` |
| `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | Required for crawling |
| `S3_REGION`, `S3_USE_SSL`, `S3_URL_STYLE` | `auto`, `false`, `path` |
| `S3_PUBLIC_URL` | Endpoint/bucket, matching the Rust crawler convention |
| `IGGY_ADDRESS`, `IGGY_USERNAME`, `IGGY_PASSWORD` | `iggy:8090`, `iggy`, required password |
| `IGGY_STREAM`, `IGGY_TOPIC`, `IGGY_TOPIC_RESULTS` | `ckan-ingestor`, `jobs`, `job-results` |
| `SICOM_ORCHESTRATOR_URL` | `http://localhost:8000` |
| `SICOM_INSTANCE_ID` | `4be9dd37-1f28-540d-a7c3-104e4f5a54e8` |
| `SICOM_CDP_URL`, `SICOM_LIGHTPANDA_BIN` | External CDP, or owned `lightpanda` process |
| `SICOM_FORMATS` | `CSV,JSON,GEOJSON` |
| `SICOM_MAX_ZIP_BYTES` | 8 GiB |
| `SICOM_MAX_EXPANDED_BYTES` | 64 GiB per archive (including expanded nested ZIPs) |
| `SICOM_MAX_ARCHIVE_DEPTH`, `SICOM_MAX_ARCHIVE_ENTRIES` | 4 nested levels, 100000 entries |
| `TMPDIR` | Temporary seekable files; provision space for originals, members and nesting |

**R2 accessibility:** an R2 S3 API endpoint is authenticated, not a public
download domain. Configure `S3_PUBLIC_URL` with a public custom domain/r2.dev URL
accessible to existing workers. Storage API requests are signed independently;
published URLs are permanent and do not contain expiring presigned credentials.
Keep credentials in a Secret; crawler requires object read/write permissions,
not bucket administration. Existing Iggy topology is created by the coordinator.

## Deployment and tests

SICOM discovery runs in `source-sync-worker`, using the same metadata-sync
trigger and scheduling path as every other source instance. Configure the
shared worker through `sourceSyncWorker` in the Helm chart. Credentials come
from the existing chart Secret.
No production uploads or queue publications occur during tests.

```sh
go test -race ./...
go vet ./...
docker buildx build --platform linux/amd64,linux/arm64 \
  -f ingestor_orchestrator/sicom-crawler/Dockerfile . # run from repo root
helm unittest charts/ingestor-orchestrator             # run from repo root
```

Docker copies the official nightly executable from `lightpanda/browser:nightly`
for each target architecture and packages both executables with Debian/glibc,
system CAs and tini. Build with `--pull` to refresh the nightly image. The Go
binary is cross-compiled. `.gitlab-ci.yml` runs tests and publishes a multiarch image.

Optional read-only live test against an already running CDP browser:

The CDP browser needs the same trusted certificate chain as the crawler. For
Lightpanda on macOS, start it from this module directory with:

```sh
lightpanda serve --host 127.0.0.1 --port 9222 \
  --ca-cert /etc/ssl/cert.pem \
  --ca-cert internal/portal/certificates/sectigo-chain.pem \
  --load-resources iframe --load-resources worker
```

On Debian, use `/etc/ssl/certs/ca-certificates.crt` for the system CA file.
The crawler configures this trust store automatically for its owned Lightpanda
process. A visible image CAPTCHA is reported as requiring intervention; use a
visual CDP browser with `--interactive` to allow manual completion. Timeouts and
navigation errors are reported separately.

```sh
SICOM_LIVE_CDP=http://127.0.0.1:9222 \
  go test ./internal/portal -run TestLive -v -count=1
```

An already downloaded ZIP can be tested with an in-memory storage/broker using
`SICOM_TEST_ZIP=/path/to/archive.zip go test ./internal/crawler -run TestLocalSICOMArchive -v -count=1`.

Tests cover archive nesting, immutable cache, manifest reconstruction, unsafe
paths/expansion limits, storage failures, publication order, resumability,
authorization renewal and dynamic year discovery. MySQL/Iggy integration
checks require the same explicitly configured test services as the backend.
