# Iggy session recovery validation, 2026-10-19

Application source: `523163526bc06e0cca7bd487d4c43adb201ad213`, release `0.0.139`.
Broker source: Apache Iggy `d2f970661f44da60cc6cd0510d1c999a2a70bda3` (PR #4283).
Published broker digest: `sha256:1ca2a60db33b3d75cb0200d02b711fc252c2bb2856bf49a33896b8e9f169bedd`.

## Fault tests

The unchanged production baseline failed the persisted-membership recovery
case: the replacement joined but never committed the next message within
60 seconds. The candidate passed all 71 release-gate cases in 892.61 seconds.
The probe uses the production IggySource and WorkerHandler, including polling
retries, explicit commits and asynchronous consumer shutdown.

| Scenario | Repetitions | Longest complete case |
| --- | ---: | ---: |
| Graceful shutdown | 10 | 1.59 s |
| SIGKILL | 10 | 1.17 s |
| SIGSTOP and automatic rejoin after resumption | 10 | 31.41 s |
| TCP blackhole and automatic rejoin after restoration | 10 | 51.75 s |
| Broker crash with persisted membership | 10 | 34.26 s |
| Healthy idle session beyond the expiry window | 10 | 36.44 s |
| Upgrade over the baseline's persistent volume | 10 | 34.65 s |
| Go SDK compatibility | 1 | 2.54 s |

The idle case intentionally waits 35 seconds. Case durations include setup,
rejoin and cleanup, rather than representing heartbeat expiry in isolation.
The test report is `/tmp/ckan-iggy-recovery-gate.xml` on the validation host.

Local checks also passed: Rust worker/coordinator and shared adapter tests,
Rust formatting and Clippy, Go transport race tests, Go backend tests,
27 Helm tests, Ruff and 31 ordinary root pytest tests. Fault tests are opt-in
and skipped in the ordinary pytest run. The backend's historical pytest
command finds no `tests/` directory; its tests are now Go tests.

## Restore of production data

The broker was stopped through GitOps before taking a Longhorn snapshot and
copying the volume through a read-only mount. The local compressed copy's
SHA-256 matched the cluster copy:

```text
18fed15ffa2f914dc4107bf5592ae629ec9471c20464e76b40e0c0d88460252c
```

Rollback snapshot: `longhorn-system/iggy-pre-session-recovery-20261019`.
Production volume: `pvc-74de2795-f7f9-4173-a7ef-e73c7b82d376`.

The same archive was independently restored into isolated baseline and
candidate containers. All 15 partition/group offset states matched exactly:

| Topic / group | Stored offsets by partition |
| --- | --- |
| jobs / ckan-worker | 218756, 200984, 218679, 208530, 218625, 214543 |
| jobs-retry / ckan-worker | unset, 65537, unset |
| parquet-results / ducklake-writer | 1673865 |
| job-results / ckan-result-consumer | 686320, 1211655, 686572 |
| ckan_metadata_sync / source-sync-worker | 65553 |
| ckan_metadata_sync_result / ckan-metadata-sync-result-consumer | 65553 |

On this real restore, the baseline retained both orphaned ducklake-writer
members. The candidate expired both automatically and retained the group's
ID (0) and stored offset (1673865). Groups and offsets were preserved
throughout the upgrade.

## Production rollout

The validated broker image was deployed through GitOps and reached Ready
with zero restarts on 2026-10-19 at approximately 18:30 UTC. The API,
source worker, worker and worker coordinator now run 0.0.139.

The broker session recovery itself is healthy. The production observation
found an independent application-data condition: `job-results` offsets
686321, 1211656 and 686573 contain PENDING messages for deleted instance
`19edbc27-3852-4d39-81a2-265787fb96de`. Oracle correctly rejects their job
inserts with `FK_CKAN_DATA_JOB_INSTANCE_ID`; the consumer therefore retains
the offsets for a durable, auditable resolution rather than skipping them.
