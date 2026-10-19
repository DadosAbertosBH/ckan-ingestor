# Iggy session recovery release gate

The candidate is Apache Iggy's exact commit
`d2f970661f44da60cc6cd0510d1c999a2a70bda3` (PR #4283), which expires
orphaned consumer-group sessions without deleting the groups or offsets.
The production baseline is `apache/iggy@sha256:b35fc2841964f2962a5f164d33b3ae3a9de99a66920defd3cd8a1c039470602d`.

Build and run the fault suite from the repository root:

```sh
sh tools/iggy-recovery/build.sh ckan-iggy:session-recovery-d2f97066 --load
cargo build --manifest-path ckan-ingestor-rs/Cargo.toml -p iggy_processor --example session_probe
IGGY_RECOVERY_IMAGE=ckan-iggy:session-recovery-d2f97066 \
IGGY_SESSION_PROBE="$PWD/ckan-ingestor-rs/target/debug/examples/session_probe" \
uv run pytest tests/test_iggy_session_recovery.py -v --junitxml=iggy-recovery-results.xml
```

Ten repetitions of each scenario are required; `IGGY_RECOVERY_REPEATS=1` is
only a diagnostic shortcut. Tests create isolated Docker containers and keep
the same container filesystem across broker restarts. They check graceful
leave, SIGKILL, SIGSTOP, a TCP blackhole, persisted membership after a broker
crash, healthy idle sessions, and an upgrade over a volume written by the
baseline. That is 70 fault cases plus one Go compatibility test. The recovery
deadline is 60 seconds. Returning consumers must rejoin automatically.
`probe.log` and `broker.log` remain in pytest's temporary directory.

To reproduce the defect on the baseline, use the same suite with
`IGGY_RECOVERY_IMAGE=apache/iggy:0.9.0 IGGY_RECOVERY_LEGACY=1`; the legacy
switch omits the two session settings unsupported by that version.

The Rust probe uses the application's IggySource, WorkerHandler and pinned SDK,
including the application's retry behavior for transient polling errors.
Set `IGGY_INTEGRATION_ADDRESS` to the isolated broker address to validate the
Go SDK with `go test -run TestSessionBrokerCompatibility -count=1 ./...` in
`ingestor_orchestrator/iggy-message-transport`. Credentials in these tests
belong exclusively to the disposable test broker.

Candidate settings:

| Setting | Value |
| --- | --- |
| `IGGY_HEARTBEAT_ENABLED` | `true` |
| `IGGY_HEARTBEAT_INTERVAL` | `5s` |
| `IGGY_CONSUMER_GROUP_HEARTBEAT_INTERVAL` | `2s` |
| `IGGY_CONSUMER_GROUP_SESSION_TIMEOUT` | `30s` |
| Rust connection heartbeat | `2s` |
| Graceful consumer cleanup deadline | `10s` |

Deployment requires passing the fault suite, checking Go compatibility, and
testing a restored copy of the production volume. Record the published image
digest, source revision, test reports and pre-upgrade offsets together.
Stop the broker through GitOps before taking the final consistent backup.
The new server reads the old WAL/snapshots, but writes a changed format;
rollback restores the saved volume together with the saved image digest.
After deploying through GitOps, observe partition ownership and offset
advancement for 30 minutes. Preserve the existing group names and offsets.

The candidate built from the clean upstream checkout was published as:

```text
registry.gitlab.com/pedalin/ckan-ingestor/iggy:session-recovery-d2f97066@sha256:1ca2a60db33b3d75cb0200d02b711fc252c2bb2856bf49a33896b8e9f169bedd
```

Its image configuration includes the `org.opencontainers.image.revision`
label with the full upstream commit. Publication stages the candidate;
deployment is conditional on the full release gate and restore validation.
