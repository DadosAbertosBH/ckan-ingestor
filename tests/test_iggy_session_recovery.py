# Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
# Licensed under the GNU Affero General Public License, version 3 or later.
"""Opt-in broker fault tests using the same Rust adapter as the coordinator.

Build: cargo build -p iggy_processor --example session_probe
Run: IGGY_RECOVERY_IMAGE=<image> IGGY_SESSION_PROBE=<binary> uv run pytest \
    tests/test_iggy_session_recovery.py -v
The default ten repetitions are the release gate. Every case owns its container.
"""

import json
import os
import selectors
import signal
import select
import socket
import socketserver
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest

IMAGE = os.environ.get("IGGY_RECOVERY_IMAGE")
PROBE = os.environ.get("IGGY_SESSION_PROBE")
pytestmark = pytest.mark.skipif(
    not IMAGE or not PROBE, reason="set IGGY_RECOVERY_IMAGE and IGGY_SESSION_PROBE"
)


def docker(*args):
    return subprocess.check_output(
        ["docker", *args], text=True, stderr=subprocess.STDOUT
    ).strip()


class NetworkProxy:
    """Drop traffic while retaining sockets, simulating a network blackhole."""

    def __init__(self, address):
        self.blocked = threading.Event()
        self.closed = threading.Event()
        proxy = self
        host, port = address.rsplit(":", 1)

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                with socket.create_connection((host, int(port)), timeout=5) as upstream:
                    sockets = [self.request, upstream]
                    try:
                        while not proxy.closed.is_set():
                            for source in select.select(sockets, [], [], 0.1)[0]:
                                payload = source.recv(65536)
                                if not payload:
                                    return
                                if not proxy.blocked.is_set():
                                    target = (
                                        upstream
                                        if source is self.request
                                        else self.request
                                    )
                                    target.sendall(payload)
                    except OSError:
                        return

        self.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.address = f"127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.closed.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class Probe:
    def __init__(self, address, log):
        self.process = subprocess.Popen(
            [str(Path(PROBE).resolve()), "consume"],
            env={**os.environ, "IGGY_PROBE_ADDRESS": address},
            stdout=subprocess.PIPE,
            stderr=log,
        )
        self.buffer = b""
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)

    def buffered_event(self, expected):
        while b"\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n", 1)
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if value.get("event") == expected:
                return value
        return None

    def event(self, expected, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = self.buffered_event(expected)
            if value:
                return value
            for key, _ in self.selector.select(
                min(1, max(0, deadline - time.monotonic()))
            ):
                chunk = os.read(key.fileobj.fileno(), 65536)
                assert chunk, f"probe exited {self.process.poll()} before {expected}"
                self.buffer += chunk
        pytest.fail(f"no {expected} event within {timeout}s")

    def close(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGCONT)
            self.process.kill()
        self.process.wait(timeout=10)
        self.selector.close()
        self.process.stdout.close()


def any_committed(probes, timeout=60):
    deadline = time.monotonic() + timeout
    with selectors.DefaultSelector() as selector:
        for probe in probes:
            selector.register(probe.process.stdout, selectors.EVENT_READ, probe)
        while time.monotonic() < deadline:
            for probe in probes:
                value = probe.buffered_event("committed")
                if value:
                    return value
            for key, _ in selector.select(0.25):
                chunk = os.read(key.fileobj.fileno(), 65536)
                assert chunk, (
                    f"consumer exited {key.data.process.poll()} after session recovery"
                )
                key.data.buffer += chunk
    pytest.fail("no message committed after the expired consumer returned")


@pytest.fixture
def broker(tmp_path, request):
    name = f"ckan-session-test-{uuid.uuid4().hex[:12]}"
    volume = f"{name}-data"
    upgrade_case = getattr(request.node, "callspec", None)
    upgrade_case = upgrade_case and upgrade_case.params.get("failure") == "upgrade"
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        tcp_port = reservation.getsockname()[1]
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        http_port = reservation.getsockname()[1]
    session_settings = (
        []
        if os.environ.get("IGGY_RECOVERY_LEGACY") == "1"
        else [
            "-e",
            "IGGY_CONSUMER_GROUP_HEARTBEAT_INTERVAL=2s",
            "-e",
            "IGGY_CONSUMER_GROUP_SESSION_TIMEOUT=30s",
        ]
    )
    run_args = (
        "run",
        "-d",
        "--name",
        name,
        "--security-opt",
        "seccomp=unconfined",
        "--cap-add",
        "IPC_LOCK",
        "--cap-add",
        "SYS_NICE",
        "--mount",
        f"type=volume,source={volume},target=/data",
        "-p",
        f"127.0.0.1:{tcp_port}:8090",
        "-p",
        f"127.0.0.1:{http_port}:3000",
        "-e",
        "IGGY_ROOT_USERNAME=iggy",
        "-e",
        "IGGY_ROOT_PASSWORD=session-test-password",
        "-e",
        "IGGY_HTTP_ADDRESS=0.0.0.0:3000",
        "-e",
        "IGGY_TCP_ADDRESS=0.0.0.0:8090",
        "-e",
        "IGGY_NODE_ADVERTISED_ADDRESS=127.0.0.1",
        "-e",
        "IGGY_SHARDING_CPU_ALLOCATION=2",
        "-e",
        "IGGY_PATH=/data",
        "-e",
        "IGGY_HEARTBEAT_ENABLED=true",
        "-e",
        "IGGY_HEARTBEAT_INTERVAL=5s",
    )
    docker("volume", "create", volume)
    initial_image = "apache/iggy:0.9.0" if upgrade_case else IMAGE
    docker(*run_args, *([] if upgrade_case else session_settings), initial_image)
    ports = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"]
    address = f"127.0.0.1:{ports['8090/tcp'][0]['HostPort']}"
    http = f"http://127.0.0.1:{ports['3000/tcp'][0]['HostPort']}"

    def ready():
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            state = json.loads(docker("inspect", name))[0]["State"]
            assert state["Running"], docker("logs", "--tail", "30", name)
            try:
                with urllib.request.urlopen(f"{http}/ping", timeout=1):
                    return
            except (OSError, urllib.error.URLError):
                time.sleep(0.25)
        pytest.fail(f"broker failed to start: {docker('logs', '--tail', '30', name)}")

    def upgrade():
        (tmp_path / "baseline-broker.log").write_text(docker("logs", name))
        docker("rm", name)
        docker(*run_args, *session_settings, IMAGE)
        ready()

    try:
        ready()
        yield name, address, ready, upgrade
    finally:
        (tmp_path / "broker.log").write_text(docker("logs", name))
        docker("rm", "-f", name)
        docker("volume", "rm", volume)


def command(address, *args):
    return subprocess.run(
        [str(Path(PROBE).resolve()), *args],
        env={**os.environ, "IGGY_PROBE_ADDRESS": address},
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )


def wait_for_members(address, count):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        state = json.loads(command(address, "state").stdout.splitlines()[-1])["group"]
        if state["members_count"] == count:
            return state
        time.sleep(0.25)
    pytest.fail(f"group did not converge to {count} members: {state}")


@pytest.mark.parametrize(
    "repeat", range(int(os.environ.get("IGGY_RECOVERY_REPEATS", "10")))
)
@pytest.mark.parametrize(
    "failure",
    ["graceful", "kill", "stop", "network", "broker_restart", "idle", "upgrade"],
)
def test_membership_recovers_and_preserves_committed_offset(
    broker, tmp_path, repeat, failure
):
    name, address, ready, upgrade = broker
    command(address, "setup")
    probes = []
    proxy = NetworkProxy(address) if failure == "network" else None
    with (tmp_path / "probe.log").open("w") as log:
        try:
            owner = Probe(proxy.address if proxy else address, log)
            probes.append(owner)
            owner.event("joined")
            command(address, "send", "first")
            first = owner.event("committed")
            assert first["payload"] == "first"

            if failure == "idle":
                # More than one full session timeout without application messages.
                time.sleep(35)
                command(address, "send", "second")
                second = owner.event("committed")
            else:
                if failure == "graceful":
                    owner.process.terminate()
                    owner.event("left", timeout=10)
                    owner.process.wait(timeout=10)
                elif failure == "kill":
                    owner.process.kill()
                    owner.process.wait(timeout=10)
                elif failure == "stop":
                    owner.process.send_signal(signal.SIGSTOP)
                elif failure == "network":
                    proxy.blocked.set()
                elif failure in {"broker_restart", "upgrade"}:
                    docker("kill", name)
                    owner.process.kill()
                    owner.process.wait(timeout=10)
                    if failure == "upgrade":
                        upgrade()
                    else:
                        docker("start", name)
                        ready()

                replacement = Probe(address, log)
                probes.append(replacement)
                replacement.event("joined")
                command(address, "send", "second")
                second = replacement.event("committed", timeout=60)
                state = json.loads(command(address, "state").stdout.splitlines()[-1])[
                    "group"
                ]
                assert state["members_count"] == 1, (
                    f"dead member still retained: {state}"
                )
                assert state["members"][0]["partitions"] == [0]
                if failure in {"stop", "network"}:
                    if proxy:
                        proxy.blocked.clear()
                    else:
                        owner.process.send_signal(signal.SIGCONT)
                    # The expired session must authenticate and rejoin on its own.
                    wait_for_members(address, 2)
                    command(address, "send", "third")
                    assert any_committed(probes)["payload"] == "third"
            assert second["payload"] == "second", "a confirmed message was replayed"
            assert second["offset"] > first["offset"]
        finally:
            for probe in probes:
                probe.close()
            if proxy:
                proxy.close()


def test_go_sdk_compatible_with_candidate_broker(broker):
    _, address, _, _ = broker
    result = subprocess.run(
        ["go", "test", "-run", "TestSessionBrokerCompatibility", "-count=1", "."],
        cwd=Path(__file__).resolve().parents[1]
        / "ingestor_orchestrator/iggy-message-transport",
        env={**os.environ, "IGGY_INTEGRATION_ADDRESS": address},
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
