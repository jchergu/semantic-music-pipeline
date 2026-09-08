"""
Process and job lifecycle for eval/8_2's active harness.

METRICS.md section 0 lists three things that must be running per harness
invocation. Reading the code while building this stage turned up two more,
both hard blockers rather than nice-to-haves:

  * the Semantic API -- both branches of refresh_recommendations() call
    context_builder.build_context(http_client, ...), which is pure HTTP
    against the stage 4 API;
  * the stages 9-10 raw-state consumer -- refresh_recommendations() reads
    get_session_events() and returns skip_insufficient_data below 2 events,
    so with nothing populating session:{id}:events NO refresh ever fires.
    Stage 14's live test stands in for that consumer by calling
    record_event() directly; this harness runs the real
    session_consumer.consume_and_cache_many() instead, which is also what a
    real deployment would do.

So: five moving parts (two FastAPI services, the Flink job, and the two
independent Kafka consumer groups). This module owns starting and stopping
all of them, so `python -m eval.8_2.run` is one command rather than a
five-step runbook whose ordering silently decides whether the numbers mean
anything.

The uvicorn subprocess pattern (free port, poll /health, terminate/kill) is
lifted from tests/conftest.py, which has been running it since stage 4.
"""
from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent.parent

# docker-compose.yml pins the compose project name to 8-1-batch-reactive
# (see CLAUDE.md's Stack section for why it survived the repo rename), so
# container names are stable.
FLINK_JOBMANAGER_CONTAINER = "8-1-flink-jobmanager"
FLINK_REST = "http://localhost:8081"
FLINK_JOB_SRC = ROOT / "platform" / "streaming" / "flink_session_profile_job.py"
FLINK_JOB_DEST = "/opt/flink/session_profile_job.py"

# The Kafka source is KafkaOffsetsInitializer.latest(): a job that is
# RUNNING has not necessarily finished subscribing, and anything produced
# before it does is never seen at all (not replayed later). Deliberately
# generous -- a scenario that silently loses its first events would look
# like a cold-start finding rather than a harness bug.
FLINK_SETTLE_SECONDS = 20


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RestartableService:
    """A uvicorn service whose process can be stopped and started again on
    the SAME port.

    The port is claimed once, in __init__, and reused across restarts --
    that is the whole point. Stage 15D's semantic_api_outage arm kills this
    service mid-scenario while the refresh daemon is already running with a
    fixed --semantic-api-url, so a restart that landed on a different port
    would look like a permanent outage no matter how the daemon behaved,
    and the arm would measure the harness instead of the system.

    Same subprocess shape tests/conftest.py has used since stage 4 (free
    port, poll /health, terminate then kill), including running under
    sys.executable so it always uses the interpreter the harness itself is
    running under.
    """

    def __init__(self, app: str, name: str, ready_timeout: float = 30.0):
        self.app = app
        self.name = name
        self.ready_timeout = ready_timeout
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._proc: subprocess.Popen | None = None

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def start(self) -> str:
        if self.running:
            return self.base_url
        self._proc = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", self.app,
                "--app-dir", str(ROOT / "platform"),
                "--host", "127.0.0.1", "--port", str(self.port),
            ],
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.monotonic() + self.ready_timeout
        while time.monotonic() < deadline:
            if self._proc.poll() is not None:
                out = self._proc.stdout.read() if self._proc.stdout else ""
                raise RuntimeError(f"{self.name} exited early (code {self._proc.returncode}):\n{out}")
            try:
                if httpx.get(f"{self.base_url}/health", timeout=1.0).status_code == 200:
                    print(f"  [{self.name}] ready at {self.base_url}")
                    return self.base_url
            except httpx.TransportError:
                pass
            time.sleep(0.3)
        raise RuntimeError(f"{self.name} did not become healthy within {self.ready_timeout}s")

    def stop(self) -> None:
        if self._proc is None:
            return
        self._proc.terminate()
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=5)
        self._proc = None


@contextlib.contextmanager
def uvicorn_service(app: str, name: str, ready_timeout: float = 30.0):
    """Runs `uvicorn <app> --app-dir platform` on a free port until exit.

    Thin wrapper over RestartableService for the callers that never need to
    restart anything (run.py's metric 1-7 harness); the lifecycle itself
    lives there so there is exactly one copy of it.
    """
    service = RestartableService(app, name, ready_timeout)
    service.start()
    try:
        yield service.base_url
    finally:
        service.stop()


@contextlib.contextmanager
def daemon_process(module: str, args: list[str], name: str, log_path: Path,
                   ready_marker: str = "started", ready_timeout: float = 60.0):
    """Runs one of the stage 16 daemons as a real subprocess.

    Stage 15D drives the real deployment shape rather than this package's
    own threads (METRICS.md section 9.1), and a daemon is a process: it
    holds its own connections, its own consumer-group membership and its own
    committed offsets, none of which a thread inside the harness would
    exercise the same way.

    Output goes to a file rather than a PIPE. A daemon logs one line per
    event and these runs are minutes long, so an unread PIPE would fill its
    64KB buffer and block the daemon mid-scenario -- which would look
    exactly like the dependency outage the arm is trying to measure.
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "platform")
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [sys.executable, "-m", module, *args],
            cwd=str(ROOT), env=env, stdout=log_file, stderr=subprocess.STDOUT, text=True,
        )
        try:
            deadline = time.monotonic() + ready_timeout
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError(
                        f"{name} exited early (code {proc.returncode}); see {log_path}"
                    )
                if ready_marker in log_path.read_text():
                    print(f"  [{name}] ready (log: {log_path})")
                    yield proc
                    return
                time.sleep(0.3)
            raise RuntimeError(f"{name} did not log {ready_marker!r} within {ready_timeout}s")
        finally:
            # SIGTERM, not kill: daemon_runtime installs a handler that lets
            # the loop finish the message it is holding and commit, which is
            # the shutdown path a real deployment uses.
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


def list_flink_jobs() -> list[dict]:
    resp = httpx.get(f"{FLINK_REST}/jobs", timeout=10.0)
    resp.raise_for_status()
    return resp.json()["jobs"]


def cancel_running_flink_jobs() -> list[str]:
    """Cancels every RUNNING job before submitting ours.

    Not tidiness: a job left over from a previous invocation carries an
    already-advanced watermark, and this harness replays event time from a
    fixed epoch. Its events would arrive behind that watermark and be
    dropped silently -- the exact failure mode Stage 15B documented.
    """
    cancelled = []
    for job in list_flink_jobs():
        if job.get("status") in ("RUNNING", "RESTARTING", "CREATED"):
            httpx.patch(f"{FLINK_REST}/jobs/{job['id']}?mode=cancel", timeout=20.0).raise_for_status()
            cancelled.append(job["id"])
    for job_id in cancelled:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            states = {j["id"]: j["status"] for j in list_flink_jobs()}
            if states.get(job_id) not in ("RUNNING", "RESTARTING", "CREATED", "CANCELLING"):
                break
            time.sleep(1.0)
    return cancelled


def submit_flink_job(settle_seconds: float = FLINK_SETTLE_SECONDS) -> str:
    """docker cp + `flink run -d -py`, exactly the manual sequence in
    docs/platform/stage13-flink-session-job.md, then waits for RUNNING plus
    a settle window. Returns the job id."""
    subprocess.run(
        ["docker", "cp", str(FLINK_JOB_SRC), f"{FLINK_JOBMANAGER_CONTAINER}:{FLINK_JOB_DEST}"],
        check=True, capture_output=True, text=True,
    )
    proc = subprocess.run(
        ["docker", "exec", FLINK_JOBMANAGER_CONTAINER, "flink", "run", "-d", "-py", FLINK_JOB_DEST],
        check=False, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"flink run failed:\n{proc.stdout}\n{proc.stderr}")

    deadline = time.monotonic() + 120
    job_id = None
    while time.monotonic() < deadline:
        running = [j for j in list_flink_jobs() if j["status"] == "RUNNING"]
        if running:
            job_id = running[-1]["id"]
            break
        time.sleep(2.0)
    if job_id is None:
        raise RuntimeError(f"submitted job never reached RUNNING:\n{proc.stdout}")

    print(f"  [flink] job {job_id} RUNNING; settling {settle_seconds}s before producing events")
    time.sleep(settle_seconds)
    return job_id


@contextlib.contextmanager
def flink_session_profile_job(manage: bool = True):
    """Fresh job for the duration of the harness run, cancelled afterward
    so the next invocation starts from a clean watermark. manage=False
    leaves an operator-submitted job alone (it still checks one is
    RUNNING) -- an escape hatch for debugging against a job started by
    hand, not the default path."""
    if not manage:
        running = [j for j in list_flink_jobs() if j["status"] == "RUNNING"]
        if not running:
            raise RuntimeError("--no-manage-flink given but no RUNNING Flink job found")
        print(f"  [flink] reusing operator-submitted job {running[-1]['id']}")
        yield running[-1]["id"]
        return

    cancelled = cancel_running_flink_jobs()
    if cancelled:
        print(f"  [flink] cancelled pre-existing job(s): {', '.join(cancelled)}")
    job_id = submit_flink_job()
    try:
        yield job_id
    finally:
        with contextlib.suppress(Exception):
            httpx.patch(f"{FLINK_REST}/jobs/{job_id}?mode=cancel", timeout=20.0)
            print(f"  [flink] cancelled job {job_id}")
