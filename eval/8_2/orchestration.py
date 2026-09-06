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


@contextlib.contextmanager
def uvicorn_service(app: str, name: str, ready_timeout: float = 30.0):
    """Runs `uvicorn <app> --app-dir platform` on a free port until exit.
    Same shape as tests/conftest.py's semantic_api_server /
    event_ingestion_server fixtures, including running under sys.executable
    so it always uses the interpreter the harness itself is running under."""
    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", app,
            "--app-dir", str(ROOT / "platform"),
            "--host", "127.0.0.1", "--port", str(port),
        ],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + ready_timeout
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                raise RuntimeError(f"{name} exited early (code {proc.returncode}):\n{out}")
            try:
                if httpx.get(f"{base_url}/health", timeout=1.0).status_code == 200:
                    print(f"  [{name}] ready at {base_url}")
                    yield base_url
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.3)
        raise RuntimeError(f"{name} did not become healthy within {ready_timeout}s")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
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
