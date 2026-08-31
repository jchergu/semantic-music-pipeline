"""Verifies the Stage 8 Flink provisioning (JobManager + TaskManager) against the live cluster."""
import os
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

FLINK_JOBMANAGER_URL = f"http://localhost:{os.environ.get('FLINK_JOBMANAGER_WEBUI_PORT', '8081')}"


def test_jobmanager_rest_reachable():
    response = httpx.get(f"{FLINK_JOBMANAGER_URL}/config", timeout=10.0)
    assert response.status_code == 200
    assert "flink-version" in response.json()


def test_taskmanager_registered():
    deadline = time.monotonic() + 30.0
    taskmanagers = []
    while time.monotonic() < deadline:
        response = httpx.get(f"{FLINK_JOBMANAGER_URL}/taskmanagers", timeout=10.0)
        taskmanagers = response.json().get("taskmanagers", [])
        if taskmanagers:
            break
        time.sleep(1.0)
    assert len(taskmanagers) >= 1
