"""
Stage 11: Event ingestion service.

A separate service from the Semantic API, per Decision A (CLAUDE.md): the
Semantic API stays read-only; this is where a behavioral event actually
lands. Accepts one HTTP request per event and produces it onto the
`behavioral-events` Kafka topic (platform/streaming/producer.py, stage 7)
-- downstream, the platform-owned consumer from Decision B
(platform/streaming/session_consumer.py, stages 9-10) picks it up from
there. The event shape stays ad hoc, not a frozen contract, same stance
stages 7/9/10 already took -- see contracts/README.md.

Run: uvicorn event_ingestion.main:app --app-dir platform --reload
     (from the repo root)
"""
import json

from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict

from streaming.config import TOPIC_BEHAVIORAL_EVENTS
from streaming.producer import produce

app = FastAPI(title="Event Ingestion Service")


class BehavioralEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    session_id: str
    event_type: str
    track_id: str | None = None


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/events", status_code=202)
def ingest_event(event: BehavioralEvent) -> dict:
    produce(TOPIC_BEHAVIORAL_EVENTS, json.dumps(event.model_dump()), key=event.session_id)
    return {"status": "accepted"}
