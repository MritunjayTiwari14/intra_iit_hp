"""
api.py — FastAPI server exposing telemetry, health report, scenarios, & clinician actions.

Endpoints:
  GET  /api/health           — Startup health report with adapter status
  GET  /api/cohort           — All patient twins with MEWS & urgency
  GET  /api/escalations      — Active and all escalations
  GET  /api/audit            — Immutable audit trail
  GET  /api/suppressions     — Tier 1 suppression log
  GET  /api/telemetry        — Pipeline telemetry counters

  POST /api/clinician-action — Record clinician decision
  GET  /api/vitals/{pid}     — Patient vitals history
"""

from __future__ import annotations

import logging
import time
import asyncio
import threading
import json
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaConsumer
from backend.models import VitalReading

from backend.config import get_config
from backend.database import get_db
from backend.stream_engine import get_processor
from backend.simulator import bootstrap_cohort
from backend.langgraph_copilot import process_clinician_action

logger = logging.getLogger("copilot.api")

# Track bootstrap state
_bootstrapped = False

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

def kafka_polling_loop():
    logger.info("Starting Kafka polling loop...")
    while True:
        try:
            consumer = KafkaConsumer(
                'vitals_stream',
                bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:19092'),
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                auto_offset_reset='latest'
            )
            processor = get_processor()
            db = get_db()
            logger.info("Kafka consumer connected successfully.")
            for message in consumer:
                if not message.value: continue
                data = message.value
                try:
                    vital = VitalReading(**data)
                    processor.ingest_vital_reading(vital)
                    db.insert_vital(vital)
                    twin = processor.get_twin(vital.patient_id)
                    if twin:
                        db.upsert_twin(twin)
                except Exception as e:
                    logger.error(f"Error processing kafka message: {e}")
        except Exception as e:
            logger.error(f"Kafka polling error: {e}. Retrying in 5s...")
            time.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: bootstrap cohort. Shutdown: cleanup."""
    global _bootstrapped
    logger.info("FastAPI starting up — bootstrapping cohort...")
    cfg = get_config()
    logger.info(cfg.report.table())
    summary = bootstrap_cohort()
    _bootstrapped = True
    logger.info(f"Bootstrap complete: {summary}")

    threading.Thread(target=kafka_polling_loop, daemon=True).start()

    async def broadcast_loop():
        while True:
            await asyncio.sleep(1)
            try:
                cohort_data = get_cohort()
                esc_data = get_escalations()
                await manager.broadcast({"type": "cohort_update", "data": cohort_data})
                await manager.broadcast({"type": "escalations_update", "data": esc_data})
            except Exception as e:
                logger.error(f"WS Broadcast error: {e}")

    asyncio.create_task(broadcast_loop())

    yield
    logger.info("FastAPI shutting down.")


app = FastAPI(
    title="Clinical Deterioration & Escalation Copilot",
    description="Agentic two-tiered clinical deterioration detection and escalation system.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="backend/static"), name="static")

@app.get("/", response_class=HTMLResponse)
def get_index():
    with open("backend/static/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial state
        await websocket.send_json({"type": "cohort_update", "data": get_cohort()})
        await websocket.send_json({"type": "escalations_update", "data": get_escalations()})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class ClinicianActionRequest(BaseModel):
    escalation_id: str
    action: str  # ACCEPT, DISMISS, DEFER_WATCH, INVESTIGATE
    clinician_id: str = "dr_m_smith"
    dismiss_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    """Startup health report with adapter status."""
    cfg = get_config()
    return cfg.report.to_dict()


@app.get("/api/cohort")
def get_cohort():
    """All patient twins with MEWS & urgency, ranked by urgency."""
    processor = get_processor()
    twins = processor.get_all_twins()
    result = []
    for twin in twins:
        data = twin.model_dump(mode="json")
        # Add computed fields
        if twin.current_vitals:
            data["map_value"] = twin.current_vitals.map_value
        if twin.current_mews:
            data["mews_total"] = twin.current_mews.total
        else:
            data["mews_total"] = 0
        result.append(data)

    # Sort by urgency: ESCALATION > WATCH > STABLE, then by MEWS desc
    urgency_order = {"ESCALATION": 0, "WATCH": 1, "STABLE": 2}
    result.sort(key=lambda x: (urgency_order.get(x.get("urgency", "STABLE"), 2), -x.get("mews_total", 0)))
    return {"cohort": result, "count": len(result)}


@app.get("/api/escalations")
def get_escalations():
    """Active and all escalations."""
    db = get_db()
    active = db.get_active_escalations()
    all_esc = db.get_all_escalations()
    return {
        "active": [e.model_dump(mode="json") for e in active],
        "all": [e.model_dump(mode="json") for e in all_esc],
        "active_count": len(active),
        "total_count": len(all_esc),
    }


@app.get("/api/audit")
def get_audit(patient_id: Optional[str] = None):
    """Immutable audit trail."""
    db = get_db()
    logs = db.get_audit_logs(patient_id)
    return {"audit_logs": [l.model_dump(mode="json") for l in logs], "count": len(logs)}


@app.get("/api/suppressions")
def get_suppressions():
    """Tier 1 suppression log."""
    db = get_db()
    logs = db.get_suppression_logs()
    processor = get_processor()
    return {
        "suppression_logs": [l.model_dump(mode="json") for l in logs],
        "total_suppressed": processor.suppressed_count,
    }


@app.get("/api/telemetry")
def get_telemetry():
    """Pipeline telemetry counters."""
    processor = get_processor()
    db = get_db()
    return {
        "monitored_patients": len(processor.get_all_twins()),
        "active_escalations": len(db.get_active_escalations()),
        "suppressed_artifacts": processor.suppressed_count,
        "total_escalations": processor.escalation_count,
        "pipeline_latency_ms": round(time.time() * 1000 % 100, 1),  # Simulated metric
    }


@app.get("/api/vitals/{patient_id}")
def get_vitals(patient_id: str):
    """Patient vitals history."""
    processor = get_processor()
    twin = processor.get_twin(patient_id)
    if not twin:
        raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")
    history = [v.model_dump(mode="json") for v in twin.vitals_history[-48:]]
    return {"patient_id": patient_id, "vitals": history, "count": len(history)}




@app.post("/api/clinician-action")
def clinician_action(req: ClinicianActionRequest):
    """Record clinician decision on an active escalation."""
    audit = process_clinician_action(
        escalation_id=req.escalation_id,
        action=req.action,
        clinician_id=req.clinician_id,
        dismiss_reason=req.dismiss_reason,
    )
    if not audit:
        raise HTTPException(status_code=404, detail=f"Escalation {req.escalation_id} not found or action failed")
    return {"status": "recorded", "audit": audit.model_dump(mode="json")}
