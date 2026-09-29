# Agentic Clinical Deterioration & Escalation Copilot

## Overview

A two-tiered agentic clinical deterioration detection and escalation system built for healthcare hackathon demonstration. The system continuously monitors patient vital signs, detects clinical deterioration patterns using stateful stream processing, and generates structured escalation recommendations through a 5-node LangGraph pipeline with deterministic numeric verification.

> **⚠️ Hackathon Demonstration System** — This system is architected to demonstrate the two-tiered design and is not intended for deployment in real clinical care.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                    TIER 1: Stream Engine                        │
│  ┌──────────┐   ┌──────────────┐   ┌───────────────────────┐  │
│  │ Vital    │──▶│ MEWS Scoring │──▶│ Window Evaluator      │  │
│  │ Ingestion│   │ (4-Channel)  │   │ (Tumbling + Sliding)  │  │
│  └──────────┘   └──────────────┘   └───────────┬───────────┘  │
│                                                 │              │
│                  ┌──────────────┐               │              │
│                  │ Artifact     │◀──────────────┤              │
│                  │ Suppression  │               │              │
│                  └──────────────┘               ▼              │
│                              EscalationCandidateEvent          │
└────────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────────────┐
│                    TIER 2: LangGraph Copilot                   │
│  ┌──────────────┐   ┌────────────────┐   ┌────────────────┐  │
│  │ 1. Context   │──▶│ 2. Risk        │──▶│ 3. Protocol    │  │
│  │ Hydration    │   │ Assessment     │   │ RAG Retrieval  │  │
│  └──────────────┘   └────────────────┘   └───────┬────────┘  │
│                                                   │           │
│  ┌──────────────────┐   ┌───────────────────────┐│           │
│  │ 5. Numeric       │◀──│ 4. SBAR Synthesis     │◀┘          │
│  │ Verifier (Dumb)  │   │ (Template-Grounded)   │            │
│  └────────┬─────────┘   └───────────────────────┘            │
│           │ ▲ Cyclic retry (max 2)                            │
│           │ └─── verification_errors ──────────┘              │
│           ▼                                                    │
│    Verified StructuredAgentEscalation                         │
└────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────────────┐
│              Clinician Action Hub (Streamlit)                  │
│  Accept │ Dismiss │ Defer/Watch │ Investigate                  │
│              ↓ Immutable Audit Trail (SQLite/PG)              │
└────────────────────────────────────────────────────────────────┘
```

## PyFlink Design Note

The stream processing engine (`backend/stream_engine.py`) implements **Flink-style stateful tumbling and sliding window semantics in pure Python**. This is the local hackathon substitute for Apache Flink (PyFlink), designed for zero-config execution without JVM dependencies. The `StatefulWindowProcessor` maintains per-patient sliding windows and evaluates multi-parameter protocol breach rules at each ingestion tick. In a production deployment, this would be replaced with a PyFlink job consuming from Redpanda/Kafka topics.

---

## Quick Start

### Prerequisites
- Python 3.10+
- pip

### Installation

```bash
# Initialize and sync uv environment from pyproject.toml
uv sync
```

### Copy environment file
```bash
cp .env.example .env
```

### Verify system (run tests)
```bash
uv run python verify_system.py
```

### Launch the application
```bash
uv run python run.py
```

This starts:
- **FastAPI Backend** at `http://localhost:8000`
- **Streamlit Dashboard** at `http://localhost:8501`

---

## Infrastructure Modes

| Mode | Description | Services |
|------|-------------|----------|
| `local` (default) | Zero-config local adapters | SQLite WAL, in-process broker, embedded Qdrant |
| `docker` | Container services | Redpanda, Redis, TimescaleDB, Qdrant |
| `auto` | Auto-detect | Probes Docker endpoints, falls back to local |

Set `INFRA_MODE` in `.env` or environment variable.

For Docker mode:
```bash
docker-compose up -d
INFRA_MODE=docker python run.py
```

---

## Demo Scenarios

Use the sidebar buttons in the Streamlit dashboard:

| Button | What it does |
|--------|-------------|
| Step Live Vitals Tick | Advances one stable tick for all patients |
| SpO2 Artifact | Injects transient artifact on pt_10882 → Tier 1 suppresses |
| Sepsis Trajectory | Runs sepsis deterioration on pt_30045 → Tier 2 LangGraph |
| Hypoxia Trajectory | Runs hypoxia deterioration on pt_20419 → Tier 2 LangGraph |
| Test Verifier Guardrail | Injects HR=165 bad claim → cyclic retry self-correction |
| Reset Cohort | Clears all data and replays startup pipeline |

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Infrastructure health report |
| GET | `/api/cohort` | Patient twins ranked by urgency |
| GET | `/api/escalations` | Active and historical escalations |
| GET | `/api/audit` | Immutable audit trail |
| GET | `/api/suppressions` | Tier 1 suppression log |
| GET | `/api/telemetry` | Pipeline telemetry counters |
| GET | `/api/vitals/{pid}` | Patient vitals history |
| POST | `/api/scenarios/{name}` | Trigger demo scenarios |
| POST | `/api/clinician-action` | Record clinician decision |

---

## Patient Cohort

| Patient ID | Bed | Diagnosis | Role |
|-----------|-----|-----------|------|
| pt_30045 | ICU-04 | Community-acquired pneumonia | Sepsis trajectory demo |
| pt_20419 | MedSurg-12 | COPD exacerbation | Hypoxia trajectory demo |
| pt_10882 | MedSurg-07 | Post-op appendectomy | Artifact suppression demo |
| pt_40291 | ICU-08 | Decompensated heart failure | Stable monitoring |
| pt_50163 | StepDown-03 | Traumatic brain injury | Stable monitoring |

---

## Testing

```bash
# Run full verification suite
python verify_system.py

# Run with pytest
python -m pytest verify_system.py -v
```

---

## Directory Structure

```
├── docker-compose.yml          # Container services
├── requirements.txt            # Dependencies
├── .env.example                # Environment template
├── knowledge_base/             # Clinical protocol documents
│   ├── sepsis_protocol_v2.md
│   ├── hypoxia_protocol_v1.md
│   └── mews_guidelines.md
├── backend/
│   ├── config.py               # INFRA_MODE resolution
│   ├── models.py               # Pydantic v2 schemas
│   ├── database.py             # Dual-mode DB layer
│   ├── stream_engine.py        # Tier 1: Stateful window processor
│   ├── rag_engine.py           # Vector KB retrieval
│   ├── langgraph_copilot.py    # Tier 2: 5-node LangGraph
│   ├── simulator.py            # Synthetic vitals generator
│   └── api.py                  # FastAPI server
├── frontend/
│   └── dashboard.py            # Streamlit dashboard
├── verify_system.py            # E2E verification suite
└── run.py                      # Single-command launcher
```
