# Agentic Clinical Deterioration & Escalation Copilot

**Final Project Documentation**

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [System Architecture Overview](#3-system-architecture-overview)
4. [Component Deep Dive](#4-component-deep-dive)
   - 4.1. [Configuration & Infrastructure Resolution](#41-configuration--infrastructure-resolution)
   - 4.2. [Data Models (Pydantic v2 Schemas)](#42-data-models-pydantic-v2-schemas)
   - 4.3. [Tier 1: Stateful Stream Engine](#43-tier-1-stateful-stream-engine)
   - 4.4. [Tier 2: 5-Node LangGraph Copilot](#44-tier-2-5-node-langgraph-copilot)
   - 4.5. [RAG Engine (Qdrant Vector Retrieval)](#45-rag-engine-qdrant-vector-retrieval)
   - 4.6. [Database & Immutable Audit Trail](#46-database--immutable-audit-trail)
   - 4.7. [Patient Simulator (Kafka Producer)](#47-patient-simulator-kafka-producer)
   - 4.8. [FastAPI Backend & WebSocket Server](#48-fastapi-backend--websocket-server)
   - 4.9. [Frontend Dashboard (Vanilla JS)](#49-frontend-dashboard-vanilla-js)
5. [Clinical Knowledge Base](#5-clinical-knowledge-base)
6. [Patient Cohort](#6-patient-cohort)
7. [Clinician Action Workflow](#7-clinician-action-workflow)
8. [Verification Suite](#8-verification-suite)
9. [API Reference](#9-api-reference)
10. [Infrastructure Modes](#10-infrastructure-modes)
11. [Setup & Execution](#11-setup--execution)
12. [Directory Structure](#12-directory-structure)

---

## 1. Executive Summary

This project implements a **two-tiered agentic clinical deterioration detection and escalation system**. It is designed to address the critical problem of **alarm fatigue** in hospital settings — where clinicians are overwhelmed by a high volume of false or low-priority alerts from bedside monitors.

The system continuously ingests patient vital signs from a real-time Kafka stream, detects clinical deterioration patterns using a stateful stream processor, suppresses transient sensor artifacts, and generates structured SBAR (Situation-Background-Assessment-Recommendation) escalation narratives through a 5-node LangGraph pipeline. A deterministic **Numeric Verifier** node guarantees zero hallucinations in the final clinical output, ensuring that no ungrounded numeric value is ever presented to a clinician.

---

## 2. Problem Statement

In modern Intensive Care Units (ICUs) and Medical-Surgical wards, continuous physiological monitors generate thousands of alerts daily. Studies estimate that **85-99% of clinical alarms are false or non-actionable**, leading clinicians to develop alarm fatigue — a desensitization that can cause genuinely critical deterioration events to be missed.

This copilot addresses alarm fatigue through:
- **Tier 1 Intelligent Suppression:** A stream processing engine that uses multi-channel companion analysis to suppress single-channel artifacts (e.g., a probe displacement causing a transient SpO2 drop while HR, RR, and SBP remain stable).
- **Tier 2 Agentic Escalation:** When a sustained, multi-parameter clinical pattern is detected (e.g., concurrent tachycardia + hypotension sustained over 30 minutes), a LangGraph-based agent synthesizes a verified, protocol-grounded SBAR narrative for the clinician, rather than a raw numeric alert.

---

## 3. System Architecture Overview

The system is composed of three operational layers connected by event-driven bridges:

```
┌──────────────────────────────────────────────────────────────────┐
│               KAFKA (vitals_stream topic)                        │
│    patient_simulator/main.py publishes vital readings            │
└─────────────────────────────┬────────────────────────────────────┘
                              │ KafkaConsumer (api.py polling loop)
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                  TIER 1: Stream Engine                            │
│  ┌──────────┐  ┌──────────────┐  ┌────────────────────────────┐ │
│  │ Vital    │─▶│ MEWS Scoring │─▶│ Window Evaluator           │ │
│  │ Ingestion│  │ (4-Channel)  │  │ (Tumbling + Sliding)       │ │
│  └──────────┘  └──────────────┘  └──────────┬─────────────────┘ │
│                                              │                   │
│               ┌──────────────┐               │                   │
│               │ Artifact     │◀──────────────┤                   │
│               │ Suppression  │               │                   │
│               └──────────────┘               ▼                   │
│                           EscalationCandidateEvent               │
└───────────────────────────────┬──────────────────────────────────┘
                                │ Callback Bridge
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                  TIER 2: LangGraph Copilot                       │
│  ┌──────────────┐  ┌────────────────┐  ┌─────────────────────┐  │
│  │ 1. Context   │─▶│ 2. Risk        │─▶│ 3. Protocol         │  │
│  │ Hydration    │  │ Assessment     │  │ RAG Retrieval       │  │
│  └──────────────┘  └────────────────┘  └──────────┬──────────┘  │
│                                                    │             │
│  ┌──────────────────┐  ┌──────────────────────────┐│            │
│  │ 5. Numeric       │◀─│ 4. SBAR Synthesis        │◀┘           │
│  │ Verifier (Dumb)  │  │ (Template-Grounded)      │             │
│  └────────┬─────────┘  └──────────────────────────┘             │
│           │ ▲ Cyclic retry (max 2)                               │
│           │ └─── verification_errors ─────────┘                  │
│           ▼                                                      │
│    Verified StructuredAgentEscalation                            │
└───────────────────────────────┬──────────────────────────────────┘
                                │ WebSocket Broadcast
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│              Clinician Action Hub (Vanilla JS Dashboard)         │
│  Accept │ Dismiss │ Defer/Watch │ Investigate │ Resolve          │
│               ↓ Immutable Audit Trail (SQLite WAL)               │
└──────────────────────────────────────────────────────────────────┘
```

---

## 4. Component Deep Dive

### 4.1. Configuration & Infrastructure Resolution

**File:** `backend/config.py`

The `AppConfig` singleton resolves the runtime infrastructure mode at startup by reading the `INFRA_MODE` environment variable:

| Mode | Behavior |
|------|----------|
| `local` (default) | Uses SQLite WAL for persistence, in-process event broker (thread-safe `deque`), in-process state cache (`dict`), and embedded Qdrant for vector search. Zero external dependencies. |
| `docker` | Expects Redpanda (Kafka-compatible), Redis, TimescaleDB, and remote Qdrant containers. Connection strings are read from environment variables. |
| `auto` | Probes each Docker service endpoint with a 1.5-second TCP socket timeout. Falls back to `local` if any service is unreachable. |

On resolution, a `InfraReport` object is built containing adapter status for each component (Stream Broker, State Cache, Timeseries DB, Vector KB), which is exposed via the `/api/health` endpoint.

### 4.2. Data Models (Pydantic v2 Schemas)

**File:** `backend/models.py`

All domain objects are strictly typed using Pydantic v2 `BaseModel` classes:

| Model | Purpose |
|-------|---------|
| `VitalReading` | Single telemetry tick: HR, SBP, DBP, SpO2, RR, Temperature. Includes a computed `map_value` property (Mean Arterial Pressure). |
| `PatientStaticContext` | Demographics, diagnosis, comorbidities, immunocompromised flag, recent lab results, attending physician. |
| `MEWSScore` | Per-channel breakdown (rr_score, hr_score, sbp_score, spo2_score) and composite total. |
| `PatientTwin` | Digital twin combining static context + current vitals + history + MEWS history + urgency state + dynamic threshold multiplier + suppression rules. |
| `NumericClaim` | A single numeric assertion (value, unit, source dot-path) that the verifier validates. |
| `StructuredAgentEscalation` | Full escalation output: type, triggering signals, retrieved protocols, SBAR reasoning, recommended action, numeric claims, verification status, retry count, agent trace. |
| `EscalationCandidateEvent` | Bridge event from Tier 1 → Tier 2 containing patient ID, trigger type, triggering vitals snapshot, MEWS score, and priority modifiers. |
| `SuppressionEvent` | Logged when Tier 1 suppresses a transient artifact. |
| `AuditLogEntry` | Immutable record of every clinician decision and the agent reasoning that led to it. |

**Enums:**
- `ClinicalUrgency`: STABLE → WATCH → ESCALATION
- `EscalationType`: Hemodynamic_Sepsis, Respiratory_Hypoxia, MEWS_High, MEWS_Trend
- `EscalationStatus`: PENDING, ACCEPTED, RESOLVED, DISMISSED, DEFERRED_WATCH, INVESTIGATED, VERIFICATION_BLOCKED

### 4.3. Tier 1: Stateful Stream Engine

**File:** `backend/stream_engine.py`

The `StatefulWindowProcessor` is the heart of Tier 1. It implements Apache Flink-style stateful tumbling and sliding window semantics in pure Python:

**MEWS Scoring (4-Channel Lookup):**

Each vital reading is scored independently across four channels using the authoritative MEWS lookup table defined in `knowledge_base/mews_guidelines.md`:

| Channel | Score 0 | Score 1 | Score 2 | Score 3 |
|---------|---------|---------|---------|---------|
| RR (breaths/min) | 9–14 | 15–20 | <9 or 21–29 | ≥30 |
| HR (bpm) | 51–100 | 41–50 or 101–110 | ≤40 or 111–129 | ≥130 |
| SBP (mmHg) | 101–199 | 81–100 | ≤70–80 or ≥200 | ≤70 |
| SpO2 (%) | ≥96 | 94–95 | 90–93 | <90 |

Composite MEWS = sum of all four channel scores.

**Ingestion Pipeline (per-tick):**
1. Compute MEWS from the incoming `VitalReading`.
2. Append to per-patient sliding window (max 360 readings) and MEWS window (max 72 entries).
3. Update the `PatientTwin` with current vitals, MEWS, and trend delta (current MEWS vs. 60 minutes ago).
4. **Artifact Suppression Check:** If SpO2 < 90 but companion channels (HR, RR, SBP) remain stable vs. the previous reading AND previous SpO2 was ≥ 90, classify as transient artifact and suppress without escalation. A `SuppressionEvent` is published to the in-process broker.
5. **Clinician Suppression Check:** If the patient has active `baseline_suppression` rules (set after a DISMISS action), suppress if vitals are within normal baseline ranges.
6. **Escalation Rule Evaluation (4 rules):**
   - **Rule 1 (Sepsis):** HR > 120 AND (MAP < 70 OR SBP ≤ 90) sustained over ≥ 2 readings in a 30-minute window → `HEMODYNAMIC_SEPSIS`
   - **Rule 2 (Hypoxia):** SpO2 < 90 AND RR ≥ 26 sustained over ≥ 2 readings in a 30-minute window → `RESPIRATORY_HYPOXIA`
   - **Rule 3 (High MEWS):** MEWS ≥ 5 across ≥ 2 consecutive windows → `MEWS_HIGH`
   - **Rule 4 (MEWS Trend):** MEWS trend delta ≥ 3 (adjusted by `dynamic_threshold_multiplier`) → `MEWS_TREND`
7. If an escalation rule fires, build an `EscalationCandidateEvent` and invoke the registered Tier 2 callback.

**Supporting Components:**
- `InProcessBroker`: Thread-safe pub/sub event broker using `deque` (replaces Redpanda in local mode).
- `InProcessCache`: Thread-safe key-value store using `dict` (replaces Redis in local mode).

### 4.4. Tier 2: 5-Node LangGraph Copilot

**File:** `backend/langgraph_copilot.py`

The Tier 2 pipeline is a cyclic 5-node state graph that produces a `StructuredAgentEscalation` from an `EscalationCandidateEvent`:

**Node 1 — Context Hydration:**
Fetches the full `PatientTwin` (demographics, comorbidities, immunocompromised status, recent labs, 4-hour vitals history) from the stream processor's in-memory store, with a database fallback.

**Node 2 — Risk Assessment:**
Classifies the failing organ system (`Hemodynamic_Sepsis` or `Respiratory_Hypoxia`) based on the trigger type. Computes trajectory slopes across the vitals history (delta between first and last readings for HR, SBP, SpO2, RR).

**Node 3 — Protocol RAG Retrieval:**
Queries the Qdrant vector database with a clinical keyword query tailored to the organ system (e.g., *"sepsis hemodynamic tachycardia hypotension..."* or *"hypoxia respiratory failure oxygen saturation..."*). Returns the top-3 matching protocol chunks and their filenames.

**Node 4 — SBAR Synthesis:**
Generates a deterministic, template-grounded SBAR narrative. Every numeric value in the output is derived directly from the patient's actual vitals, lab results, and protocol thresholds. Builds a list of `NumericClaim` objects that declare every numeric assertion (with its source dot-path) for downstream verification.

**Node 5 — Deterministic Numeric Verifier:**
The "dumb" verifier enforces two layers of safety:

*Primary Check:* For each `NumericClaim`, resolves the source path (e.g., `current_vitals.hr`) against a ground-truth lookup table built from the patient's actual data. Asserts `abs(claimed_value - actual_value) <= 0.01`. Any mismatch is a verification error.

*Secondary Guardrail (Prose Scan):* Strips non-clinical identifiers (patient IDs, bed labels, timestamps, age references, protocol version numbers) from the SBAR text. Extracts all remaining numeric literals via regex. Asserts each is either declared in the `NumericClaim` list or exists in the patient context / protocol threshold whitelist. Undeclared numbers trigger a verification error.

**Cyclic Retry:** On verification failure with `retry_count < 2`, the graph routes back to Node 4 (SBAR Synthesis), which removes the offending claims and regenerates the text. If retries are exhausted, the escalation is blocked with status `VERIFICATION_BLOCKED` and is NOT presented to the clinician.

### 4.5. RAG Engine (Qdrant Vector Retrieval)

**File:** `backend/rag_engine.py`

The `RAGEngine` indexes all Markdown files in the `knowledge_base/` directory into a Qdrant vector collection:

- **Encoding:** Uses a zero-dependency TF-IDF-style encoding over a curated 60-term clinical vocabulary (e.g., *sepsis, hemodynamic, tachycardia, hypotension, lactate, spo2...*). Each document chunk is converted to a 60-dimensional L2-normalized frequency vector. No external embedding API is required.
- **Chunking:** Documents are split into 500-word overlapping chunks (100-word overlap) for granular retrieval.
- **Retrieval:** Queries are encoded using the same vocabulary and matched via cosine similarity in Qdrant. Falls back to keyword-overlap scoring if Qdrant is unavailable.
- **Modes:** Supports both embedded Qdrant (local mode, stored at `data/qdrant_db/`) and remote Qdrant (Docker mode).

### 4.6. Database & Immutable Audit Trail

**File:** `backend/database.py`

The `DatabaseManager` uses a unified adapter interface backed by `SQLiteAdapter` (WAL mode):

**Tables:**

| Table | Purpose | Constraints |
|-------|---------|-------------|
| `patient_twins` | Current digital twin state (JSON) | PK: `patient_id`, `CHECK(json_valid())` |
| `vitals_history` | Historical vital readings (JSON) | FK → `patient_twins` |
| `escalations` | All escalations with status | PK: `escalation_id`, FK → `patient_twins` |
| `immutable_audit_log` | Clinician decisions + agent reasoning | `UNIQUE(event_id)`, **immutability enforced** |
| `suppression_log` | Tier 1 artifact suppression events | `UNIQUE(event_id)` |

**Immutability Enforcement:**
Two SQLite triggers (`enforce_audit_no_update`, `enforce_audit_no_delete`) prevent any `UPDATE` or `DELETE` operations on the `immutable_audit_log` table. This ensures that once a clinician decision is recorded, it cannot be retroactively modified — a requirement for healthcare compliance.

The `test_audit_immutability()` method programmatically verifies that these triggers are active by attempting an update and delete and asserting both are blocked.

### 4.7. Patient Simulator (Kafka Producer)

**File:** `patient_simulator/main.py`

An autonomous process that runs independently of the backend, continuously publishing vital signs to the `vitals_stream` Kafka topic:

- **Baseline Mode:** Generates physiologically realistic vitals for all 5 patients with small random jitter (±2 bpm HR, ±3 mmHg SBP, etc.).
- **Spike Mode:** Every ~15 seconds (8 ticks × 2s interval), triggers a random deterioration spike on a random patient:
  - **Sepsis spike:** HR → 125 bpm, SBP → 85 mmHg, DBP → 55 mmHg
  - **Hypoxia spike:** SpO2 → 85%, RR → 28 breaths/min
- **Recovery:** After ~6 seconds (3 ticks), the patient automatically returns to baseline.
- **Retry Logic:** Handles Kafka broker unavailability with a 3-second retry loop.

### 4.8. FastAPI Backend & WebSocket Server

**File:** `backend/api.py`

The FastAPI application serves as the central hub:

- **Lifespan Startup:** Bootstraps the patient cohort (seeds 5 patients with static context, 4-hour baseline vitals, and runs the stream engine), starts the Kafka polling loop in a background daemon thread, and launches a WebSocket broadcast loop.
- **Kafka Consumer Thread:** Continuously polls the `vitals_stream` topic, deserializes each message into a `VitalReading`, ingests it through the stream engine, and persists to the database.
- **WebSocket Broadcast:** Every 1 second, broadcasts the current cohort state and escalation data to all connected WebSocket clients, enabling real-time dashboard updates without polling.
- **Static File Serving:** Mounts `backend/static/` for the Vanilla JS frontend, serving `index.html` at the root path `/`.

### 4.9. Frontend Dashboard (Vanilla JS)

**Files:** `backend/static/index.html`, `backend/static/app.js`, `backend/static/style.css`, `backend/static/treatments.html`

A custom-built, single-page clinical dashboard using Vanilla JavaScript, Chart.js, and CSS:

**Main Dashboard (`index.html`):**
- **Top Navigation Bar:** Displays live telemetry (cohort count, active escalation count, LIVE status indicator with CSS pulse animation).
- **Cohort Priority Ranking Table:** All patients sorted by urgency (ESCALATION > WATCH > STABLE), displaying patient ID, bed, urgency badge (color-coded), MEWS score, HR, BP (MAP), SpO2, RR. Rows are clickable.
- **Patient Detail Panel:** Four Chart.js line charts (Heart Rate, Blood Pressure, SpO2, Respiratory Rate) rendering the last 48 vitals readings for the selected patient.
- **Escalation Cards:** Active PENDING escalations for the selected patient, showing the full SBAR reasoning, recommended action, and an "Accept & Clear" button.

**Treatments Page (`treatments.html`):**
- Displays ACCEPTED escalations that are currently under active treatment, with a "Resolve & Return to Stable" button.

**Real-Time Updates:**
- Connects via WebSocket (`/ws`) with automatic reconnection on disconnect (2-second backoff).
- Receives `cohort_update` and `escalations_update` messages from the backend broadcast loop.
- All UI updates are driven by incoming WebSocket messages — no HTTP polling.

**Design System:**
- Dark clinical theme (`#0B0F19` background, `#111827` panels).
- Inter font family from Google Fonts.
- Color-coded urgency badges: red for ESCALATION, amber for WATCH, green for STABLE.

---

## 5. Clinical Knowledge Base

**Directory:** `knowledge_base/`

Three Markdown protocol documents are indexed into the Qdrant vector database at startup:

| File | Content |
|------|---------|
| `sepsis_protocol_v2.md` | Sepsis & hemodynamic escalation trigger criteria (HR > 120 + MAP < 70 / SBP ≤ 90 over 30-min window), priority modifiers (immunocompromised, lactate > 2.0), MAP calculation formula, canonical example (pt_30045), recommended clinical actions (blood cultures, fluid resuscitation, antibiotics), SBAR template. |
| `hypoxia_protocol_v1.md` | Progressive hypoxia trigger criteria (SpO2 < 90 + RR ≥ 26 over 15-30 min window), artifact suppression rule (single-channel SpO2 dip with stable companions), clinical presentation patterns, recommended actions (increase O2, ABG, BiPAP/CPAP), SBAR template. |
| `mews_guidelines.md` | Authoritative 4-channel MEWS scoring table, composite calculation formula, MAP formula, MEWS trend delta definition, all four Tier 1 escalation trigger rules, dynamic threshold adjustment for DEFER_WATCH, tumbling vs. sliding window semantics. |

---

## 6. Patient Cohort

The system is pre-seeded with a 5-patient cohort representing diverse clinical scenarios:

| Patient ID | Bed | Age/Sex | Primary Diagnosis | Comorbidities | Role |
|-----------|-----|---------|-------------------|---------------|------|
| `pt_30045` | ICU-04 | 67/M | Community-acquired pneumonia, R/O sepsis | T2DM, CKD Stage III, Immunocompromised | Sepsis trajectory target. Lactate 2.8 mmol/L. |
| `pt_20419` | MedSurg-12 | 54/F | Acute COPD exacerbation | COPD Stage III, Hypertension | Hypoxia trajectory target. pH 7.35, pCO2 48. |
| `pt_10882` | MedSurg-07 | 45/M | Post-op appendectomy recovery | None | Artifact suppression target. |
| `pt_40291` | ICU-08 | 72/F | Acute decompensated heart failure | CHF NYHA III, AFib, DM | Stable monitoring (BNP 890). |
| `pt_50163` | StepDown-03 | 38/M | Traumatic brain injury (GCS 13) | None | Stable monitoring. |

---

## 7. Clinician Action Workflow

When a verified escalation is presented on the dashboard, the clinician can take one of four actions:

| Action | System Effect |
|--------|---------------|
| **ACCEPT** | Escalation status → `ACCEPTED`. Patient moves to Treatments page. Urgency reverts to STABLE (unless other active escalations exist). |
| **DISMISS** | Escalation status → `DISMISSED`. Adds `baseline_suppression` rule to the patient twin, preventing re-fire for stable vitals. Suppression updated flag set in audit. |
| **DEFER_WATCH** | Escalation status → `DEFERRED_WATCH`. Sets `dynamic_threshold_multiplier` to 0.85, tightening the MEWS trend delta re-trigger threshold. Patient urgency set to WATCH. |
| **RESOLVE** | Escalation status → `RESOLVED`. Patient returns to STABLE. Used from the Treatments page after successful intervention. |

Every action appends a new **immutable audit record** containing the triggering vitals, agent reasoning, retrieved protocols, clinician ID, decision, and suppression flag.

---

## 8. Verification Suite

**File:** `verify_system.py`

A 66-test automated E2E verification suite that validates every row of the acceptance matrix using the actual production code paths (no mocks):

| Scenario | Tests |
|----------|-------|
| **MEWS Scoring** | Validates all 20 individual channel score boundaries + composite MEWS + MAP calculation. |
| **Scenario 1: Startup Bootstrap** | 5 patients seeded, escalation fired for pt_30045, correct urgency, active escalation count, numeric verification passed, SBAR mentions HR/SBP thresholds, does NOT falsely claim MAP < 70, triggering vitals correct, sepsis protocol retrieved, initial audit is PENDING. |
| **Scenario 2: Artifact Suppression** | SpO2 artifact on pt_10882 is suppressed, no escalation fired, suppressed count incremented, no new escalation created. |
| **Scenario 3: Progressive Hypoxia** | Hypoxia escalation fired for pt_20419, saved to DB, numerically verified, hypoxia protocol retrieved, reasoning mentions SpO2 and RR thresholds. |
| **Scenario 4: Verifier Guardrail** | Injects HR=165 bad claim (actual=122), verifier catches mismatch, triggers cyclic retry, corrected escalation produced, verification passes after correction. |
| **Scenario 5: Clinician ACCEPT** | Audit record created, decision is ACCEPT, clinician ID correct, suppression NOT updated, escalation status is ACCEPTED. |
| **Scenario 6: Clinician DISMISS** | Audit record created, decision is DISMISS, suppression updated, baseline_suppression rule activated, repeat stable tick does NOT re-fire. |
| **Scenario 7: Clinician DEFER_WATCH** | Audit record created, decision is DEFER_WATCH, threshold multiplier set to 0.85, patient urgency is WATCH. |
| **Immutability Test** | UPDATE and DELETE on `immutable_audit_log` are both blocked by SQLite triggers. |

---

## 9. API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Serves the main HTML dashboard. |
| `GET` | `/api/health` | Returns infrastructure health report with adapter statuses. |
| `GET` | `/api/cohort` | Returns all patient twins sorted by urgency (ESCALATION > WATCH > STABLE), then by MEWS descending. |
| `GET` | `/api/escalations` | Returns active (PENDING) and all escalations with counts. |
| `GET` | `/api/audit?patient_id=` | Returns immutable audit trail, optionally filtered by patient ID. |
| `GET` | `/api/suppressions` | Returns Tier 1 suppression log and total suppressed count. |
| `GET` | `/api/telemetry` | Returns pipeline telemetry: monitored patients, active escalations, suppressed artifacts, total escalations. |
| `GET` | `/api/vitals/{patient_id}` | Returns last 48 vitals readings for a patient. |
| `POST` | `/api/clinician-action` | Records a clinician decision. Body: `{escalation_id, action, clinician_id, dismiss_reason}`. |
| `WS` | `/ws` | WebSocket endpoint for real-time cohort and escalation updates. |

---

## 10. Infrastructure Modes

| Component | Local Mode | Docker Mode |
|-----------|-----------|-------------|
| Stream Broker | In-process `deque` with pub/sub | Redpanda (Kafka-compatible) on port 19092 |
| State Cache | In-process `dict` | Redis on port 6379 |
| Timeseries/Audit DB | SQLite WAL (`data/clinical_copilot.db`) | TimescaleDB (PostgreSQL) on port 5432 |
| Vector KB | Embedded Qdrant (`data/qdrant_db/`) | Remote Qdrant on ports 6333/6334 |

The Docker services are defined in `docker-compose.yml` and can be launched with `docker-compose up -d`.

---

## 11. Setup & Execution

**Prerequisites:** Python 3.10+, `uv` package manager, Kafka broker (Redpanda via Docker).

```bash
# 1. Clone and enter the repository
git clone https://github.com/MritunjayTiwari14/intra_iit_hp.git
cd intra_iit_hp

# 2. Copy environment variables
cp .env.example .env

# 3. Install dependencies
uv sync

# 4. Start Docker services (Redpanda for Kafka)
docker-compose up -d

# 5. Run the verification suite (66/66 tests)
uv run python verify_system.py

# 6. Start the autonomous patient simulator (Terminal 1)
uv run python patient_simulator/main.py

# 7. Start the backend + dashboard server (Terminal 2)
uv run python run.py
```

The real-time dashboard will be available at `http://localhost:8080`.

---

## 12. Directory Structure

```
intra_iit_hp/
├── .env.example                   # Environment variable template
├── .gitignore                     # Git exclusions (venv, data, PDFs, caches)
├── docker-compose.yml             # Redpanda, Redis, TimescaleDB, Qdrant containers
├── pyproject.toml                 # Project metadata and dependencies (uv)
├── uv.lock                        # Locked dependency versions
├── run.py                         # Single-command FastAPI launcher
├── verify_system.py               # 66-test E2E verification suite
│
├── knowledge_base/                # Clinical protocol documents (indexed by RAG)
│   ├── sepsis_protocol_v2.md      # Sepsis trigger rules, SBAR template, canonical example
│   ├── hypoxia_protocol_v1.md     # Hypoxia trigger rules, artifact suppression logic
│   └── mews_guidelines.md         # MEWS scoring table, escalation rules, window semantics
│
├── patient_simulator/             # Autonomous Kafka-based vitals producer
│   └── main.py                    # Generates randomized vitals with periodic spikes
│
├── backend/
│   ├── __init__.py
│   ├── config.py                  # Infrastructure mode resolution (auto/local/docker)
│   ├── models.py                  # Pydantic v2 schemas (11 models, 3 enums)
│   ├── database.py                # SQLite WAL adapter with immutability triggers
│   ├── stream_engine.py           # Tier 1: MEWS scoring, artifact suppression, escalation rules
│   ├── rag_engine.py              # Qdrant vector indexing and clinical protocol retrieval
│   ├── langgraph_copilot.py       # Tier 2: 5-node LangGraph with cyclic numeric verifier
│   ├── simulator.py               # Cohort bootstrap, baseline generation, scenario methods
│   ├── api.py                     # FastAPI server, Kafka consumer, WebSocket broadcast
│   └── static/
│       ├── index.html             # Main dashboard (cohort table, charts, escalation cards)
│       ├── app.js                 # WebSocket client, Chart.js rendering, clinician actions
│       ├── style.css              # Dark clinical theme (Inter font, #0B0F19 palette)
│       └── treatments.html        # Active treatments management page
│
└── data/                          # Runtime data (git-ignored)
    ├── clinical_copilot.db        # SQLite WAL database
    └── qdrant_db/                 # Embedded Qdrant vector storage
```
