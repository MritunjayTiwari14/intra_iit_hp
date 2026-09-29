# Agentic Clinical Deterioration & Escalation Copilot
**Final Project Documentation**

## 1. Executive Summary
This project implements a robust, two-tiered agentic clinical deterioration detection and escalation system. Built for healthcare, the system continuously monitors patient vital signs, detects clinical deterioration patterns using stateful stream processing, and generates structured escalation recommendations through a 5-node LangGraph pipeline with deterministic numeric verification.

The system is fully modular, highly testable, and deterministic, ensuring zero hallucinations in critical medical alerts through a strict numeric verification node.

## 2. System Architecture
The application architecture consists of three main components: Tier 1 (Stream Engine), Tier 2 (LangGraph Copilot), and a Frontend Dashboard. They communicate seamlessly to process high-throughput vitals streams and escalate safely.

### 2.1. Tier 1: Stream Engine
Tier 1 acts as a Flink-style stateful tumbling and sliding window processor implemented in pure Python. It evaluates multi-parameter clinical protocols on the fly. Key features include:
* **Real-time Ingestion:** Calculates Modified Early Warning Score (MEWS) iteratively upon receiving vitals.
* **Artifact Suppression:** Identifies transient sensor drops (e.g., brief single-channel SpO2 dips) and suppresses false alarms.
* **Pattern Recognition:** Fires an `EscalationCandidateEvent` when sustained multi-parameter protocols (like Sepsis or Hypoxia) are breached.

### 2.2. Tier 2: Agentic LangGraph Copilot
Upon an escalation trigger, Tier 2 synthesizes a clinical narrative and retrieves relevant protocols using an agentic Graph logic:
1. **Node 1: Context Hydration** - Gathers recent patient baseline and static context.
2. **Node 2: Risk Assessment** - Classifies the failing organ system.
3. **Node 3: Protocol RAG** - Retrieves deterministic treatment guidelines via a vector database (Qdrant).
4. **Node 4: SBAR Synthesis** - Generates a structured Situation-Background-Assessment-Recommendation (SBAR) narrative.
5. **Node 5: Numeric Verifier** - Enforces strict extraction guardrails. Any hallucinated numeric claim triggers a cyclic retry to Node 4, completely blocking ungrounded escalations.

## 3. Data Models and Technologies
The project heavily leverages modern Python typing and infrastructure:
* **Pydantic:** Ensures strict schema validation for Patient Twins, Vital Readings, and Escalations.
* **FastAPI:** Serves asynchronous endpoints for telemetry, cohort management, and clinical actions.
* **Qdrant:** Used as a local vector database to map clinical deterioration signatures to markdown protocols.
* **Streamlit:** Provides a dark-themed, interactive frontend Clinician Action Hub and Immutable Audit Trail.

## 4. User Interfaces and Audit Trail
The frontend Clinician Action Hub allows medical professionals to view live metrics, active SBAR escalations, and patient vital trend histories. Actions (ACCEPT, DISMISS, DEFER) interact with the backend to append rows to the SQLite Database.

A strict immutability test is applied to the Audit Trail. No logs can be mutated or deleted post-creation, simulating a reliable and auditable healthcare event system.

## 5. Setup and Execution
To run the project, ensure `uv` (Python 3.10+) is installed.

1. Copy environment variables: `cp .env.example .env`
2. Sync dependencies: `uv sync`
3. Run tests: `uv run python verify_system.py`
4. Launch servers: `uv run python run.py`

> **Note:** The FastAPI backend runs on port 8080, and the Streamlit dashboard runs on port 8501.
