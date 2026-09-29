# Agentic Clinical Deterioration & Escalation Copilot
**Final Project Documentation**

## 1. Executive Summary
This project implements a robust, two-tiered agentic clinical deterioration detection and escalation system. Built for healthcare, the system continuously monitors patient vital signs from a real-time Kafka stream, detects clinical deterioration patterns using stateful stream processing, and generates structured escalation recommendations through a 5-node LangGraph pipeline with deterministic numeric verification.

The system is fully modular, highly testable, and deterministic, ensuring zero hallucinations in critical medical alerts through a strict numeric verification node.

## 2. System Architecture
The application architecture consists of three main components: Tier 1 (Stream Engine), Tier 2 (LangGraph Copilot), and a Real-Time Frontend Dashboard. They communicate seamlessly to process high-throughput vitals streams and escalate safely.

### 2.1. Vitals Simulation & Kafka
An autonomous `patient_simulator` acts as the source of truth, continuously generating live vital signs for a cohort of patients. It publishes these telemetry events to a Kafka topic (`vitals_stream`). The simulator intelligently injects stable jitter and occasionally triggers random Sepsis or Hypoxia deterioration trajectories.

### 2.2. Tier 1: Stream Engine
Tier 1 acts as a Flink-style stateful tumbling and sliding window processor implemented in pure Python. It consumes from Kafka and evaluates multi-parameter clinical protocols on the fly. Key features include:
* **Real-time Ingestion:** Calculates Modified Early Warning Score (MEWS) iteratively upon receiving vitals.
* **Artifact Suppression:** Identifies transient sensor drops (e.g., brief single-channel SpO2 dips) and suppresses false alarms.
* **Pattern Recognition:** Fires an `EscalationCandidateEvent` when sustained multi-parameter protocols (like Sepsis or Hypoxia) are breached.

### 2.3. Tier 2: Agentic LangGraph Copilot
Upon an escalation trigger, Tier 2 synthesizes a clinical narrative and retrieves relevant protocols using an agentic Graph logic:
1. **Node 1: Context Hydration** - Gathers recent patient baseline and static context.
2. **Node 2: Risk Assessment** - Classifies the failing organ system.
3. **Node 3: Protocol RAG** - Retrieves deterministic treatment guidelines via a vector database (Qdrant).
4. **Node 4: SBAR Synthesis** - Generates a structured Situation-Background-Assessment-Recommendation (SBAR) narrative.
5. **Node 5: Numeric Verifier** - Enforces strict extraction guardrails. Any hallucinated numeric claim triggers a cyclic retry to Node 4, completely blocking ungrounded escalations.

## 3. Data Models and Technologies
The project heavily leverages modern Python typing and infrastructure:
* **Kafka:** Real-time event streaming broker for high-throughput vitals telemetry.
* **Pydantic:** Ensures strict schema validation for Patient Twins, Vital Readings, and Escalations.
* **FastAPI:** Serves asynchronous endpoints and WebSockets for real-time frontend updates.
* **Qdrant:** Used as a local vector database to map clinical deterioration signatures to markdown protocols.
* **Vanilla JS/HTML:** Provides a sleek, custom-built frontend dashboard served statically via FastAPI for real-time Clinician Action and Monitoring.

## 4. User Interfaces and Audit Trail
The frontend Clinician Action Hub updates in real-time via WebSockets, allowing medical professionals to view live metrics, active SBAR escalations, and patient urgency rankings. Actions (ACCEPT, DISMISS, DEFER) interact with the backend to append rows to the SQLite Database.

A strict immutability test is applied to the Audit Trail. No logs can be mutated or deleted post-creation, simulating a reliable and auditable healthcare event system.

## 5. Setup and Execution
To run the project, ensure `uv` (Python 3.10+) is installed. You must also have Kafka running locally or via Docker.

1. Start your Kafka broker.
2. Copy environment variables: `cp .env.example .env`
3. Sync dependencies: `uv sync`
4. Start the autonomous simulator: `uv run python patient_simulator/main.py`
5. In a separate terminal, launch the backend and dashboard: `uv run python run.py`

> **Note:** The real-time dashboard is available at `http://localhost:8080`.
