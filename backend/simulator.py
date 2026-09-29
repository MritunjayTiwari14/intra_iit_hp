"""
simulator.py — Synthetic vitals stream generator & live pipeline startup bootstrapper.

Seeds 5 patients with static context and 4-hour baseline history, then advances
the live pipeline for pt_30045's sepsis trajectory ticks. Tier 1's window evaluator
naturally detects the breach, fires EscalationCandidateEvent, and runs the full
Tier 2 LangGraph pipeline — producing the initial active escalation visible in the UI
through the actual production code path (no hardcoded escalation rows).
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from backend.models import (
    ClinicalUrgency,
    EscalationCandidateEvent,
    EscalationType,
    LabResult,
    PatientStaticContext,
    PatientTwin,
    VitalReading,
)
from backend.stream_engine import get_processor, compute_mews
from backend.langgraph_copilot import run_tier2_pipeline
from backend.database import get_db

logger = logging.getLogger("copilot.simulator")

# ---------------------------------------------------------------------------
# Patient cohort definitions
# ---------------------------------------------------------------------------

PATIENT_COHORT = [
    {
        "patient_id": "pt_30045",
        "age": 67,
        "sex": "Male",
        "bed": "ICU-04",
        "primary_dx": "Community-acquired pneumonia, rule out sepsis",
        "comorbidities": ["Type 2 Diabetes", "Chronic Kidney Disease Stage III"],
        "immunocompromised": True,
        "recent_labs": [
            {"name": "Lactate", "value": 2.8, "unit": "mmol/L"},
            {"name": "WBC", "value": 14.2, "unit": "x10^9/L"},
            {"name": "Creatinine", "value": 1.6, "unit": "mg/dL"},
        ],
        "attending_physician": "dr_m_smith",
        "baseline": {"hr": 88, "sbp": 128, "dbp": 76, "spo2": 96, "rr": 16},
        "trajectory": "sepsis",
    },
    {
        "patient_id": "pt_20419",
        "age": 54,
        "sex": "Female",
        "bed": "MedSurg-12",
        "primary_dx": "Acute exacerbation of COPD",
        "comorbidities": ["COPD Stage III", "Hypertension"],
        "immunocompromised": False,
        "recent_labs": [
            {"name": "pH", "value": 7.35, "unit": ""},
            {"name": "pCO2", "value": 48, "unit": "mmHg"},
        ],
        "attending_physician": "dr_a_chen",
        "baseline": {"hr": 82, "sbp": 135, "dbp": 82, "spo2": 93, "rr": 18},
        "trajectory": "stable",
    },
    {
        "patient_id": "pt_10882",
        "age": 45,
        "sex": "Male",
        "bed": "MedSurg-07",
        "primary_dx": "Post-operative recovery (appendectomy)",
        "comorbidities": [],
        "immunocompromised": False,
        "recent_labs": [
            {"name": "Hemoglobin", "value": 13.5, "unit": "g/dL"},
        ],
        "attending_physician": "dr_j_patel",
        "baseline": {"hr": 78, "sbp": 122, "dbp": 78, "spo2": 97, "rr": 15},
        "trajectory": "stable",
    },
    {
        "patient_id": "pt_40291",
        "age": 72,
        "sex": "Female",
        "bed": "ICU-08",
        "primary_dx": "Acute decompensated heart failure",
        "comorbidities": ["CHF NYHA III", "Atrial Fibrillation", "Diabetes Mellitus"],
        "immunocompromised": False,
        "recent_labs": [
            {"name": "BNP", "value": 890, "unit": "pg/mL"},
            {"name": "Troponin", "value": 0.04, "unit": "ng/mL"},
        ],
        "attending_physician": "dr_s_kumar",
        "baseline": {"hr": 92, "sbp": 118, "dbp": 72, "spo2": 94, "rr": 20},
        "trajectory": "stable",
    },
    {
        "patient_id": "pt_50163",
        "age": 38,
        "sex": "Male",
        "bed": "StepDown-03",
        "primary_dx": "Traumatic brain injury (GCS 13), observation",
        "comorbidities": [],
        "immunocompromised": False,
        "recent_labs": [
            {"name": "Hemoglobin", "value": 11.8, "unit": "g/dL"},
            {"name": "Platelets", "value": 185, "unit": "x10^9/L"},
        ],
        "attending_physician": "dr_l_wang",
        "baseline": {"hr": 72, "sbp": 130, "dbp": 82, "spo2": 98, "rr": 14},
        "trajectory": "stable",
    },
]


def _generate_baseline_history(
    patient_id: str, baseline: Dict[str, float], hours: int = 4, interval_minutes: int = 5
) -> List[VitalReading]:
    """Generate stable baseline vitals history with minor physiological variance."""
    readings = []
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)
    num_readings = (hours * 60) // interval_minutes

    for i in range(num_readings):
        ts = start + timedelta(minutes=i * interval_minutes)
        # Small random physiological variance
        reading = VitalReading(
            patient_id=patient_id,
            timestamp=ts,
            hr=baseline["hr"] + random.uniform(-3, 3),
            sbp=baseline["sbp"] + random.uniform(-4, 4),
            dbp=baseline["dbp"] + random.uniform(-3, 3),
            spo2=min(100, baseline["spo2"] + random.uniform(-1, 1)),
            rr=max(8, baseline["rr"] + random.uniform(-1, 1)),
        )
        readings.append(reading)
    return readings


def _generate_sepsis_trajectory(patient_id: str, num_ticks: int = 8) -> List[VitalReading]:
    """
    Generate a deteriorating sepsis trajectory for pt_30045.
    Gradually escalates HR and drops SBP/MAP toward the trigger thresholds.
    Final ticks: HR=122, BP=90/60, MAP=70.0.
    """
    now = datetime.now(timezone.utc)
    ticks = []

    # Trajectory: stable baseline → gradual deterioration → trigger
    trajectory_points = [
        # (hr, sbp, dbp, spo2, rr)
        (95, 120, 74, 96, 17),    # Tick 1: slightly elevated
        (100, 115, 72, 95, 18),   # Tick 2: creeping up
        (106, 108, 68, 95, 19),   # Tick 3: noticeable change
        (112, 102, 65, 95, 20),   # Tick 4: concerning
        (116, 96, 62, 94, 22),    # Tick 5: deteriorating
        (120, 92, 61, 94, 23),    # Tick 6: borderline
        (122, 90, 60, 94, 24),    # Tick 7: TRIGGER — HR>120, SBP<=90
        (122, 90, 60, 94, 24),    # Tick 8: sustained — confirms in window
    ]

    for i, (hr, sbp, dbp, spo2, rr) in enumerate(trajectory_points[:num_ticks]):
        ts = now + timedelta(minutes=i * 5)
        ticks.append(VitalReading(
            patient_id=patient_id,
            timestamp=ts,
            hr=hr,
            sbp=sbp,
            dbp=dbp,
            spo2=spo2,
            rr=rr,
        ))
    return ticks


def _generate_hypoxia_trajectory(patient_id: str, num_ticks: int = 8) -> List[VitalReading]:
    """
    Generate a progressive hypoxia trajectory.
    SpO2 drops below 90% with RR rising above 26.
    """
    now = datetime.now(timezone.utc)
    ticks = []
    trajectory_points = [
        (84, 132, 80, 93, 20),
        (86, 130, 78, 91, 22),
        (88, 128, 78, 90, 24),
        (90, 126, 76, 89, 25),
        (92, 124, 76, 88, 27),
        (94, 122, 74, 87, 28),
        (96, 120, 74, 87, 29),  # TRIGGER — SpO2<90 + RR>=26
        (96, 120, 74, 87, 29),  # sustained
    ]
    for i, (hr, sbp, dbp, spo2, rr) in enumerate(trajectory_points[:num_ticks]):
        ts = now + timedelta(minutes=i * 5)
        ticks.append(VitalReading(
            patient_id=patient_id,
            timestamp=ts,
            hr=hr,
            sbp=sbp,
            dbp=dbp,
            spo2=spo2,
            rr=rr,
        ))
    return ticks


def _generate_artifact_tick(patient_id: str) -> VitalReading:
    """
    Generate a single-tick transient SpO2 artifact.
    SpO2 drops to 83% while companion channels remain stable/baseline.
    """
    return VitalReading(
        patient_id=patient_id,
        timestamp=datetime.now(timezone.utc),
        hr=78,
        sbp=122,
        dbp=78,
        spo2=83,
        rr=15,
    )


# ---------------------------------------------------------------------------
# Bootstrap & scenario methods
# ---------------------------------------------------------------------------

def bootstrap_cohort() -> Dict[str, Any]:
    """
    Seed all 5 patients with static context and baseline 4-hour history,
    then advance the simulator through pt_30045's sepsis trajectory ticks.

    Returns a summary dict of what was bootstrapped.
    """
    processor = get_processor()
    db = get_db()
    summary = {"patients_seeded": 0, "baseline_readings": 0, "escalation_fired": False}

    # Set up the Tier 2 callback
    def tier2_callback(event: EscalationCandidateEvent):
        result = run_tier2_pipeline(event)
        if result:
            summary["escalation_fired"] = True
            logger.info(f"Bootstrap: Tier 2 pipeline produced escalation {result.escalation_id}")

    processor.set_escalation_callback(tier2_callback)

    for patient_def in PATIENT_COHORT:
        pid = patient_def["patient_id"]
        now = datetime.now(timezone.utc)

        # Build static context
        labs = [
            LabResult(
                name=lab["name"],
                value=lab["value"],
                unit=lab["unit"],
                timestamp=now - timedelta(hours=2),
            )
            for lab in patient_def.get("recent_labs", [])
        ]

        static_ctx = PatientStaticContext(
            patient_id=pid,
            age=patient_def["age"],
            sex=patient_def["sex"],
            bed=patient_def["bed"],
            primary_dx=patient_def["primary_dx"],
            comorbidities=patient_def.get("comorbidities", []),
            immunocompromised=patient_def.get("immunocompromised", False),
            recent_labs=labs,
            admission_date=now - timedelta(days=random.randint(1, 5)),
            attending_physician=patient_def.get("attending_physician", ""),
        )

        # Generate baseline history
        baseline = patient_def["baseline"]
        history = _generate_baseline_history(pid, baseline, hours=4)

        # Create twin
        twin = PatientTwin(
            patient_id=pid,
            static_context=static_ctx,
        )

        # Register twin with processor
        processor.register_twin(twin)

        # Save twin to database FIRST (FK constraint for vitals_history)
        db.upsert_twin(twin)

        # Ingest baseline history through the pipeline
        for vital in history:
            processor.ingest_vital_reading(vital)
            db.insert_vital(vital)

        summary["patients_seeded"] += 1
        summary["baseline_readings"] += len(history)

        # Save updated twin state to database
        db.upsert_twin(processor.get_twin(pid))

    # (Removed) Now advance pt_30045 through sepsis trajectory
    # We now rely on the external Kafka patient_simulator to generate spikes!

    # Save final twin state
    for patient_def in PATIENT_COHORT:
        pid = patient_def["patient_id"]
        twin = processor.get_twin(pid)
        if twin:
            db.upsert_twin(twin)

    logger.info(f"Bootstrap complete: {summary}")
    return summary


def step_all_patients() -> Dict[str, Any]:
    """Advance one live tick for all patients (stable jitter)."""
    processor = get_processor()
    results = {}
    for twin in processor.get_all_twins():
        baseline = None
        for p in PATIENT_COHORT:
            if p["patient_id"] == twin.patient_id:
                baseline = p["baseline"]
                break
        if not baseline:
            continue

        vital = VitalReading(
            patient_id=twin.patient_id,
            timestamp=datetime.now(timezone.utc),
            hr=baseline["hr"] + random.uniform(-3, 3),
            sbp=baseline["sbp"] + random.uniform(-4, 4),
            dbp=baseline["dbp"] + random.uniform(-3, 3),
            spo2=min(100, baseline["spo2"] + random.uniform(-1, 1)),
            rr=max(8, baseline["rr"] + random.uniform(-1, 1)),
        )
        result = processor.ingest_vital_reading(vital)
        db = get_db()
        db.insert_vital(vital)
        db.upsert_twin(processor.get_twin(twin.patient_id))
        results[twin.patient_id] = result

    return results


def inject_artifact(patient_id: str = "pt_10882") -> Dict[str, Any]:
    """Inject a transient SpO2 artifact tick for artifact suppression testing."""
    processor = get_processor()
    artifact_tick = _generate_artifact_tick(patient_id)
    result = processor.ingest_vital_reading(artifact_tick)
    db = get_db()
    db.insert_vital(artifact_tick)
    twin = processor.get_twin(patient_id)
    if twin:
        db.upsert_twin(twin)
    return result


def trigger_sepsis_trajectory(patient_id: str = "pt_30045") -> Dict[str, Any]:
    """Trigger a sepsis trajectory for the specified patient."""
    processor = get_processor()
    db = get_db()

    def tier2_callback(event: EscalationCandidateEvent):
        run_tier2_pipeline(event)

    processor.set_escalation_callback(tier2_callback)

    ticks = _generate_sepsis_trajectory(patient_id)
    last_result = {}
    for tick in ticks:
        last_result = processor.ingest_vital_reading(tick)
        db.insert_vital(tick)

    twin = processor.get_twin(patient_id)
    if twin:
        db.upsert_twin(twin)

    return last_result


def trigger_hypoxia_trajectory(patient_id: str = "pt_20419") -> Dict[str, Any]:
    """Trigger a progressive hypoxia trajectory for the specified patient."""
    processor = get_processor()
    db = get_db()

    def tier2_callback(event: EscalationCandidateEvent):
        run_tier2_pipeline(event)

    processor.set_escalation_callback(tier2_callback)

    ticks = _generate_hypoxia_trajectory(patient_id)
    last_result = {}
    for tick in ticks:
        last_result = processor.ingest_vital_reading(tick)
        db.insert_vital(tick)

    twin = processor.get_twin(patient_id)
    if twin:
        db.upsert_twin(twin)

    return last_result


def test_verifier_guardrail() -> Dict[str, Any]:
    """
    Test the numeric verifier guardrail by injecting an ungrounded HR=165 claim.
    The verifier should reject the draft, loop back, and produce a corrected escalation.
    """
    processor = get_processor()

    # Use pt_30045's current state
    twin = processor.get_twin("pt_30045")
    if not twin or not twin.current_vitals:
        return {"error": "pt_30045 not found or has no vitals"}

    vitals = twin.current_vitals
    event = EscalationCandidateEvent(
        patient_id="pt_30045",
        trigger_type=EscalationType.HEMODYNAMIC_SEPSIS,
        triggering_vitals={
            "HR": vitals.hr, "SBP": vitals.sbp, "DBP": vitals.dbp,
            "MAP": vitals.map_value, "SpO2": vitals.spo2, "RR": vitals.rr,
            "trend": "hypotensive_tachycardia",
        },
        mews_score=twin.current_mews.total if twin.current_mews else 5,
        mews_trend_delta=twin.mews_trend_delta,
    )

    # Inject bad claim: HR=165 when actual is 122
    result = run_tier2_pipeline(event, inject_bad_claim={"value": 165, "unit": "bpm", "source": "current_vitals.hr"})

    return {
        "verifier_test": True,
        "escalation_produced": result is not None,
        "retry_count": result.retry_count if result else -1,
        "verification_passed": result.numeric_verification_passed if result else False,
    }


def reset_cohort() -> Dict[str, Any]:
    """Reset all data and replay the startup pipeline."""
    from backend.stream_engine import reset_processor, reset_broker, reset_cache
    from backend.database import get_db

    reset_processor()
    reset_broker()
    reset_cache()
    db = get_db()
    db.clear_all()

    return bootstrap_cohort()
