"""
verify_system.py — Automated E2E verification suite validating the Acceptance Matrix.

Tests all 7 rows of the Acceptance Matrix plus an immutability constraint test.
Runs the actual production code paths (no mocks) to validate deterministic behavior.

Usage:
    python verify_system.py
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime, timedelta, timezone

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Force local mode for testing
os.environ["INFRA_MODE"] = "local"

logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("verify")
logger.setLevel(logging.INFO)


def reset_all():
    """Reset all singletons to clean state."""
    from backend.config import reset_config
    from backend.stream_engine import reset_processor, reset_broker, reset_cache
    from backend.database import reset_db
    from backend.rag_engine import reset_rag

    # Remove old database
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    db_path = os.path.join(data_dir, "clinical_copilot.db")
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    reset_config()
    reset_processor()
    reset_broker()
    reset_cache()
    reset_db()
    reset_rag()


class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def ok(self, name: str, detail: str = ""):
        self.passed += 1
        logger.info(f"  ✓ PASS: {name}" + (f" — {detail}" if detail else ""))

    def fail(self, name: str, detail: str = ""):
        self.failed += 1
        msg = f"  ✗ FAIL: {name}" + (f" — {detail}" if detail else "")
        self.errors.append(msg)
        logger.error(msg)

    def assert_true(self, condition: bool, name: str, detail: str = ""):
        if condition:
            self.ok(name, detail)
        else:
            self.fail(name, detail)

    def summary(self) -> bool:
        total = self.passed + self.failed
        logger.info(f"\n{'='*60}")
        logger.info(f" RESULTS: {self.passed}/{total} passed, {self.failed} failed")
        logger.info(f"{'='*60}")
        if self.errors:
            for e in self.errors:
                logger.error(e)
        return self.failed == 0


def test_scenario_1_startup_pipeline(results: TestResults):
    """
    Scenario 1: Startup Pipeline Bootstrap
    - Seed cohort + pt_30045 trajectory ticks (HR=122, BP=90/60, MAP=70.0)
    - Tier 1 detects HR>120 + SBP<=90, emits EscalationCandidateEvent
    - Tier 2 runs 5-node LangGraph; cites SBP 90 <= 90 & HR 122 > 120; passes verifier
    - Escalation saved + PENDING audit record
    """
    logger.info("\n[Scenario 1] Startup Pipeline Bootstrap")

    from backend.simulator import bootstrap_cohort
    from backend.stream_engine import get_processor
    from backend.database import get_db

    summary = bootstrap_cohort()

    # Check patients seeded
    results.assert_true(summary["patients_seeded"] == 5, "5 patients seeded", f"seeded={summary['patients_seeded']}")

    # Check escalation fired for pt_30045
    results.assert_true(summary["escalation_fired"], "Escalation fired during bootstrap")

    # Check processor state
    processor = get_processor()
    twin = processor.get_twin("pt_30045")
    results.assert_true(twin is not None, "pt_30045 twin exists")
    results.assert_true(
        twin.urgency.value == "ESCALATION",
        "pt_30045 urgency is ESCALATION",
        f"actual={twin.urgency.value}",
    )

    # Check escalation in database
    db = get_db()
    active = db.get_active_escalations()
    pt_30045_esc = [e for e in active if e.patient_id == "pt_30045"]
    results.assert_true(len(pt_30045_esc) >= 1, "pt_30045 has active escalation", f"count={len(pt_30045_esc)}")

    if pt_30045_esc:
        esc = pt_30045_esc[0]
        # Check verification passed
        results.assert_true(esc.numeric_verification_passed, "Numeric verification passed")
        # Check SBAR mentions correct thresholds
        results.assert_true("120" in esc.Reasoning, "Reasoning mentions HR threshold 120")
        results.assert_true("90" in esc.Reasoning and "SBP" in esc.Reasoning, "Reasoning mentions SBP 90")
        # Check it does NOT claim MAP < 70 (since MAP = 70.0)
        results.assert_true(
            "MAP < 70" not in esc.Reasoning and "MAP<70" not in esc.Reasoning,
            "Reasoning does NOT falsely claim MAP < 70",
        )
        # Check triggering signals
        results.assert_true(
            esc.Triggering_Signals.get("HR") == 122 or esc.Triggering_Signals.get("HR") == 122.0,
            "Triggering HR = 122",
            f"actual={esc.Triggering_Signals.get('HR')}",
        )
        results.assert_true(
            esc.Triggering_Signals.get("MAP") == 70.0,
            "Triggering MAP = 70.0",
            f"actual={esc.Triggering_Signals.get('MAP')}",
        )
        # Check retrieved protocols
        results.assert_true(
            "sepsis_protocol_v2.md" in esc.Retrieved_Protocol,
            "Retrieved sepsis protocol",
            f"protocols={esc.Retrieved_Protocol}",
        )

    # Check audit log
    audits = db.get_audit_logs("pt_30045")
    results.assert_true(len(audits) >= 1, "pt_30045 has audit record")
    if audits:
        results.assert_true(
            audits[0].clinician_decision == "PENDING",
            "Initial audit decision is PENDING",
            f"actual={audits[0].clinician_decision}",
        )


def test_scenario_2_artifact_suppression(results: TestResults):
    """
    Scenario 2: Transient 10s SpO2 Artifact
    - Single-tick SpO2=83% on pt_10882 while HR=78, RR=15, BP=122/78 stay stable
    - Tier 1 flags SUPPRESSED_ARTIFACT
    - 0 LangGraph invocations
    - suppressed_alarms_count increments by +1
    """
    logger.info("\n[Scenario 2] Transient SpO2 Artifact Suppression")

    from backend.stream_engine import get_processor
    from backend.simulator import inject_artifact
    from backend.database import get_db

    processor = get_processor()
    twin_before = processor.get_twin("pt_10882")
    suppressed_before = twin_before.suppressed_alarms_count if twin_before else 0

    # Get escalation count before
    db = get_db()
    esc_before = len([e for e in db.get_all_escalations() if e.patient_id == "pt_10882"])

    result = inject_artifact("pt_10882")

    results.assert_true(result.get("suppressed") == True, "Artifact was suppressed", f"result={result}")
    results.assert_true(result.get("escalation_fired") == False, "No escalation fired")

    # Check suppression count incremented
    twin_after = processor.get_twin("pt_10882")
    results.assert_true(
        twin_after.suppressed_alarms_count == suppressed_before + 1,
        "Suppressed alarms count incremented",
        f"before={suppressed_before}, after={twin_after.suppressed_alarms_count}",
    )

    # Check no new escalation for pt_10882
    esc_after = len([e for e in db.get_all_escalations() if e.patient_id == "pt_10882"])
    results.assert_true(esc_after == esc_before, "No new escalation for pt_10882", f"before={esc_before}, after={esc_after}")


def test_scenario_3_hypoxia(results: TestResults):
    """
    Scenario 3: Progressive Hypoxia
    - Sustained SpO2=87% + RR=29 on pt_20419 across window
    - Tier 2 retrieves hypoxia_protocol_v1.md; passes verifier
    """
    logger.info("\n[Scenario 3] Progressive Hypoxia Trajectory")

    from backend.simulator import trigger_hypoxia_trajectory
    from backend.stream_engine import get_processor
    from backend.database import get_db

    result = trigger_hypoxia_trajectory("pt_20419")

    results.assert_true(
        result.get("escalation_fired") == True,
        "Hypoxia escalation fired",
        f"result={result}",
    )

    # Check escalation exists
    db = get_db()
    all_esc = db.get_all_escalations()
    hypoxia_esc = [e for e in all_esc if e.patient_id == "pt_20419" and e.escalation_type.value == "Respiratory_Hypoxia"]
    results.assert_true(len(hypoxia_esc) >= 1, "Hypoxia escalation saved", f"count={len(hypoxia_esc)}")

    if hypoxia_esc:
        esc = hypoxia_esc[0]
        results.assert_true(esc.numeric_verification_passed, "Hypoxia escalation verified")
        results.assert_true(
            "hypoxia_protocol_v1.md" in esc.Retrieved_Protocol,
            "Retrieved hypoxia protocol",
            f"protocols={esc.Retrieved_Protocol}",
        )
        results.assert_true("SpO2" in esc.Reasoning, "Reasoning mentions SpO2")
        results.assert_true("26" in esc.Reasoning or "tachypnea" in esc.Reasoning.lower(), "Reasoning mentions RR threshold")


def test_scenario_4_verifier_guardrail(results: TestResults):
    """
    Scenario 4: Hallucinated Number (HR=165)
    - Inject ungrounded claim 165 bpm (actual HR=122)
    - Verifier rejects, loops back, corrected escalation saved with retry_count >= 1
    """
    logger.info("\n[Scenario 4] Verifier Guardrail Test")

    from backend.simulator import test_verifier_guardrail

    result = test_verifier_guardrail()

    results.assert_true(result.get("verifier_test") == True, "Verifier test executed")
    results.assert_true(result.get("escalation_produced") == True, "Corrected escalation produced")
    results.assert_true(
        result.get("retry_count", 0) >= 1,
        "At least 1 self-correction retry",
        f"retry_count={result.get('retry_count')}",
    )
    results.assert_true(result.get("verification_passed") == True, "Verification passed after correction")


def test_scenario_5_clinician_accept(results: TestResults):
    """
    Scenario 5: Clinician ACCEPT
    - Accept active escalation for pt_30045
    - Audit row with clinician_decision=ACCEPT, suppression_updated=false
    """
    logger.info("\n[Scenario 5] Clinician ACCEPT")

    from backend.database import get_db
    from backend.langgraph_copilot import process_clinician_action

    db = get_db()
    active = db.get_active_escalations()
    pt_esc = [e for e in active if e.patient_id == "pt_30045"]

    if not pt_esc:
        results.fail("No active escalation for pt_30045 to accept")
        return

    esc = pt_esc[0]
    audit = process_clinician_action(esc.escalation_id, "ACCEPT", "dr_m_smith")

    results.assert_true(audit is not None, "Audit record created")
    if audit:
        results.assert_true(audit.clinician_decision == "ACCEPT", "Decision is ACCEPT")
        results.assert_true(audit.clinician_id == "dr_m_smith", "Clinician ID correct")
        results.assert_true(audit.suppression_updated == False, "Suppression not updated for ACCEPT")

    # Verify escalation status updated
    updated = db.get_escalation(esc.escalation_id)
    results.assert_true(
        updated.status.value == "ACCEPTED",
        "Escalation status is ACCEPTED",
        f"actual={updated.status.value}",
    )


def test_scenario_6_clinician_dismiss(results: TestResults):
    """
    Scenario 6: Clinician DISMISS with "Known baseline"
    - Creates a new escalation, then dismisses it
    - Activates baseline suppression rule
    - Audit row: clinician_decision=DISMISS, suppression_updated=true
    """
    logger.info("\n[Scenario 6] Clinician DISMISS")

    from backend.simulator import trigger_sepsis_trajectory
    from backend.database import get_db
    from backend.stream_engine import get_processor
    from backend.langgraph_copilot import process_clinician_action

    # Trigger a fresh sepsis trajectory to get a new escalation
    trigger_sepsis_trajectory("pt_30045")

    db = get_db()
    active = db.get_active_escalations()
    pt_esc = [e for e in active if e.patient_id == "pt_30045"]

    if not pt_esc:
        results.fail("No active escalation for pt_30045 to dismiss")
        return

    esc = pt_esc[0]
    audit = process_clinician_action(esc.escalation_id, "DISMISS", "dr_m_smith", "Known baseline")

    results.assert_true(audit is not None, "Dismiss audit record created")
    if audit:
        results.assert_true(audit.clinician_decision == "DISMISS", "Decision is DISMISS")
        results.assert_true(audit.suppression_updated == True, "Suppression updated for DISMISS")

    # Verify suppression rule added
    processor = get_processor()
    twin = processor.get_twin("pt_30045")
    results.assert_true(
        "baseline_suppression" in twin.suppression_rules,
        "Baseline suppression rule activated",
        f"rules={twin.suppression_rules}",
    )

    # Verify repeat tick does not re-fire (suppressed)
    from backend.models import VitalReading
    stable_tick = VitalReading(
        patient_id="pt_30045",
        timestamp=datetime.now(timezone.utc),
        hr=88, sbp=128, dbp=76, spo2=96, rr=16,
    )
    result = processor.ingest_vital_reading(stable_tick)
    results.assert_true(
        result.get("suppressed") == True or result.get("escalation_fired") == False,
        "Repeat stable tick does not re-fire escalation",
        f"result={result}",
    )


def test_scenario_7_clinician_defer_watch(results: TestResults):
    """
    Scenario 7: Clinician DEFER_WATCH
    - Tightens dynamic_threshold_multiplier to 0.85
    - Audit row: clinician_decision=DEFER_WATCH
    """
    logger.info("\n[Scenario 7] Clinician DEFER_WATCH")

    from backend.simulator import trigger_sepsis_trajectory
    from backend.database import get_db
    from backend.stream_engine import get_processor
    from backend.langgraph_copilot import process_clinician_action

    # Clear suppression rules first so we can get a fresh escalation
    processor = get_processor()
    twin = processor.get_twin("pt_30045")
    if twin:
        twin.suppression_rules = []

    # Trigger fresh escalation
    trigger_sepsis_trajectory("pt_30045")

    db = get_db()
    active = db.get_active_escalations()
    pt_esc = [e for e in active if e.patient_id == "pt_30045"]

    if not pt_esc:
        results.fail("No active escalation for pt_30045 to defer")
        return

    esc = pt_esc[0]
    audit = process_clinician_action(esc.escalation_id, "DEFER_WATCH", "dr_m_smith")

    results.assert_true(audit is not None, "Defer audit record created")
    if audit:
        results.assert_true(audit.clinician_decision == "DEFER_WATCH", "Decision is DEFER_WATCH")

    # Check threshold tightened
    twin = processor.get_twin("pt_30045")
    results.assert_true(
        abs(twin.dynamic_threshold_multiplier - 0.85) < 0.01,
        "Threshold multiplier set to 0.85",
        f"actual={twin.dynamic_threshold_multiplier}",
    )

    # Check urgency
    results.assert_true(
        twin.urgency.value == "WATCH",
        "Patient urgency is WATCH",
        f"actual={twin.urgency.value}",
    )


def test_audit_immutability(results: TestResults):
    """
    Additional test: Verify that UPDATE and DELETE operations on the
    immutable_audit_log table raise errors.
    """
    logger.info("\n[Immutability Test] Audit log UPDATE/DELETE blocked")

    from backend.database import get_db

    db = get_db()
    update_blocked, delete_blocked = db.test_audit_immutability()

    results.assert_true(update_blocked, "UPDATE on immutable_audit_log is blocked")
    results.assert_true(delete_blocked, "DELETE on immutable_audit_log is blocked")


def test_mews_scoring(results: TestResults):
    """Additional test: Verify MEWS scoring against the authoritative lookup table."""
    logger.info("\n[MEWS Test] Scoring validation")

    from backend.stream_engine import score_rr, score_hr, score_sbp, score_spo2, compute_mews
    from backend.models import VitalReading

    # Test individual channel scores
    results.assert_true(score_rr(8) == 2, "RR < 9 → 2")
    results.assert_true(score_rr(12) == 0, "RR 12 → 0")
    results.assert_true(score_rr(18) == 1, "RR 18 → 1")
    results.assert_true(score_rr(25) == 2, "RR 25 → 2")
    results.assert_true(score_rr(32) == 3, "RR 32 → 3")

    results.assert_true(score_hr(35) == 2, "HR 35 → 2")
    results.assert_true(score_hr(45) == 1, "HR 45 → 1")
    results.assert_true(score_hr(75) == 0, "HR 75 → 0")
    results.assert_true(score_hr(105) == 1, "HR 105 → 1")
    results.assert_true(score_hr(120) == 2, "HR 120 → 2")
    results.assert_true(score_hr(135) == 3, "HR 135 → 3")

    results.assert_true(score_sbp(65) == 3, "SBP 65 → 3")
    results.assert_true(score_sbp(75) == 2, "SBP 75 → 2")
    results.assert_true(score_sbp(90) == 1, "SBP 90 → 1")
    results.assert_true(score_sbp(120) == 0, "SBP 120 → 0")
    results.assert_true(score_sbp(210) == 2, "SBP 210 → 2")

    results.assert_true(score_spo2(85) == 3, "SpO2 85 → 3")
    results.assert_true(score_spo2(92) == 2, "SpO2 92 → 2")
    results.assert_true(score_spo2(94) == 1, "SpO2 94 → 1")
    results.assert_true(score_spo2(98) == 0, "SpO2 98 → 0")

    # Test composite MEWS for pt_30045 trigger vitals (HR=122, SBP=90, SpO2=94, RR=24)
    vital = VitalReading(patient_id="test", hr=122, sbp=90, dbp=60, spo2=94, rr=24)
    mews = compute_mews(vital)
    # HR=122 → 2, SBP=90 → 1, SpO2=94 → 1, RR=24 → 2 = total 6
    expected_hr = score_hr(122)  # 2
    expected_sbp = score_sbp(90)  # 1
    expected_spo2 = score_spo2(94)  # 1
    expected_rr = score_rr(24)  # 2
    expected_total = expected_hr + expected_sbp + expected_spo2 + expected_rr  # 6

    results.assert_true(
        mews.total == expected_total,
        f"Composite MEWS for trigger vitals = {expected_total}",
        f"actual={mews.total} (hr={mews.hr_score}, sbp={mews.sbp_score}, spo2={mews.spo2_score}, rr={mews.rr_score})",
    )

    # Test MAP calculation
    results.assert_true(vital.map_value == 70.0, "MAP = 70.0 for BP 90/60", f"actual={vital.map_value}")


def main():
    print("\n" + "=" * 60)
    print(" Clinical Deterioration Copilot — Verification Suite")
    print("=" * 60)

    results = TestResults()

    try:
        # Reset everything
        reset_all()

        # Run all test scenarios
        test_mews_scoring(results)
        test_scenario_1_startup_pipeline(results)
        test_scenario_2_artifact_suppression(results)
        test_scenario_3_hypoxia(results)
        test_scenario_4_verifier_guardrail(results)
        test_scenario_5_clinician_accept(results)
        test_scenario_6_clinician_dismiss(results)
        test_scenario_7_clinician_defer_watch(results)
        test_audit_immutability(results)

    except Exception as e:
        logger.error(f"Fatal error during verification: {e}")
        traceback.print_exc()
        results.fail("FATAL", str(e))

    all_passed = results.summary()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
