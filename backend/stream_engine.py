"""
stream_engine.py — Tier 1: Stateful Window Processor & MEWS Engine.

This module implements Flink-style stateful tumbling/sliding window semantics
in pure Python for zero-config local execution. This is the local hackathon
substitute for Apache Flink (PyFlink). In docker mode, the same stateful
window processor could consume directly from Redpanda topics.

Key responsibilities:
  1. MEWS scoring using the authoritative 4-channel lookup table
  2. Tumbling window (5-min) and sliding window (30-min, 60-min) evaluation
  3. Multi-parameter protocol breach detection (Sepsis, Hypoxia)
  4. Transient artifact suppression (single-channel SpO2 filter)
  5. Emission of EscalationCandidateEvent for Tier 2 processing
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.models import (
    ClinicalUrgency,
    EscalationCandidateEvent,
    EscalationType,
    MEWSScore,
    PatientTwin,
    SuppressionEvent,
    VitalReading,
)

logger = logging.getLogger("copilot.stream_engine")

# ---------------------------------------------------------------------------
# Authoritative MEWS Lookup Functions
# ---------------------------------------------------------------------------

def score_rr(rr: float) -> int:
    """Respiratory Rate scoring."""
    if rr < 9:
        return 2
    elif rr <= 14:
        return 0
    elif rr <= 20:
        return 1
    elif rr <= 29:
        return 2
    else:  # >= 30
        return 3


def score_hr(hr: float) -> int:
    """Heart Rate scoring."""
    if hr <= 40:
        return 2
    elif hr <= 50:
        return 1
    elif hr <= 100:
        return 0
    elif hr <= 110:
        return 1
    elif hr <= 129:
        return 2
    else:  # >= 130
        return 3


def score_sbp(sbp: float) -> int:
    """Systolic Blood Pressure scoring."""
    if sbp <= 70:
        return 3
    elif sbp <= 80:
        return 2
    elif sbp <= 100:
        return 1
    elif sbp <= 199:
        return 0
    else:  # >= 200
        return 2


def score_spo2(spo2: float) -> int:
    """Oxygen Saturation scoring."""
    if spo2 < 90:
        return 3
    elif spo2 <= 93:
        return 2
    elif spo2 <= 95:
        return 1
    else:  # >= 96
        return 0


def compute_mews(vital: VitalReading) -> MEWSScore:
    """Compute composite MEWS from a vital reading."""
    rr_s = score_rr(vital.rr)
    hr_s = score_hr(vital.hr)
    sbp_s = score_sbp(vital.sbp)
    spo2_s = score_spo2(vital.spo2)
    total = rr_s + hr_s + sbp_s + spo2_s
    return MEWSScore(
        rr_score=rr_s,
        hr_score=hr_s,
        sbp_score=sbp_s,
        spo2_score=spo2_s,
        total=total,
        timestamp=vital.timestamp,
    )


def compute_map(sbp: float, dbp: float) -> float:
    """Mean Arterial Pressure: DBP + 1/3 * (SBP - DBP)"""
    return round(dbp + (1 / 3) * (sbp - dbp), 1)


# ---------------------------------------------------------------------------
# In-process Event Broker (local mode)
# ---------------------------------------------------------------------------

class InProcessBroker:
    """Thread-safe in-process event broker for local mode (replaces Redpanda)."""

    def __init__(self):
        self._topics: Dict[str, deque] = defaultdict(deque)
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = threading.RLock()

    def publish(self, topic: str, event: Any):
        with self._lock:
            self._topics[topic].append(event)
            for callback in self._subscribers.get(topic, []):
                try:
                    callback(event)
                except Exception as e:
                    logger.error(f"Broker subscriber error on {topic}: {e}")

    def subscribe(self, topic: str, callback: Callable):
        with self._lock:
            self._subscribers[topic].append(callback)

    def drain(self, topic: str) -> List[Any]:
        with self._lock:
            items = list(self._topics[topic])
            self._topics[topic].clear()
            return items

    def clear(self):
        with self._lock:
            self._topics.clear()
            self._subscribers.clear()


# Module-level broker singleton
_broker = InProcessBroker()


def get_broker() -> InProcessBroker:
    return _broker


def reset_broker():
    global _broker
    _broker = InProcessBroker()


# ---------------------------------------------------------------------------
# In-process State Cache (local mode, replaces Redis)
# ---------------------------------------------------------------------------

class InProcessCache:
    """Thread-safe in-process state cache (replaces Redis in local mode)."""

    def __init__(self):
        self._store: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def set(self, key: str, value: Any):
        with self._lock:
            self._store[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._store.get(key, default)

    def increment(self, key: str, amount: int = 1) -> int:
        with self._lock:
            current = self._store.get(key, 0)
            self._store[key] = current + amount
            return self._store[key]

    def clear(self):
        with self._lock:
            self._store.clear()


_cache = InProcessCache()


def get_cache() -> InProcessCache:
    return _cache


def reset_cache():
    global _cache
    _cache = InProcessCache()


# ---------------------------------------------------------------------------
# Stateful Window Processor
# ---------------------------------------------------------------------------

class StatefulWindowProcessor:
    """
    Flink-style stateful tumbling/sliding window processor implemented in pure Python.

    Maintains per-patient sliding windows for vital readings and MEWS scores.
    Evaluates multi-parameter protocol breach rules at each ingestion tick.
    """

    def __init__(self):
        # Per-patient sliding windows of VitalReading
        self._vital_windows: Dict[str, deque] = defaultdict(lambda: deque(maxlen=360))
        # Per-patient MEWS history (for tumbling 5-min windows)
        self._mews_windows: Dict[str, deque] = defaultdict(lambda: deque(maxlen=72))
        # Per-patient twins (in-memory cache)
        self._twins: Dict[str, PatientTwin] = {}
        # Tier 2 callback
        self._escalation_callback: Optional[Callable] = None
        # Counters
        self._suppressed_count = 0
        self._escalation_count = 0
        self._lock = threading.RLock()

    def register_twin(self, twin: PatientTwin):
        """Register or update a patient twin."""
        with self._lock:
            self._twins[twin.patient_id] = twin

    def get_twin(self, patient_id: str) -> Optional[PatientTwin]:
        with self._lock:
            return self._twins.get(patient_id)

    def get_all_twins(self) -> List[PatientTwin]:
        with self._lock:
            return list(self._twins.values())

    def set_escalation_callback(self, callback: Callable):
        """Set the Tier 2 callback invoked when an EscalationCandidateEvent fires."""
        self._escalation_callback = callback

    @property
    def suppressed_count(self) -> int:
        return self._suppressed_count

    @property
    def escalation_count(self) -> int:
        return self._escalation_count

    def ingest_vital_reading(self, vital: VitalReading) -> Dict[str, Any]:
        """
        Core ingestion pipeline:
        1. Compute MEWS
        2. Check artifact suppression
        3. Evaluate sliding/tumbling window rules
        4. Emit EscalationCandidateEvent if triggered
        Returns a dict summarizing what happened.
        """
        pid = vital.patient_id
        result: Dict[str, Any] = {
            "patient_id": pid,
            "mews": None,
            "suppressed": False,
            "escalation_fired": False,
            "trigger_type": None,
        }

        with self._lock:
            twin = self._twins.get(pid)
            if not twin:
                logger.warning(f"No twin registered for {pid}, skipping")
                return result

            # 1. Compute MEWS
            mews = compute_mews(vital)
            result["mews"] = mews.total

            # 2. Update windows
            self._vital_windows[pid].append(vital)
            self._mews_windows[pid].append(mews)

            # 3. Update twin
            twin.current_vitals = vital
            twin.vitals_history.append(vital)
            # Keep history manageable
            if len(twin.vitals_history) > 360:
                twin.vitals_history = twin.vitals_history[-360:]
            twin.current_mews = mews
            twin.mews_history.append(mews)
            if len(twin.mews_history) > 72:
                twin.mews_history = twin.mews_history[-72:]

            # 4. Compute MEWS trend delta (current vs 60 min ago)
            mews_60_min_ago = self._get_mews_at_offset(pid, minutes=60)
            if mews_60_min_ago is not None:
                twin.mews_trend_delta = mews.total - mews_60_min_ago
            else:
                twin.mews_trend_delta = 0.0

            # 5. Check artifact suppression (single-channel SpO2 dip)
            if self._is_artifact(pid, vital):
                self._suppressed_count += 1
                twin.suppressed_alarms_count += 1
                result["suppressed"] = True
                # Log suppression event
                sup_event = SuppressionEvent(
                    patient_id=pid,
                    reason="SUPPRESSED_ARTIFACT",
                    vitals_snapshot={
                        "HR": vital.hr, "SBP": vital.sbp, "DBP": vital.dbp,
                        "SpO2": vital.spo2, "RR": vital.rr,
                    },
                )
                get_broker().publish("suppression_events", sup_event)
                logger.info(f"ARTIFACT SUPPRESSED for {pid}: SpO2={vital.spo2}% (companion channels stable)")
                # Update urgency but don't fire escalation
                twin.urgency = ClinicalUrgency.STABLE
                return result

            # 6. Check clinician suppression rules
            if self._is_clinician_suppressed(twin, vital):
                result["suppressed"] = True
                self._suppressed_count += 1
                twin.suppressed_alarms_count += 1
                return result

            # 7. Evaluate escalation rules
            trigger_type = self._evaluate_escalation_rules(pid, vital, mews, twin)

            if trigger_type:
                result["escalation_fired"] = True
                result["trigger_type"] = trigger_type.value
                twin.urgency = ClinicalUrgency.ESCALATION
                self._escalation_count += 1

                # Build the candidate event
                map_val = compute_map(vital.sbp, vital.dbp)
                priority_mods = []
                if twin.static_context.immunocompromised:
                    priority_mods.append("immunocompromised")
                for lab in twin.static_context.recent_labs:
                    if lab.name == "Lactate" and lab.value > 2.0:
                        priority_mods.append(f"Lactate={lab.value}")

                event = EscalationCandidateEvent(
                    patient_id=pid,
                    trigger_type=trigger_type,
                    triggering_vitals={
                        "HR": vital.hr, "SBP": vital.sbp, "DBP": vital.dbp,
                        "MAP": map_val, "SpO2": vital.spo2, "RR": vital.rr,
                        "trend": self._get_trend_label(trigger_type),
                    },
                    mews_score=mews.total,
                    mews_trend_delta=twin.mews_trend_delta,
                    priority_modifiers=priority_mods,
                )

                get_broker().publish("escalation_candidates", event)
                logger.info(f"ESCALATION CANDIDATE emitted for {pid}: {trigger_type.value} (MEWS={mews.total})")

                # Fire Tier 2 callback if registered
                if self._escalation_callback:
                    try:
                        self._escalation_callback(event)
                    except Exception as e:
                        logger.error(f"Tier 2 callback error: {e}")
            else:
                # Update urgency based on MEWS
                if mews.total >= 4:
                    twin.urgency = ClinicalUrgency.WATCH
                elif twin.urgency != ClinicalUrgency.ESCALATION:
                    twin.urgency = ClinicalUrgency.STABLE

        return result

    def _is_artifact(self, pid: str, vital: VitalReading) -> bool:
        """
        Detect transient probe/motion artifact: single-channel SpO2 dip
        while companion channels (HR, RR, SBP) remain at baseline.
        """
        if vital.spo2 >= 90:
            return False  # No desaturation

        window = self._vital_windows[pid]
        if len(window) < 2:
            return False

        # Get previous reading
        prev = window[-2]  # -1 is the current one just appended

        # Check if companion channels stayed stable (within normal thresholds)
        hr_stable = abs(vital.hr - prev.hr) < 15 and 50 <= vital.hr <= 110
        rr_stable = abs(vital.rr - prev.rr) < 5 and 9 <= vital.rr <= 20
        sbp_stable = abs(vital.sbp - prev.sbp) < 15 and 90 <= vital.sbp <= 180

        # If all companion channels are stable, this is likely an artifact
        if hr_stable and rr_stable and sbp_stable:
            # Only suppress if this is a single-tick event (previous SpO2 was fine)
            if prev.spo2 >= 90:
                return True

        return False

    def _is_clinician_suppressed(self, twin: PatientTwin, vital: VitalReading) -> bool:
        """Check if patient has active clinician suppression rules."""
        for rule in twin.suppression_rules:
            if rule == "baseline_suppression":
                # Suppress if vitals are within the known baseline range
                if (50 <= vital.hr <= 110 and vital.sbp >= 90 and
                        vital.spo2 >= 92 and 9 <= vital.rr <= 22):
                    return True
        return False

    def _evaluate_escalation_rules(
        self, pid: str, vital: VitalReading, mews: MEWSScore, twin: PatientTwin
    ) -> Optional[EscalationType]:
        """
        Evaluate Tier 1 composite escalation trigger rules:
        1. Multi-parameter sepsis breach (30-min window)
        2. Multi-parameter hypoxia breach (15-30 min window)
        3. Sustained high MEWS >= 5 across >= 2 consecutive 5-min windows
        4. Rapid MEWS trend delta >= 3
        """
        map_val = compute_map(vital.sbp, vital.dbp)

        # Rule 1: Sepsis — HR > 120 AND (MAP < 70 OR SBP <= 90) sustained
        if vital.hr > 120 and (map_val < 70 or vital.sbp <= 90):
            # Check sustain over window (at least 2 readings showing the pattern)
            sepsis_readings = self._count_sustained_pattern(
                pid, lambda v: v.hr > 120 and (compute_map(v.sbp, v.dbp) < 70 or v.sbp <= 90),
                window_minutes=30,
            )
            if sepsis_readings >= 2:
                return EscalationType.HEMODYNAMIC_SEPSIS

        # Rule 2: Hypoxia — SpO2 < 90 AND RR >= 26 sustained
        if vital.spo2 < 90 and vital.rr >= 26:
            hypoxia_readings = self._count_sustained_pattern(
                pid, lambda v: v.spo2 < 90 and v.rr >= 26,
                window_minutes=30,
            )
            if hypoxia_readings >= 2:
                return EscalationType.RESPIRATORY_HYPOXIA

        # Rule 3: Sustained MEWS >= 5 across >= 2 consecutive 5-min windows
        if mews.total >= 5:
            high_mews_count = self._count_consecutive_high_mews(pid, threshold=5, min_count=2)
            if high_mews_count >= 2:
                return EscalationType.MEWS_HIGH

        # Rule 4: Rapid MEWS trend delta >= 3 (adjusted by dynamic_threshold_multiplier)
        threshold = 3 * twin.dynamic_threshold_multiplier
        if twin.mews_trend_delta >= threshold and twin.mews_trend_delta >= 3:
            return EscalationType.MEWS_TREND

        return None

    def _count_sustained_pattern(
        self, pid: str, predicate: Callable[[VitalReading], bool], window_minutes: int
    ) -> int:
        """Count readings matching predicate within the trailing window."""
        window = self._vital_windows[pid]
        if not window:
            return 0
        cutoff = window[-1].timestamp - timedelta(minutes=window_minutes)
        return sum(1 for v in window if v.timestamp >= cutoff and predicate(v))

    def _count_consecutive_high_mews(self, pid: str, threshold: int, min_count: int) -> int:
        """Count consecutive MEWS readings >= threshold from the most recent."""
        mews_window = self._mews_windows[pid]
        count = 0
        for m in reversed(mews_window):
            if m.total >= threshold:
                count += 1
            else:
                break
        return count

    def _get_mews_at_offset(self, pid: str, minutes: int) -> Optional[int]:
        """Get the MEWS score from approximately `minutes` ago."""
        mews_window = self._mews_windows[pid]
        if not mews_window:
            return None
        target_time = mews_window[-1].timestamp - timedelta(minutes=minutes)
        closest = None
        min_diff = float("inf")
        for m in mews_window:
            diff = abs((m.timestamp - target_time).total_seconds())
            if diff < min_diff:
                min_diff = diff
                closest = m
        if closest and min_diff < minutes * 60 * 1.5:
            return closest.total
        return None

    def _get_trend_label(self, trigger_type: EscalationType) -> str:
        labels = {
            EscalationType.HEMODYNAMIC_SEPSIS: "hypotensive_tachycardia",
            EscalationType.RESPIRATORY_HYPOXIA: "progressive_hypoxia",
            EscalationType.MEWS_HIGH: "sustained_high_mews",
            EscalationType.MEWS_TREND: "rapid_mews_trend",
        }
        return labels.get(trigger_type, "unknown")

    def reset(self):
        """Reset all windows and counters."""
        with self._lock:
            self._vital_windows.clear()
            self._mews_windows.clear()
            self._twins.clear()
            self._suppressed_count = 0
            self._escalation_count = 0


# Module-level singleton
_processor: Optional[StatefulWindowProcessor] = None
_processor_lock = threading.Lock()


def get_processor() -> StatefulWindowProcessor:
    global _processor
    if _processor is None:
        with _processor_lock:
            if _processor is None:
                _processor = StatefulWindowProcessor()
    return _processor


def reset_processor():
    global _processor
    if _processor:
        _processor.reset()
    _processor = None
