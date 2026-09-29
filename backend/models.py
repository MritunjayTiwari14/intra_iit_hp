"""
models.py — Strict Pydantic v2 schemas for the Clinical Deterioration Copilot.

Defines: VitalReading, PatientStaticContext, PatientTwin, NumericClaim,
StructuredAgentEscalation, AuditLogEntry, EscalationCandidateEvent, and more.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Helper functions for default_factory (Pydantic v2 requires non-lambda callables)
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _gen_esc_id() -> str:
    return f"esc_{uuid.uuid4().hex[:6]}"


def _gen_evt_id() -> str:
    return f"evt_{uuid.uuid4().hex[:6]}"


def _gen_sup_id() -> str:
    return f"sup_{uuid.uuid4().hex[:6]}"


def _gen_aud_id() -> str:
    return f"aud_{uuid.uuid4().hex[:6]}"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class EscalationStatus(str, Enum):
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    RESOLVED = "RESOLVED"
    DISMISSED = "DISMISSED"
    DEFERRED_WATCH = "DEFERRED_WATCH"
    INVESTIGATED = "INVESTIGATED"
    VERIFICATION_BLOCKED = "VERIFICATION_BLOCKED"


class EscalationType(str, Enum):
    HEMODYNAMIC_SEPSIS = "Hemodynamic_Sepsis"
    RESPIRATORY_HYPOXIA = "Respiratory_Hypoxia"
    MEWS_HIGH = "MEWS_High"
    MEWS_TREND = "MEWS_Trend"


class ClinicalUrgency(str, Enum):
    STABLE = "STABLE"
    WATCH = "WATCH"
    ESCALATION = "ESCALATION"


# ---------------------------------------------------------------------------
# Vital reading
# ---------------------------------------------------------------------------

class VitalReading(BaseModel):
    patient_id: str
    timestamp: datetime = Field(default_factory=_utcnow)
    hr: float = Field(..., description="Heart rate (bpm)")
    sbp: float = Field(..., description="Systolic blood pressure (mmHg)")
    dbp: float = Field(..., description="Diastolic blood pressure (mmHg)")
    spo2: float = Field(..., description="Oxygen saturation (%)")
    rr: float = Field(..., description="Respiratory rate (breaths/min)")
    temperature: Optional[float] = Field(None, description="Temperature (°C)")

    @property
    def map_value(self) -> float:
        """Mean Arterial Pressure = DBP + 1/3 * (SBP - DBP)"""
        return round(self.dbp + (1 / 3) * (self.sbp - self.dbp), 1)


# ---------------------------------------------------------------------------
# Patient static context
# ---------------------------------------------------------------------------

class LabResult(BaseModel):
    name: str
    value: float
    unit: str
    timestamp: datetime = Field(default_factory=_utcnow)


class PatientStaticContext(BaseModel):
    patient_id: str
    age: int
    sex: str
    bed: str
    primary_dx: str
    comorbidities: List[str] = Field(default_factory=list)
    immunocompromised: bool = False
    recent_labs: List[LabResult] = Field(default_factory=list)
    admission_date: datetime = Field(default_factory=_utcnow)
    attending_physician: str = ""


# ---------------------------------------------------------------------------
# MEWS state
# ---------------------------------------------------------------------------

class MEWSScore(BaseModel):
    rr_score: int = 0
    hr_score: int = 0
    sbp_score: int = 0
    spo2_score: int = 0
    total: int = 0
    timestamp: datetime = Field(default_factory=_utcnow)


# ---------------------------------------------------------------------------
# Patient digital twin
# ---------------------------------------------------------------------------

class PatientTwin(BaseModel):
    patient_id: str
    static_context: PatientStaticContext
    current_vitals: Optional[VitalReading] = None
    vitals_history: List[VitalReading] = Field(default_factory=list)
    current_mews: Optional[MEWSScore] = None
    mews_history: List[MEWSScore] = Field(default_factory=list)
    mews_trend_delta: float = 0.0
    urgency: ClinicalUrgency = ClinicalUrgency.STABLE
    dynamic_threshold_multiplier: float = 1.0
    suppression_rules: List[str] = Field(default_factory=list)
    active_escalation_id: Optional[str] = None
    suppressed_alarms_count: int = 0


# ---------------------------------------------------------------------------
# Numeric claim & structured escalation
# ---------------------------------------------------------------------------

class NumericClaim(BaseModel):
    value: float
    unit: str
    source: str  # Dot-path: "current_vitals.hr", "protocol.sepsis.sbp_max", etc.


class StructuredAgentEscalation(BaseModel):
    escalation_id: str = Field(default_factory=_gen_esc_id)
    patient_id: str = ""
    timestamp: datetime = Field(default_factory=_utcnow)
    escalation_type: EscalationType = EscalationType.HEMODYNAMIC_SEPSIS
    status: EscalationStatus = EscalationStatus.PENDING
    Triggering_Signals: Dict[str, Any] = Field(default_factory=dict)
    Retrieved_Protocol: List[str] = Field(default_factory=list)
    numeric_claims: List[NumericClaim] = Field(default_factory=list)
    Reasoning: str = ""
    Recommended_Action: str = ""
    numeric_verification_passed: bool = False
    verification_errors: List[str] = Field(default_factory=list)
    retry_count: int = 0
    agent_trace: List[Dict[str, Any]] = Field(default_factory=list)
    clinician_id: Optional[str] = None
    clinician_decision: Optional[str] = None
    dismiss_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Escalation candidate event (Tier 1 → Tier 2 bridge)
# ---------------------------------------------------------------------------

class EscalationCandidateEvent(BaseModel):
    event_id: str = Field(default_factory=_gen_evt_id)
    patient_id: str
    timestamp: datetime = Field(default_factory=_utcnow)
    trigger_type: EscalationType
    triggering_vitals: Dict[str, Any] = Field(default_factory=dict)
    mews_score: int = 0
    mews_trend_delta: float = 0.0
    window_duration_min: int = 30
    priority_modifiers: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Suppression event (Tier 1 artifact filter)
# ---------------------------------------------------------------------------

class SuppressionEvent(BaseModel):
    event_id: str = Field(default_factory=_gen_sup_id)
    patient_id: str
    timestamp: datetime = Field(default_factory=_utcnow)
    reason: str = "SUPPRESSED_ARTIFACT"
    vitals_snapshot: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Audit log entry
# ---------------------------------------------------------------------------

class AuditLogEntry(BaseModel):
    event_id: str = Field(default_factory=_gen_aud_id)
    patient_id: str = ""
    timestamp: datetime = Field(default_factory=_utcnow)
    triggering_vitals: Dict[str, Any] = Field(default_factory=dict)
    agent_retrieved_evidence: List[str] = Field(default_factory=list)
    agent_reasoning: str = ""
    clinician_decision: str = ""
    clinician_id: str = ""
    suppression_updated: bool = False
