# Canonical Sepsis & Hemodynamic Escalation Protocol v2

## Protocol Identifier
- **File:** `sepsis_protocol_v2.md`
- **Version:** 2.0
- **Last Reviewed:** 2026-09-01
- **Applies To:** All monitored inpatients with continuous telemetry

---

## Sepsis & Hemodynamic Compromise Rule Definition

### Primary Trigger Criteria

A **Sepsis / Hemodynamic Escalation** is triggered when the following conditions
are sustained over a **30-minute sliding window**:

1. **Tachycardia:** Heart Rate (HR) > 120 bpm
2. **Hemodynamic Compromise** (at least ONE of the following):
   - Mean Arterial Pressure (MAP) < 70 mmHg
   - Systolic Blood Pressure (SBP) <= 90 mmHg

### Priority Modifiers

Escalation priority is heightened when any of the following are present in the
patient's static clinical context:

- **Immunocompromised status** (e.g., chemotherapy, transplant, HIV/AIDS)
- **Elevated Lactate:** Lactate > 2.0 mmol/L (from most recent lab results)
- **Recent surgical history** within 72 hours

### MAP Calculation

```
MAP = round(DBP + (1/3) * (SBP - DBP), 1)
```

---

## Canonical Example: Patient pt_30045

When a patient presents with:
- HR = 122 bpm
- BP = 90/60 mmHg (SBP = 90, DBP = 60)
- MAP = round(60 + (1/3)(90 - 60), 1) = 70.0 mmHg

**Correct Escalation Reasoning:**
- Tachycardia is present: HR 122 > 120 bpm threshold ✓
- Systolic hypotension is present: SBP 90 <= 90 mmHg threshold ✓
- MAP is at the 70.0 mmHg boundary (NOT < 70, so MAP criterion alone is not met)
- The escalation is triggered by the combination of tachycardia AND systolic
  hypotension (SBP <= 90)

**CRITICAL:** The escalation reasoning MUST correctly state that:
- `HR 122 > 120 bpm` (tachycardia criterion met)
- `SBP 90 <= 90 mmHg` (systolic hypotension criterion met)
- `MAP = 70.0 mmHg` (at boundary, not below — do NOT claim `MAP < 70`)

---

## Recommended Clinical Actions

1. **Immediate:** Obtain blood cultures x2, serum lactate, CBC with differential
2. **Within 30 minutes:** Initiate fluid resuscitation (30 mL/kg crystalloid)
3. **Within 60 minutes:** Administer broad-spectrum antibiotics if infection suspected
4. **Continuous:** Reassess hemodynamics every 15 minutes
5. **Escalate:** Notify attending physician and consider ICU transfer if no improvement

---

## SBAR Communication Template

- **SITUATION:** Patient [ID] in [Location] is showing signs of hemodynamic
  compromise with sustained tachycardia.
- **BACKGROUND:** [Relevant medical history, immunocompromised status, recent labs]
- **ASSESSMENT:** HR [value] exceeds tachycardia threshold (>120 bpm) and
  SBP [value] indicates systolic hypotension (<=90 mmHg). MAP [value] mmHg.
  Pattern consistent with early sepsis / hemodynamic deterioration.
- **RECOMMENDATION:** [Recommended clinical actions from above list]
