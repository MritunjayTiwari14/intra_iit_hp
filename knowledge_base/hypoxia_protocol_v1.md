# Acute Respiratory Failure & Hypoxia Escalation Protocol v1

## Protocol Identifier
- **File:** `hypoxia_protocol_v1.md`
- **Version:** 1.0
- **Last Reviewed:** 2026-09-01
- **Applies To:** All monitored inpatients with continuous pulse oximetry

---

## Progressive Hypoxia Rule Definition

### Primary Trigger Criteria

A **Progressive Hypoxia / Acute Respiratory Failure Escalation** is triggered when
the following conditions are sustained over a **15–30 minute sliding window**:

1. **Hypoxemia:** Oxygen Saturation (SpO2) < 90%
2. **Tachypnea:** Respiratory Rate (RR) >= 26 breaths/min

Both conditions must be present concurrently and sustained across the evaluation
window to differentiate true deterioration from transient artifacts.

---

## Artifact Suppression Rule

### Transient Probe/Motion Artifact Detection

A single-channel SpO2 drop is classified as a **transient probe/motion artifact**
and suppressed at Tier 1 (without invoking Tier 2 LangGraph) when ALL of the
following conditions are met:

1. SpO2 drops below threshold for **< 30 seconds** (approximately 1 reading tick)
2. **Companion channels remain at baseline:**
   - Heart Rate (HR) within normal range (no acute change)
   - Respiratory Rate (RR) within normal range (no acute change)
   - Systolic Blood Pressure (SBP) within normal range (no acute change)

**Rationale:** Isolated single-channel SpO2 desaturation without corroborating
changes in HR, RR, or SBP is most likely caused by probe displacement, patient
movement, or signal interference rather than true clinical deterioration.

---

## Clinical Presentation Patterns

### Early Progressive Hypoxia
- SpO2 trending downward: 94% → 91% → 88%
- RR increasing: 20 → 24 → 28
- HR compensatory increase (secondary sign)
- Patient may report dyspnea or increased work of breathing

### Fulminant Respiratory Failure
- SpO2 < 85% despite supplemental oxygen
- RR > 30 breaths/min or paradoxical decrease (exhaustion)
- Accessory muscle use, diaphoresis
- Altered mental status

---

## Recommended Clinical Actions

1. **Immediate:** Increase supplemental oxygen delivery (target SpO2 >= 94%)
2. **Assessment:** Auscultate lung fields, obtain arterial blood gas (ABG)
3. **Diagnostics:** Portable chest X-ray, consider CT pulmonary angiography if PE suspected
4. **Intervention:** Consider non-invasive ventilation (BiPAP/CPAP) if SpO2 < 90% on high-flow O2
5. **Escalation:** Notify respiratory therapy and attending physician
6. **Transfer:** Consider ICU transfer if FiO2 requirement > 0.6 or clinical worsening

---

## SBAR Communication Template

- **SITUATION:** Patient [ID] in [Location] is experiencing progressive oxygen
  desaturation with concurrent tachypnea.
- **BACKGROUND:** [Baseline respiratory status, COPD/asthma history, current O2 therapy]
- **ASSESSMENT:** SpO2 [value]% (below 90% threshold) with RR [value] breaths/min
  (>= 26, indicating tachypnea). Multi-parameter respiratory compromise pattern
  sustained over [duration] minutes. Consistent with acute respiratory deterioration.
- **RECOMMENDATION:** [Recommended clinical actions from above list]
