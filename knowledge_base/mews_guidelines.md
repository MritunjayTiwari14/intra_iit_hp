# Authoritative 4-Channel MEWS Lookup Table & Trajectory Rules

## Modified Early Warning Score (MEWS) — Canonical Scoring Reference

This document defines the authoritative scoring table used by the Agentic Clinical
Deterioration & Escalation Copilot for continuous patient risk stratification.

---

## MEWS Channel Scoring Table

### Respiratory Rate (RR, breaths/min)

| Range         | Score |
|---------------|-------|
| < 9           | 2     |
| 9 – 14        | 0     |
| 15 – 20       | 1     |
| 21 – 29       | 2     |
| >= 30         | 3     |

### Heart Rate (HR, bpm)

| Range         | Score |
|---------------|-------|
| <= 40         | 2     |
| 41 – 50       | 1     |
| 51 – 100      | 0     |
| 101 – 110     | 1     |
| 111 – 129     | 2     |
| >= 130        | 3     |

### Systolic Blood Pressure (SBP, mmHg)

| Range         | Score |
|---------------|-------|
| <= 70         | 3     |
| 71 – 80       | 2     |
| 81 – 100      | 1     |
| 101 – 199     | 0     |
| >= 200        | 2     |

### Oxygen Saturation (SpO2, %)

| Range         | Score |
|---------------|-------|
| < 90          | 3     |
| 90 – 93       | 2     |
| 94 – 95       | 1     |
| >= 96         | 0     |

---

## Composite MEWS Calculation

```
current_MEWS = score(RR) + score(HR) + score(SBP) + score(SpO2)
```

## Mean Arterial Pressure (MAP) Formula

```
MAP = round(DBP + (1/3) * (SBP - DBP), 1)
```

## MEWS Trend Delta

```
mews_trend_delta = current_MEWS - MEWS_at_T_minus_60_minutes
```

---

## Tier 1 Composite Escalation Trigger Rules

An `EscalationCandidateEvent` is queued for Tier 2 review when ANY of the following
conditions are met:

1. **Multi-Parameter Protocol Breach:** Sustained multi-parameter protocol breach
   over a 30-minute tumbling/sliding window (see Sepsis and Hypoxia protocols).

2. **Sustained High MEWS:** `current_MEWS >= 5` sustained across >= 2 consecutive
   5-minute windows.

3. **Rapid MEWS Trend:** `mews_trend_delta >= +3` over the past 60 minutes,
   scaled by `dynamic_threshold_multiplier` when patient is on `DEFER_WATCH` status.

---

## Dynamic Threshold Adjustment

When a clinician places a patient on `DEFER_WATCH` status, the
`dynamic_threshold_multiplier` is set to `0.85`, meaning the MEWS trend delta
threshold becomes `3 * 0.85 = 2.55` (effectively triggering on `delta >= 3` still,
since MEWS scores are integers, but providing tighter re-evaluation).

---

## Window Semantics

- **Tumbling Window:** Non-overlapping fixed-duration windows (e.g., 5-minute blocks).
- **Sliding Window:** Overlapping windows evaluated at each new reading ingestion
  (e.g., trailing 30-minute or 60-minute evaluation on each tick).
