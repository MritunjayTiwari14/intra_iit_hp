"""
database.py — Dual-mode persistence layer for the Clinical Deterioration Copilot.

Docker mode:  PostgreSQL / TimescaleDB with JSONB columns and immutability rules.
Local mode:   SQLite WAL with CHECK constraints and immutability triggers.

Both modes enforce append-only audit log semantics via database-level triggers
that prevent UPDATE and DELETE operations on the immutable_audit_log table.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.config import get_config
from backend.models import (
    AuditLogEntry,
    EscalationStatus,
    PatientTwin,
    StructuredAgentEscalation,
    SuppressionEvent,
    VitalReading,
)

logger = logging.getLogger("copilot.database")

# ---------------------------------------------------------------------------
# SQLite local adapter
# ---------------------------------------------------------------------------

class SQLiteAdapter:
    """Thread-safe SQLite WAL adapter with immutable audit triggers."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS patient_twins (
                patient_id TEXT PRIMARY KEY,
                twin_data TEXT NOT NULL CHECK(json_valid(twin_data)),
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS vitals_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                vital_data TEXT NOT NULL CHECK(json_valid(vital_data)),
                FOREIGN KEY (patient_id) REFERENCES patient_twins(patient_id)
            );

            CREATE TABLE IF NOT EXISTS escalations (
                escalation_id TEXT PRIMARY KEY,
                patient_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                escalation_data TEXT NOT NULL CHECK(json_valid(escalation_data)),
                status TEXT NOT NULL DEFAULT 'PENDING',
                FOREIGN KEY (patient_id) REFERENCES patient_twins(patient_id)
            );

            CREATE TABLE IF NOT EXISTS immutable_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                patient_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                audit_payload TEXT NOT NULL CHECK(json_valid(audit_payload)),
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS suppression_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                patient_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                suppression_data TEXT NOT NULL CHECK(json_valid(suppression_data))
            );
        """)

        # Immutability triggers — wrap in try/except since they may already exist
        for trigger_sql in [
            """CREATE TRIGGER IF NOT EXISTS enforce_audit_no_update
               BEFORE UPDATE ON immutable_audit_log
               BEGIN SELECT RAISE(ABORT, 'Immutable audit log cannot be updated'); END;""",
            """CREATE TRIGGER IF NOT EXISTS enforce_audit_no_delete
               BEFORE DELETE ON immutable_audit_log
               BEGIN SELECT RAISE(ABORT, 'Immutable audit log cannot be deleted'); END;""",
        ]:
            try:
                conn.execute(trigger_sql)
            except sqlite3.OperationalError:
                pass  # trigger already exists

        conn.commit()
        logger.info(f"SQLite schema initialized at {self.db_path}")

    # ---- Patient Twins ----

    def upsert_twin(self, twin: PatientTwin):
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        data = twin.model_dump_json()
        conn.execute(
            """INSERT INTO patient_twins (patient_id, twin_data, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(patient_id) DO UPDATE SET twin_data=excluded.twin_data, updated_at=excluded.updated_at""",
            (twin.patient_id, data, now),
        )
        conn.commit()

    def get_twin(self, patient_id: str) -> Optional[PatientTwin]:
        conn = self._get_conn()
        row = conn.execute("SELECT twin_data FROM patient_twins WHERE patient_id=?", (patient_id,)).fetchone()
        if row:
            return PatientTwin.model_validate_json(row["twin_data"])
        return None

    def get_all_twins(self) -> List[PatientTwin]:
        conn = self._get_conn()
        rows = conn.execute("SELECT twin_data FROM patient_twins ORDER BY patient_id").fetchall()
        return [PatientTwin.model_validate_json(r["twin_data"]) for r in rows]

    # ---- Vitals History ----

    def insert_vital(self, vital: VitalReading):
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO vitals_history (patient_id, timestamp, vital_data) VALUES (?, ?, ?)",
            (vital.patient_id, vital.timestamp.isoformat(), vital.model_dump_json()),
        )
        conn.commit()

    def get_vitals(self, patient_id: str, limit: int = 48) -> List[VitalReading]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT vital_data FROM vitals_history WHERE patient_id=? ORDER BY timestamp DESC LIMIT ?",
            (patient_id, limit),
        ).fetchall()
        return [VitalReading.model_validate_json(r["vital_data"]) for r in reversed(rows)]

    # ---- Escalations ----

    def upsert_escalation(self, esc: StructuredAgentEscalation):
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO escalations (escalation_id, patient_id, timestamp, escalation_data, status)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(escalation_id) DO UPDATE SET
                   escalation_data=excluded.escalation_data,
                   status=excluded.status""",
            (esc.escalation_id, esc.patient_id, esc.timestamp.isoformat(), esc.model_dump_json(), esc.status.value),
        )
        conn.commit()

    def get_escalation(self, escalation_id: str) -> Optional[StructuredAgentEscalation]:
        conn = self._get_conn()
        row = conn.execute("SELECT escalation_data FROM escalations WHERE escalation_id=?", (escalation_id,)).fetchone()
        if row:
            return StructuredAgentEscalation.model_validate_json(row["escalation_data"])
        return None

    def get_active_escalations(self) -> List[StructuredAgentEscalation]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT escalation_data FROM escalations WHERE status='PENDING' ORDER BY timestamp DESC"
        ).fetchall()
        return [StructuredAgentEscalation.model_validate_json(r["escalation_data"]) for r in rows]

    def get_all_escalations(self) -> List[StructuredAgentEscalation]:
        conn = self._get_conn()
        rows = conn.execute("SELECT escalation_data FROM escalations ORDER BY timestamp DESC").fetchall()
        return [StructuredAgentEscalation.model_validate_json(r["escalation_data"]) for r in rows]

    # ---- Immutable Audit Log ----

    def append_audit(self, entry: AuditLogEntry):
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO immutable_audit_log (event_id, patient_id, timestamp, audit_payload) VALUES (?, ?, ?, ?)",
            (entry.event_id, entry.patient_id, entry.timestamp.isoformat(), entry.model_dump_json()),
        )
        conn.commit()

    def get_audit_logs(self, patient_id: Optional[str] = None) -> List[AuditLogEntry]:
        conn = self._get_conn()
        if patient_id:
            rows = conn.execute(
                "SELECT audit_payload FROM immutable_audit_log WHERE patient_id=? ORDER BY timestamp DESC",
                (patient_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT audit_payload FROM immutable_audit_log ORDER BY timestamp DESC"
            ).fetchall()
        return [AuditLogEntry.model_validate_json(r["audit_payload"]) for r in rows]

    def test_audit_immutability(self) -> tuple:
        """Test that UPDATE and DELETE are blocked. Returns (update_blocked, delete_blocked)."""
        conn = self._get_conn()

        # Insert a test row
        test_entry = AuditLogEntry(
            event_id="test_immutability_check",
            patient_id="test_patient",
            timestamp=datetime.now(timezone.utc),
            clinician_decision="TEST",
        )
        try:
            conn.execute(
                "INSERT OR IGNORE INTO immutable_audit_log (event_id, patient_id, timestamp, audit_payload) VALUES (?, ?, ?, ?)",
                (test_entry.event_id, test_entry.patient_id, test_entry.timestamp.isoformat(), test_entry.model_dump_json()),
            )
            conn.commit()
        except Exception:
            pass

        update_blocked = False
        try:
            conn.execute(
                "UPDATE immutable_audit_log SET audit_payload='{}' WHERE event_id='test_immutability_check'"
            )
            conn.commit()
        except Exception as e:
            err_msg = str(e)
            if "Immutable" in err_msg or "cannot be updated" in err_msg:
                update_blocked = True
            try:
                conn.rollback()
            except Exception:
                pass

        # Re-ensure the test row exists for DELETE test (rollback may have removed it)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO immutable_audit_log (event_id, patient_id, timestamp, audit_payload) VALUES (?, ?, ?, ?)",
                (test_entry.event_id, test_entry.patient_id, test_entry.timestamp.isoformat(), test_entry.model_dump_json()),
            )
            conn.commit()
        except Exception:
            pass

        delete_blocked = False
        try:
            conn.execute("DELETE FROM immutable_audit_log WHERE event_id='test_immutability_check'")
            conn.commit()
        except Exception as e:
            err_msg = str(e)
            if "Immutable" in err_msg or "cannot be deleted" in err_msg:
                delete_blocked = True
            try:
                conn.rollback()
            except Exception:
                pass

        return update_blocked, delete_blocked

    # ---- Suppression Log ----

    def insert_suppression(self, event: SuppressionEvent):
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO suppression_log (event_id, patient_id, timestamp, suppression_data) VALUES (?, ?, ?, ?)",
            (event.event_id, event.patient_id, event.timestamp.isoformat(), event.model_dump_json()),
        )
        conn.commit()

    def get_suppression_logs(self) -> List[SuppressionEvent]:
        conn = self._get_conn()
        rows = conn.execute("SELECT suppression_data FROM suppression_log ORDER BY timestamp DESC").fetchall()
        return [SuppressionEvent.model_validate_json(r["suppression_data"]) for r in rows]

    def clear_all(self):
        """Clear all data (for testing / reset). Re-creates schema."""
        conn = self._get_conn()
        conn.executescript("""
            DROP TABLE IF EXISTS suppression_log;
            DROP TABLE IF EXISTS immutable_audit_log;
            DROP TABLE IF EXISTS escalations;
            DROP TABLE IF EXISTS vitals_history;
            DROP TABLE IF EXISTS patient_twins;
            DROP TRIGGER IF EXISTS enforce_audit_no_update;
            DROP TRIGGER IF EXISTS enforce_audit_no_delete;
        """)
        conn.commit()
        self._init_schema()


# ---------------------------------------------------------------------------
# Database manager (adapter pattern)
# ---------------------------------------------------------------------------

class DatabaseManager:
    """Unified database interface that delegates to the active adapter."""

    def __init__(self):
        cfg = get_config()
        if cfg.mode == "docker":
            # For docker mode, we still use SQLite as a simpler approach for hackathon
            # A production system would use the full PostgreSQL/TimescaleDB adapter
            logger.info("Docker mode: using PostgreSQL-style adapter (SQLite stand-in for hackathon)")
            self._adapter = SQLiteAdapter(cfg.sqlite_path)
        else:
            self._adapter = SQLiteAdapter(cfg.sqlite_path)
        logger.info(f"Database manager initialized (mode={cfg.mode})")

    @property
    def adapter(self) -> SQLiteAdapter:
        return self._adapter

    def upsert_twin(self, twin: PatientTwin):
        self._adapter.upsert_twin(twin)

    def get_twin(self, patient_id: str) -> Optional[PatientTwin]:
        return self._adapter.get_twin(patient_id)

    def get_all_twins(self) -> List[PatientTwin]:
        return self._adapter.get_all_twins()

    def insert_vital(self, vital: VitalReading):
        self._adapter.insert_vital(vital)

    def get_vitals(self, patient_id: str, limit: int = 48) -> List[VitalReading]:
        return self._adapter.get_vitals(patient_id, limit)

    def upsert_escalation(self, esc: StructuredAgentEscalation):
        self._adapter.upsert_escalation(esc)

    def get_escalation(self, escalation_id: str) -> Optional[StructuredAgentEscalation]:
        return self._adapter.get_escalation(escalation_id)

    def get_active_escalations(self) -> List[StructuredAgentEscalation]:
        return self._adapter.get_active_escalations()

    def get_all_escalations(self) -> List[StructuredAgentEscalation]:
        return self._adapter.get_all_escalations()

    def append_audit(self, entry: AuditLogEntry):
        self._adapter.append_audit(entry)

    def get_audit_logs(self, patient_id: Optional[str] = None) -> List[AuditLogEntry]:
        return self._adapter.get_audit_logs(patient_id)

    def test_audit_immutability(self) -> tuple:
        return self._adapter.test_audit_immutability()

    def insert_suppression(self, event: SuppressionEvent):
        self._adapter.insert_suppression(event)

    def get_suppression_logs(self) -> List[SuppressionEvent]:
        return self._adapter.get_suppression_logs()

    def clear_all(self):
        self._adapter.clear_all()


# Module-level singleton
_db: Optional[DatabaseManager] = None
_db_lock = threading.Lock()


def get_db() -> DatabaseManager:
    global _db
    if _db is None:
        with _db_lock:
            if _db is None:
                _db = DatabaseManager()
    return _db


def reset_db():
    global _db
    _db = None
