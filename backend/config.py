"""
config.py — Infrastructure mode resolution, connection probing, & Startup Health Report.

Supports INFRA_MODE=auto|local|docker via environment variable (default: auto).
- docker:  Uses container services (Redpanda, Redis, TimescaleDB, Qdrant).
- local:   Uses zero-config local adapters (SQLite WAL, in-process broker/cache, embedded Qdrant).
- auto:    Probes Docker endpoints (1.5s timeout) and falls back to local.
"""

import os
import socket
import logging
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("copilot.config")

# ---------------------------------------------------------------------------
# Adapter descriptor
# ---------------------------------------------------------------------------

@dataclass
class AdapterInfo:
    component: str          # e.g. "Stream Broker"
    adapter: str            # e.g. "Redpanda" or "InProcess"
    status: str = "OK"      # "OK" or "UNREACHABLE"
    endpoint: str = ""      # connection string

@dataclass
class InfraReport:
    infra_mode: str                       # resolved mode (local / docker)
    adapters: list = field(default_factory=list)  # list[AdapterInfo]

    def table(self) -> str:
        header = f"{'Component':<20} {'Adapter':<20} {'Status':<12} {'Endpoint'}"
        sep = "-" * 80
        rows = [f"{a.component:<20} {a.adapter:<20} {a.status:<12} {a.endpoint}" for a in self.adapters]
        return f"\n{sep}\n INFRA_MODE = {self.infra_mode}\n{sep}\n {header}\n{sep}\n" + "\n".join(f" {r}" for r in rows) + f"\n{sep}\n"

    def to_dict(self) -> dict:
        return {
            "infra_mode": self.infra_mode,
            "adapters": [
                {"component": a.component, "adapter": a.adapter, "status": a.status, "endpoint": a.endpoint}
                for a in self.adapters
            ],
        }

# ---------------------------------------------------------------------------
# Probe helpers
# ---------------------------------------------------------------------------

def _probe_tcp(host: str, port: int, timeout: float = 1.5) -> bool:
    """Return True if a TCP connection to host:port succeeds within timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout, ConnectionRefusedError):
        return False


def _parse_host_port(url: str, default_port: int) -> tuple:
    """Extract (host, port) from various URL formats."""
    if "://" not in url:
        url = f"tcp://{url}"
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or default_port
    return host, port

# ---------------------------------------------------------------------------
# Configuration singleton
# ---------------------------------------------------------------------------

class AppConfig:
    """Resolved application configuration — created once at startup."""

    def __init__(self):
        self.raw_mode: str = os.getenv("INFRA_MODE", "auto").lower().strip()
        assert self.raw_mode in ("auto", "local", "docker"), f"Invalid INFRA_MODE: {self.raw_mode}"

        # Service connection strings from env
        self.redpanda_broker: str = os.getenv("REDPANDA_BROKER", "localhost:19092")
        self.redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.timescaledb_url: str = os.getenv("TIMESCALEDB_URL", "postgresql://copilot:copilot@localhost:5432/clinical_copilot")
        self.qdrant_url: str = os.getenv("QDRANT_URL", "http://localhost:6333")

        # LLM Keys
        self.gemini_api_key: Optional[str] = os.getenv("GEMINI_API_KEY")
        self.openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")

        # API config
        self.api_host: str = os.getenv("API_HOST", "0.0.0.0")
        self.api_port: int = int(os.getenv("API_PORT", "8000"))
        self.streamlit_port: int = int(os.getenv("STREAMLIT_PORT", "8081"))

        # Data directories
        self.data_dir: str = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.sqlite_path: str = os.path.join(self.data_dir, "clinical_copilot.db")
        self.qdrant_local_path: str = os.path.join(self.data_dir, "qdrant_db")

        # Resolve mode & build report
        self.report: InfraReport = self._resolve()

    @property
    def mode(self) -> str:
        return self.report.infra_mode

    def _resolve(self) -> InfraReport:
        if self.raw_mode == "local":
            return self._build_local_report()
        elif self.raw_mode == "docker":
            return self._build_docker_report()
        else:
            return self._auto_probe()

    def _auto_probe(self) -> InfraReport:
        """Probe each Docker service; fall back to local if any is unreachable."""
        rp_host, rp_port = _parse_host_port(self.redpanda_broker, 19092)
        redis_host, redis_port = _parse_host_port(self.redis_url, 6379)
        pg_host, pg_port = _parse_host_port(self.timescaledb_url, 5432)
        qd_host, qd_port = _parse_host_port(self.qdrant_url, 6333)

        probes = {
            "redpanda": _probe_tcp(rp_host, rp_port),
            "redis": _probe_tcp(redis_host, redis_port),
            "timescaledb": _probe_tcp(pg_host, pg_port),
            "qdrant": _probe_tcp(qd_host, qd_port),
        }

        if all(probes.values()):
            logger.info("Auto-probe: all Docker services reachable → INFRA_MODE=docker")
            return self._build_docker_report()
        else:
            unreachable = [k for k, v in probes.items() if not v]
            logger.info(f"Auto-probe: unreachable services {unreachable} → INFRA_MODE=local")
            return self._build_local_report()

    def _build_docker_report(self) -> InfraReport:
        return InfraReport(
            infra_mode="docker",
            adapters=[
                AdapterInfo("Stream Broker", "Redpanda", "OK", self.redpanda_broker),
                AdapterInfo("State Cache", "Redis", "OK", self.redis_url),
                AdapterInfo("Timeseries/Audit DB", "TimescaleDB", "OK", self.timescaledb_url),
                AdapterInfo("Vector KB", "Qdrant (Remote)", "OK", self.qdrant_url),
            ],
        )

    def _build_local_report(self) -> InfraReport:
        return InfraReport(
            infra_mode="local",
            adapters=[
                AdapterInfo("Stream Broker", "InProcess", "OK", "thread-safe deque"),
                AdapterInfo("State Cache", "InProcess", "OK", "thread-safe dict"),
                AdapterInfo("Timeseries/Audit DB", "SQLite WAL", "OK", self.sqlite_path),
                AdapterInfo("Vector KB", "Qdrant (Embedded)", "OK", self.qdrant_local_path),
            ],
        )


# Module-level singleton — created on first import
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Return the global AppConfig singleton, creating it on first call."""
    global _config
    if _config is None:
        _config = AppConfig()
        logger.info(f"Infrastructure resolved:\n{_config.report.table()}")
    return _config


def reset_config():
    """Reset the config singleton (used in tests)."""
    global _config
    _config = None
