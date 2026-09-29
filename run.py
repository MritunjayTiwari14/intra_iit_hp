"""
run.py — Single-command launcher for the Clinical Deterioration Copilot.

Starts:
  1. FastAPI backend server (uvicorn)

Usage:
    python run.py
"""

import os
import sys
import subprocess
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("launcher")


def main():
    project_root = os.path.dirname(os.path.abspath(__file__))

    # Resolve ports
    api_port = int(os.getenv("API_PORT", "8080"))

    processes = []

    try:
        # 1. Start FastAPI backend
        logger.info(f"Starting FastAPI backend on port {api_port}...")
        api_cmd = [
            sys.executable, "-m", "uvicorn",
            "backend.api:app",
            "--host", "0.0.0.0",
            "--port", str(api_port),
            "--reload",
        ]
        api_proc = subprocess.Popen(
            api_cmd,
            cwd=project_root,
        )
        processes.append(("FastAPI", api_proc))

        logger.info(f"""
╔══════════════════════════════════════════════════════════════╗
║   Clinical Deterioration & Escalation Copilot               ║
║                                                              ║
║   Live Real-Time Dashboard:  http://localhost:{api_port}                 ║
║   API Health:                http://localhost:{api_port}/api/health         ║
║   API Docs:                  http://localhost:{api_port}/docs               ║
║                                                              ║
║   Press Ctrl+C to stop all services                          ║
╚══════════════════════════════════════════════════════════════╝
        """)

        # Stream combined output
        while True:
            for name, proc in processes:
                if proc.poll() is not None:
                    logger.warning(f"{name} process exited with code {proc.returncode}")
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("\nShutting down...")
    finally:
        for name, proc in processes:
            if proc.poll() is None:
                logger.info(f"Stopping {name}...")
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        logger.info("All services stopped.")


if __name__ == "__main__":
    main()
