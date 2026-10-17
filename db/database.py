"""
Database connection manager and persistence service for SQL models and operational metadata.
Uses SQLite 3 with strict foreign key constraints and transactional integrity.
"""

from __future__ import annotations
import contextlib
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
import uuid

from pipeline.models import OperationalMetadata, StepStatus

DB_PATH = Path(os.environ.get("PIPELINE_DB_PATH", "pipeline_ops.db"))
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


class DatabaseManager:
    """Manages SQLite database connections, migrations, and operational queries."""

    def __init__(self, db_path: Path | str = DB_PATH):
        self.db_path = Path(db_path)
        self.init_schema()

    @contextlib.contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_schema(self) -> None:
        """Applies SQL schema file to initialize database tables and views."""
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            ddl = f.read()

        with self.get_connection() as conn:
            conn.executescript(ddl)

    def insert_batch(self, batch_id: str, record_count: int, checksum: str, source_file: str = "synthetic") -> None:
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO batches (batch_id, record_count, payload_checksum, source_file, status, ingested_at)
                VALUES (?, ?, ?, ?, 'INGESTED', datetime('now', 'utc'))
                """,
                (batch_id, record_count, checksum, source_file),
            )

    def insert_transactions(self, batch_id: str, records: List[Dict[str, Any]]) -> None:
        with self.get_connection() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO transactions (txn_id, batch_id, account_id, amount, currency, status)
                VALUES (:txn_id, :batch_id, :account, :amount, :currency, 'PENDING')
                """,
                [{"batch_id": batch_id, **rec} for rec in records],
            )

    def update_enriched_transactions(self, records: List[Dict[str, Any]]) -> None:
        with self.get_connection() as conn:
            conn.executemany(
                """
                UPDATE transactions
                SET fx_rate = :fx_rate,
                    amount_usd = :amount_usd,
                    is_high_risk = :high_risk_flag,
                    status = 'VALIDATED',
                    validated_at = datetime('now', 'utc')
                WHERE txn_id = :txn_id
                """,
                records,
            )

    def update_transaction_status(self, txn_ids: List[str], new_status: str) -> None:
        if not txn_ids:
            return
        with self.get_connection() as conn:
            placeholders = ",".join("?" for _ in txn_ids)
            conn.execute(
                f"UPDATE transactions SET status = ? WHERE txn_id IN ({placeholders})",
                [new_status, *txn_ids],
            )

    def save_operational_metadata(self, metadata: OperationalMetadata, archive_seal: Optional[str] = None) -> None:
        """Persists full operational telemetry and event log to SQL tables."""
        meta_dict = metadata.to_dict()
        host = metadata.host_info

        with self.get_connection() as conn:
            # 1. Pipeline Run
            conn.execute(
                """
                INSERT OR REPLACE INTO pipeline_runs 
                (run_id, pipeline_name, overall_status, start_time, end_time, total_duration_ms, 
                 recovery_occurred, recovered_step, host_name, os_name, python_version, process_id, memory_rss_mb, archive_seal)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metadata.pipeline_id,
                    metadata.pipeline_name,
                    metadata.overall_status.value,
                    metadata.start_time,
                    metadata.end_time,
                    metadata.total_duration_ms,
                    1 if metadata.recovery_occurred else 0,
                    metadata.recovery_details.get("recovered_step") if metadata.recovery_details else None,
                    host.get("hostname"),
                    host.get("os"),
                    host.get("python_version"),
                    host.get("process_id"),
                    host.get("memory", {}).get("rss_mb") if isinstance(host.get("memory", {}).get("rss_mb"), (int, float)) else None,
                    archive_seal,
                ),
            )

            # 2. Pipeline Steps
            for step_name, step in metadata.steps.items():
                step_id = f"{metadata.pipeline_id}_{step.step_name}"
                conn.execute(
                    """
                    INSERT OR REPLACE INTO pipeline_steps
                    (step_id, run_id, step_name, step_index, status, attempts, max_retries, 
                     start_time, end_time, duration_ms, recovery_notes, skip_reason, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        step_id,
                        metadata.pipeline_id,
                        step.step_name,
                        step.step_index,
                        step.status.value,
                        step.attempts,
                        step.max_retries,
                        step.start_time,
                        step.end_time,
                        step.duration_ms,
                        step.recovery_notes,
                        step.skip_reason,
                        step.error,
                    ),
                )

                # 3. Step Retries
                for r in step.retry_history:
                    retry_id = f"{step_id}_att_{r.attempt_number}"
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO step_retries
                        (retry_id, run_id, step_name, attempt_number, error_type, error_message, backoff_delay_sec, stack_trace_snippet, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            retry_id,
                            metadata.pipeline_id,
                            step.step_name,
                            r.attempt_number,
                            r.error_type,
                            r.error_message,
                            r.delay_seconds,
                            (r.stack_trace[:300] if r.stack_trace else None),
                            r.timestamp,
                        ),
                    )

            # 4. Pipeline Events
            event_rows = [
                (
                    metadata.pipeline_id,
                    ev.step_name,
                    ev.level,
                    ev.message,
                    json.dumps(ev.details),
                    ev.timestamp,
                )
                for ev in metadata.events
            ]
            conn.executemany(
                """
                INSERT INTO pipeline_events (run_id, step_name, level, message, details_json, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                event_rows,
            )

    def save_rca_record(
        self,
        run_id: str,
        incident_ref: str,
        severity: str,
        status: str,
        executive_summary: str,
        root_cause_technical: str,
        five_whys: List[str],
        capa_actions: List[Dict[str, str]],
    ) -> None:
        rca_id = f"RCA-{run_id[:12]}"
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO rca_records
                (rca_id, run_id, incident_ref, severity, status, executive_summary, 
                 root_cause_technical, five_whys_json, capa_actions_json, generated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'utc'))
                """,
                (
                    rca_id,
                    run_id,
                    incident_ref,
                    severity,
                    status,
                    executive_summary,
                    root_cause_technical,
                    json.dumps(five_whys),
                    json.dumps(capa_actions),
                ),
            )

    def fetch_run_summary(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            row = conn.execute("SELECT * FROM v_pipeline_run_summary WHERE run_id = ?", (run_id,)).fetchone()
            return dict(row) if row else None
