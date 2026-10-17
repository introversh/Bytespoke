"""
Step 2: ValidateAndEnrich
Validates transactional schema and enriches with foreign exchange rates.
Demonstrates transient failure, retry backoff, self-healing recovery, and SQL persistence.
"""

from __future__ import annotations
import time
from typing import Any, Dict, List
from db.database import DatabaseManager
from pipeline.engine import PipelineContext


class TransientUpstreamTimeoutError(ConnectionError):
    """Simulated upstream service connection timeout / pool exhaustion."""
    pass


# Global or context-tracked failure counter for deterministic simulation
_ATTEMPT_TRACKER: Dict[str, int] = {}


def reset_validation_tracker():
    _ATTEMPT_TRACKER.clear()


def validate_and_enrich(context: PipelineContext) -> Dict[str, Any]:
    """
    Validates schema and enriches records.
    Simulates transient upstream failures on initial attempts to test
    retries, operational telemetry capture, automatic recovery, and SQL updates.
    """
    pipeline_id = context.pipeline_id
    current_attempt = _ATTEMPT_TRACKER.get(pipeline_id, 0) + 1
    _ATTEMPT_TRACKER[pipeline_id] = current_attempt

    raw_records: List[Dict[str, Any]] = context.get("raw_records", [])
    if not raw_records:
        raise ValueError("No raw records found in pipeline context.")

    # Check if simulation is disabled (e.g. for pure clean path tests)
    simulate_failure = context.get("simulate_transient_failure", True)
    failure_threshold = context.get("failure_attempts_count", 2)

    if simulate_failure and current_attempt <= failure_threshold:
        time.sleep(0.04)
        raise TransientUpstreamTimeoutError(
            f"Upstream Reference Gateway socket timeout (HTTP 503): "
            f"Connection pool exhausted while fetching FX matrix (Attempt #{current_attempt})"
        )

    # Attempt succeeded: Upstream service responded
    time.sleep(0.04)  # Simulate enrichment latency
    fx_rates = {
        "USD": 1.0,
        "EUR": 1.08,
        "GBP": 1.29,
        "JPY": 0.0067,
        "CAD": 0.73,
        "CHF": 1.12,
    }

    enriched_records = []
    has_anomalies = False

    for record in raw_records:
        curr = record["currency"]
        rate = fx_rates.get(curr, 1.0)
        usd_value = round(record["amount"] * rate, 2)
        is_high_risk = record.get("risk_flag", False) or usd_value > 50000.0

        if is_high_risk:
            has_anomalies = True

        enriched = {
            **record,
            "fx_rate": rate,
            "amount_usd": usd_value,
            "high_risk_flag": 1 if is_high_risk else 0,
            "validation_timestamp": time.time(),
        }
        enriched_records.append(enriched)

    # Persist to SQL
    db_mgr: DatabaseManager = context.get("db_manager") or DatabaseManager()
    db_mgr.update_enriched_transactions(enriched_records)

    # Update context
    context.set("enriched_records", enriched_records)
    context.set("has_anomalies", has_anomalies)
    if current_attempt > 1:
        context.set_flag("recovery_occurred", True)
        context.set_flag("failed_attempts_before_recovery", current_attempt - 1)

    return {
        "status": "VALIDATED_AND_ENRICHED",
        "processed_records": len(enriched_records),
        "recovery_achieved_on_attempt": current_attempt,
        "has_anomalies": has_anomalies,
        "high_risk_count": sum(1 for r in enriched_records if r["high_risk_flag"]),
    }
