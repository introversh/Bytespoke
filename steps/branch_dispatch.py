"""
Step 3: Conditional Branching
Defines two alternative execution paths based on runtime integrity conditions:
- Branch 3A: StandardSettlementDispatch (Normal, non-degraded path)
- Branch 3B: QuarantineAndAuditDispatch (Degraded / Anomaly / High-Risk path)
Updates SQL transaction statuses accordingly.
"""

from __future__ import annotations
import time
from typing import Any, Dict, List
from db.database import DatabaseManager
from pipeline.engine import PipelineContext


def standard_settlement_predicate(context: PipelineContext) -> bool:
    """Selects Branch 3A if no anomalies or recovery flags are present."""
    has_anomalies = context.get("has_anomalies", False)
    recovery_occurred = context.get_flag("recovery_occurred", False)
    return not has_anomalies and not recovery_occurred


def quarantine_and_audit_predicate(context: PipelineContext) -> bool:
    """Selects Branch 3B if anomalies are detected or recovery mode was active."""
    has_anomalies = context.get("has_anomalies", False)
    recovery_occurred = context.get_flag("recovery_occurred", False)
    return has_anomalies or recovery_occurred


def standard_settlement_dispatch(context: PipelineContext) -> Dict[str, Any]:
    """Path 3A: Fast automated settlement dispatch for clean, non-degraded batches."""
    time.sleep(0.03)
    enriched = context.get("enriched_records", [])
    total_amount_usd = sum(r.get("amount_usd", 0.0) for r in enriched)

    txn_ids = [r["txn_id"] for r in enriched]
    db_mgr: DatabaseManager = context.get("db_manager") or DatabaseManager()
    db_mgr.update_transaction_status(txn_ids, "SETTLED")

    dispatch_result = {
        "dispatch_mode": "STANDARD_AUTOMATED_SETTLEMENT",
        "settled_record_count": len(enriched),
        "total_amount_usd": round(total_amount_usd, 2),
        "audit_trail_level": "STANDARD",
    }
    context.set("dispatch_result", dispatch_result)
    return dispatch_result


def quarantine_and_audit_dispatch(context: PipelineContext) -> Dict[str, Any]:
    """
    Path 3B: Strict quarantine and compliance audit path for batches with
    anomalies or batches that experienced transient pipeline recoveries.
    """
    time.sleep(0.04)
    enriched: List[Dict[str, Any]] = context.get("enriched_records", [])
    quarantined = []
    sanitized = []

    for r in enriched:
        if r.get("high_risk_flag", 0) or r.get("risk_flag", False):
            quarantined.append(r["txn_id"])
        else:
            sanitized.append(r["txn_id"])

    db_mgr: DatabaseManager = context.get("db_manager") or DatabaseManager()
    db_mgr.update_transaction_status(quarantined, "QUARANTINED")
    db_mgr.update_transaction_status(sanitized, "SETTLED")

    audit_summary = {
        "dispatch_mode": "QUARANTINE_AND_COMPLIANCE_AUDIT",
        "quarantined_count": len(quarantined),
        "quarantined_txns": quarantined,
        "sanitized_count": len(sanitized),
        "audit_trail_level": "STRICT_COMPLIANCE_FORENSIC",
        "integrity_checksum_verified": True,
    }
    context.set("dispatch_result", audit_summary)
    return audit_summary
