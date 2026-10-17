"""
Step 1: IngestRawTelemetry
Ingests incoming transactional batches and telemetry records from synthetic CSV or memory,
computes cryptographic payload checksums, and persists to the SQL database.
"""

from __future__ import annotations
import csv
import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict, List
from db.database import DatabaseManager
from pipeline.engine import PipelineContext

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "data" / "sample_transactions.csv"


def ingest_raw_telemetry(context: PipelineContext) -> Dict[str, Any]:
    """Ingests raw records, persists to SQL batches/transactions, and computes checksum."""
    time.sleep(0.06)

    csv_path = Path(context.get("csv_path", DEFAULT_CSV))
    raw_records: List[Dict[str, Any]] = []

    if csv_path.exists():
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_records.append({
                    "txn_id": row["txn_id"],
                    "account": row["account"],
                    "amount": float(row["amount"]),
                    "currency": row["currency"],
                    "risk_flag": row.get("risk_flag", "false").lower() == "true",
                })
    else:
        # Fallback synthetic set
        raw_records = [
            {"txn_id": "TXN-8901", "account": "ACC-1049", "amount": 14500.00, "currency": "USD", "risk_flag": False},
            {"txn_id": "TXN-8902", "account": "ACC-9921", "amount": 250000.00, "currency": "EUR", "risk_flag": True},
            {"txn_id": "TXN-8903", "account": "ACC-3312", "amount": 890.50, "currency": "GBP", "risk_flag": False},
            {"txn_id": "TXN-8904", "account": "ACC-7741", "amount": 12000.00, "currency": "JPY", "risk_flag": False},
        ]

    payload_str = json.dumps(raw_records, sort_keys=True)
    payload_hash = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
    batch_id = f"BATCH-{int(time.time())}"

    # Persist to SQL
    db_mgr: DatabaseManager = context.get("db_manager") or DatabaseManager()
    db_mgr.insert_batch(batch_id, len(raw_records), payload_hash, str(csv_path))
    db_mgr.insert_transactions(batch_id, raw_records)

    # Store in context
    context.set("batch_id", batch_id)
    context.set("raw_records", raw_records)
    context.set("payload_checksum", payload_hash)
    context.set("db_manager", db_mgr)

    return {
        "status": "INGESTED",
        "batch_id": batch_id,
        "records_ingested": len(raw_records),
        "sha256": payload_hash[:12] + "...",
        "source": str(csv_path.name if csv_path.exists() else "in_memory"),
    }
