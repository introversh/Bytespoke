"""
Synthetic financial telemetry and transactional dataset generator.
Generates compliant, realistic synthetic batches for testing pipeline resilience.
"""

import csv
import random
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent
SAMPLE_CSV_PATH = DATA_DIR / "sample_transactions.csv"

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "CHF"]
ACCOUNTS = [f"ACC-{i:04d}" for i in range(1001, 1025)]


def generate_synthetic_transactions(num_records: int = 50, output_path: Path = SAMPLE_CSV_PATH) -> Path:
    """Generates synthetic transactions with realistic distributions and anomaly edge cases."""
    records = []
    for i in range(1, num_records + 1):
        txn_id = f"TXN-2026-{i:04d}"
        account = random.choice(ACCOUNTS)
        currency = random.choice(CURRENCIES)
        
        # 10% chance of high risk anomaly
        is_high_risk = random.random() < 0.12
        if is_high_risk:
            amount = round(random.uniform(50000.0, 300000.0), 2)
        else:
            amount = round(random.uniform(10.0, 9500.0), 2)

        records.append({
            "txn_id": txn_id,
            "account": account,
            "amount": amount,
            "currency": currency,
            "risk_flag": "true" if is_high_risk else "false",
        })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["txn_id", "account", "amount", "currency", "risk_flag"])
        writer.writeheader()
        writer.writerows(records)

    return output_path


if __name__ == "__main__":
    path = generate_synthetic_transactions(60)
    print(f"Generated synthetic transactions at {path}")
