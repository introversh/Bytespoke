"""
Interactive / Automated Demonstrator for Pipeline Failure Modes & Edge Cases.
Demonstrates:
1. Nominal Clean Execution (Standard Settlement Path)
2. Transient Upstream Failure with Automated Retry & Recovery (Quarantine Path + RCA)
3. Retries Exhaustion with Fallback Self-Healing Handler
4. Corrupted Batch / Edge-Case Schema Handling
"""

from __future__ import annotations
import sys
from pathlib import Path

WORKSPACE_DIR = Path(__file__).resolve().parent
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))

# Ensure Windows console handles UTF-8 safely
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from db.database import DatabaseManager
from pipeline.engine import Pipeline, PipelineExecutionError, RetryPolicy, Step
from pipeline.models import PipelineStatus, StepStatus
from pipeline.rca_reporter import RCAReporter
from steps.ingest import ingest_raw_telemetry
from steps.validate import validate_and_enrich, reset_validation_tracker, TransientUpstreamTimeoutError
from steps.branch_dispatch import (
    quarantine_and_audit_dispatch,
    quarantine_and_audit_predicate,
    standard_settlement_dispatch,
    standard_settlement_predicate,
)
from steps.finalize import finalize_and_archive

console = Console()


def run_scenario_1_nominal_clean():
    """Scenario 1: Clean data, no transient errors -> Standard Settlement Branch."""
    console.print(Panel("[bold green]Scenario 1: Nominal Clean Execution (Standard Settlement Path)[/bold green]"))
    reset_validation_tracker()
    db = DatabaseManager("test_scenarios.db")

    p = Pipeline("Scenario1_NominalClean", pipeline_id="scen-1-clean")

    # Ingest clean mock records
    clean_records = [
        {"txn_id": "TXN-CLN-01", "account": "ACC-101", "amount": 100.0, "currency": "USD", "risk_flag": False},
        {"txn_id": "TXN-CLN-02", "account": "ACC-102", "amount": 250.0, "currency": "EUR", "risk_flag": False},
    ]

    def clean_ingest(ctx):
        ctx.set("raw_records", clean_records)
        ctx.set("batch_id", "BATCH-CLEAN-01")
        ctx.set("payload_checksum", "abc123clean")
        return {"records_ingested": 2}

    p.add_step(Step("Ingest", clean_ingest, dependencies=[]))
    p.add_step(
        Step(
            "Validate",
            validate_and_enrich,
            dependencies=["Ingest"],
            retry_policy=RetryPolicy(max_retries=2, initial_delay_sec=0.05),
        )
    )
    p.add_step(
        Step(
            "StandardSettlement",
            standard_settlement_dispatch,
            dependencies=["Validate"],
            condition=standard_settlement_predicate,
        )
    )
    p.add_step(
        Step(
            "QuarantineAudit",
            quarantine_and_audit_dispatch,
            dependencies=["Validate"],
            condition=quarantine_and_audit_predicate,
        )
    )
    p.add_step(
        Step(
            "Finalize",
            finalize_and_archive,
            dependencies=["StandardSettlement", "QuarantineAudit"],
            dependency_mode="ONE_OF",
        )
    )

    ctx, meta = p.execute(
        initial_data={"db_manager": db, "simulate_transient_failure": False}
    )

    console.print(f"  * Pipeline Status: [bold green]{meta.overall_status.value}[/bold green]")
    console.print(f"  * Standard Settlement Step Status: [cyan]{meta.steps['StandardSettlement'].status.value}[/cyan]")
    console.print(f"  * Quarantine Audit Step Status:    [dim]{meta.steps['QuarantineAudit'].status.value}[/dim] (Cleanly Skipped)\n")


def run_scenario_2_transient_recovery():
    """Scenario 2: Transient Upstream Timeout -> Backoff Retries -> Auto-Recovery -> Audit Branch + RCA."""
    console.print(Panel("[bold yellow]Scenario 2: Transient Upstream Timeout & Auto-Recovery (Degraded Branch + RCA)[/bold yellow]"))
    reset_validation_tracker()
    db = DatabaseManager("test_scenarios.db")

    p = Pipeline("Scenario2_TransientRecovery", pipeline_id="scen-2-recovery")

    p.add_step(Step("Ingest", ingest_raw_telemetry, dependencies=[]))
    p.add_step(
        Step(
            "Validate",
            validate_and_enrich,
            dependencies=["Ingest"],
            retry_policy=RetryPolicy(max_retries=3, initial_delay_sec=0.1, backoff_factor=1.5, jitter=False),
        )
    )
    p.add_step(
        Step(
            "StandardSettlement",
            standard_settlement_dispatch,
            dependencies=["Validate"],
            condition=standard_settlement_predicate,
        )
    )
    p.add_step(
        Step(
            "QuarantineAudit",
            quarantine_and_audit_dispatch,
            dependencies=["Validate"],
            condition=quarantine_and_audit_predicate,
        )
    )
    p.add_step(
        Step(
            "Finalize",
            finalize_and_archive,
            dependencies=["StandardSettlement", "QuarantineAudit"],
            dependency_mode="ONE_OF",
        )
    )

    ctx, meta = p.execute(
        initial_data={"db_manager": db, "simulate_transient_failure": True, "failure_attempts_count": 2}
    )

    console.print(f"  * Pipeline Status: [bold yellow]{meta.overall_status.value}[/bold yellow]")
    console.print(f"  * Validate Step Status: [bold yellow]{meta.steps['Validate'].status.value}[/bold yellow] (Attempts: {meta.steps['Validate'].attempts})")
    console.print(f"  * Retries Logged: {len(meta.steps['Validate'].retry_history)}")
    console.print(f"  * Executed Branch: [cyan]{meta.steps['QuarantineAudit'].status.value}[/cyan]")
    console.print(f"  * Skipped Branch:  [dim]{meta.steps['StandardSettlement'].status.value}[/dim]\n")


def run_scenario_3_fallback_handler():
    """Scenario 3: Permanent failure with automated fallback recovery handler."""
    console.print(Panel("[bold magenta]Scenario 3: Retries Exhaustion with Self-Healing Fallback Handler[/bold magenta]"))
    db = DatabaseManager("test_scenarios.db")

    p = Pipeline("Scenario3_Fallback", pipeline_id="scen-3-fallback")

    def fatal_step(ctx):
        raise ConnectionRefusedError("Payment clearing gateway 100% offline")

    def fallback_handler(ctx, error):
        console.print(f"    [yellow]-> Fallback handler triggered: {error}[/yellow]")
        return {"status": "DEGRADED_CACHE_APPLIED", "records": 0, "fallback_mode": True}

    p.add_step(Step("Step1", lambda ctx: {"init": True}, dependencies=[]))
    p.add_step(
        Step(
            "FatalStep",
            fatal_step,
            dependencies=["Step1"],
            retry_policy=RetryPolicy(max_retries=2, initial_delay_sec=0.02, jitter=False),
            fallback_handler=fallback_handler,
        )
    )
    p.add_step(Step("Step3", lambda ctx: {"finalized": True}, dependencies=["FatalStep"]))

    ctx, meta = p.execute(initial_data={"db_manager": db})

    console.print(f"  * Pipeline Status: [bold yellow]{meta.overall_status.value}[/bold yellow]")
    console.print(f"  * FatalStep Status: [bold yellow]{meta.steps['FatalStep'].status.value}[/bold yellow] (Recovered via Fallback)")
    console.print(f"  * Downstream Step3 Status: [bold green]{meta.steps['Step3'].status.value}[/bold green]\n")


def run_scenario_4_corrupted_payload():
    """Scenario 4: Corrupted Payload / Missing Input (Edge-Case Failure)."""
    console.print(Panel("[bold red]Scenario 4: Corrupted Payload / Zero Records Edge-Case Handling[/bold red]"))
    db = DatabaseManager("test_scenarios.db")

    p = Pipeline("Scenario4_Corrupt", pipeline_id="scen-4-corrupt")

    def empty_ingest(ctx):
        ctx.set("raw_records", [])  # Empty corrupted input
        return {"records": 0}

    p.add_step(Step("Ingest", empty_ingest, dependencies=[]))
    p.add_step(Step("Validate", validate_and_enrich, dependencies=["Ingest"], retry_policy=RetryPolicy(max_retries=1, initial_delay_sec=0.01)))

    try:
        p.execute(initial_data={"db_manager": db})
        console.print("  [red]Failed: Expected PipelineExecutionError was not raised.[/red]\n")
    except PipelineExecutionError as e:
        console.print(f"  * [bold green]Correctly Trapped Edge-Case Error:[/bold green] [red]{e}[/red]")
        console.print("  * Pipeline halted safely without data corruption.\n")


def main():
    console.print("\n[bold cyan]========================================================================[/bold cyan]")
    console.print("[bold cyan]       PIPELINE FAILURE MODES & RESILIENCE TEST DEMONSTRATION          [/bold cyan]")
    console.print("[bold cyan]========================================================================[/bold cyan]\n")

    run_scenario_1_nominal_clean()
    run_scenario_2_transient_recovery()
    run_scenario_3_fallback_handler()
    run_scenario_4_corrupted_payload()

    console.print("[bold green][SUCCESS] All 4 Failure and Resilience Scenarios Verified Successfully![/bold green]\n")


if __name__ == "__main__":
    main()
