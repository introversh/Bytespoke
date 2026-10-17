"""
Main entrypoint demonstrating the 4-step resilient pipeline:
1. IngestRawTelemetry (Reads synthetic CSV & inserts to SQLite)
2. ValidateAndEnrich (Demonstrates simulated transient failure, retries, and recovery)
3. Conditional Branch: StandardSettlementDispatch vs QuarantineAndAuditDispatch
4. FinalizeAndArchive (Depends on executed branch, seals batch)
Captures operational metadata to SQLite & JSON, and generates an SRE Root Cause Analysis (RCA) note.
"""

from __future__ import annotations
import json
import os
import sys
from pathlib import Path

# Add current workspace directory to sys.path
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
from rich.table import Table
from rich.panel import Panel

from db.database import DatabaseManager
from pipeline.engine import Pipeline, RetryPolicy, Step
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


def build_pipeline(pipeline_id: str | None = None) -> Pipeline:
    """Constructs the four-step DAG pipeline with retries, recovery, and conditional branching."""
    pipeline = Pipeline(name="PaymentAndTelemetryIngestionEngine", pipeline_id=pipeline_id)

    # Step 1: Ingest
    step1 = Step(
        name="IngestRawTelemetry",
        action=ingest_raw_telemetry,
        dependencies=[],
        description="Ingests incoming financial telemetry batches, persists to SQL, and computes payload checksums.",
    )

    # Step 2: Validate & Enrich (Demonstrates failure and retry recovery)
    step2 = Step(
        name="ValidateAndEnrich",
        action=validate_and_enrich,
        dependencies=["IngestRawTelemetry"],
        retry_policy=RetryPolicy(
            max_retries=3,
            initial_delay_sec=0.25,
            backoff_factor=1.8,
            jitter=True,
            retryable_exceptions=(TransientUpstreamTimeoutError,),
        ),
        description="Validates schema and enriches with FX rates. Simulates transient timeout and auto-recovers.",
    )

    # Step 3A: Standard Settlement (Condition: clean batch, non-degraded)
    step3a = Step(
        name="StandardSettlementDispatch",
        action=standard_settlement_dispatch,
        dependencies=["ValidateAndEnrich"],
        condition=standard_settlement_predicate,
        description="Fast automated settlement path for non-anomalous batches.",
    )

    # Step 3B: Quarantine and Audit (Condition: anomalies detected or recovery mode triggered)
    step3b = Step(
        name="QuarantineAndAuditDispatch",
        action=quarantine_and_audit_dispatch,
        dependencies=["ValidateAndEnrich"],
        condition=quarantine_and_audit_predicate,
        description="Strict compliance quarantine path for degraded/high-risk batches.",
    )

    # Step 4: Finalize & Archive (Depends on whichever branch executed)
    step4 = Step(
        name="FinalizeAndArchive",
        action=finalize_and_archive,
        dependencies=["StandardSettlementDispatch", "QuarantineAndAuditDispatch"],
        dependency_mode="ONE_OF",
        description="Cryptographically seals batch records and persists final execution ledger.",
    )

    pipeline.add_step(step1)
    pipeline.add_step(step2)
    pipeline.add_step(step3a)
    pipeline.add_step(step3b)
    pipeline.add_step(step4)

    return pipeline


def main():
    reset_validation_tracker()
    pipeline_id = "run-pl-20261017-8821"

    # Initialize SQL Database
    db_manager = DatabaseManager()

    console.print(
        Panel.fit(
            "[bold cyan]Four-Step Resilient Pipeline Runner (DataOps Edition)[/bold cyan]\n"
            "[white]Features: Python + SQL, DAG Dependencies, Exponential Retries, Conditional Branch, Operational Telemetry & SRE RCA[/white]",
            border_style="cyan",
        )
    )

    console.print("\n[bold yellow]==> Pipeline DAG Architecture:[/bold yellow]")
    console.print("  [bold green][Step 1: IngestRawTelemetry][/bold green] (Root) -> SQL: batches, transactions")
    console.print("        |")
    console.print("        v (Dependency satisfied)")
    console.print("  [bold green][Step 2: ValidateAndEnrich][/bold green] [bold magenta](Retries: 3, Backoff: 1.8x)[/bold magenta] -> [bold red]Simulates Transient Timeout[/bold red]")
    console.print("        |")
    console.print("        v [bold yellow]Conditional Branch Evaluation (Risk / Recovery Check)[/bold yellow]")
    console.print("       +------------------------------+------------------------------+")
    console.print("       v                                                             v")
    console.print("  [dim][Step 3A: StandardSettlement][/dim] (Clean)              [bold cyan][Step 3B: QuarantineAndAuditDispatch][/bold cyan] (Degraded / Risk)")
    console.print("       +------------------------------+------------------------------+")
    console.print("                                      v (Dependency: ONE_OF)")
    console.print("  [bold green][Step 4: FinalizeAndArchive][/bold green] (Cryptographic Seal & SQL Persistence)\n")

    pipeline = build_pipeline(pipeline_id)

    console.print(f"[bold blue]==> Executing pipeline '{pipeline.name}' (Run ID: {pipeline.pipeline_id})...[/bold blue]\n")
    context, metadata = pipeline.execute(initial_data={"db_manager": db_manager})

    # Persist operational metadata to SQL database
    archive_seal = context.get("archive_seal")
    db_manager.save_operational_metadata(metadata, archive_seal=archive_seal)

    # Display step execution table
    table = Table(title="Pipeline Execution & Telemetry Summary", show_header=True, header_style="bold magenta")
    table.add_column("Step #", style="dim", width=8)
    table.add_column("Step Name", style="bold cyan")
    table.add_column("Dependencies")
    table.add_column("Status", justify="center")
    table.add_column("Attempts", justify="center")
    table.add_column("Duration (ms)", justify="right")
    table.add_column("Notes / Reason")

    for step_name, step_data in metadata.steps.items():
        status_style = "green"
        if step_data.status.value == "RECOVERED":
            status_style = "bold yellow"
        elif step_data.status.value == "SKIPPED":
            status_style = "dim"
        elif step_data.status.value == "FAILED":
            status_style = "bold red"

        notes = step_data.recovery_notes or step_data.skip_reason or "Normal Execution"
        deps_str = ", ".join(step_data.dependencies) if step_data.dependencies else "-"
        table.add_row(
            str(step_data.step_index),
            step_data.step_name,
            deps_str,
            f"[{status_style}]{step_data.status.value}[/{status_style}]",
            str(step_data.attempts),
            f"{step_data.duration_ms:.2f}",
            notes,
        )

    console.print(table)

    # Operational metrics summary
    console.print("\n[bold green]==> Pipeline Execution Telemetry & Host Diagnostics:[/bold green]")
    console.print(f"  * Overall Status: [bold { 'yellow' if metadata.overall_status.value == 'DEGRADED_SUCCESS' else 'green' }]{metadata.overall_status.value}[/]")
    console.print(f"  * Total Duration: [bold]{metadata.total_duration_ms:.2f} ms[/bold]")
    console.print(f"  * Auto-Recovery Triggered: [bold yellow]{metadata.recovery_occurred}[/bold yellow]")
    console.print(f"  * Host Machine: {metadata.host_info.get('hostname')} | OS: {metadata.host_info.get('os')}")
    console.print(f"  * Memory (RSS): {metadata.host_info.get('memory', {}).get('rss_mb')} MB | PID: {metadata.host_info.get('process_id')}")
    console.print(f"  * Archive Cryptographic Seal: [bold cyan]{archive_seal}[/bold cyan]")

    # Query and display SQL operational tables
    console.print("\n[bold green]==> SQL Operational Query: [cyan]v_pipeline_run_summary[/cyan][/bold green]")
    summary_row = db_manager.fetch_run_summary(pipeline.pipeline_id)
    if summary_row:
        sql_table = Table(show_header=True, header_style="bold blue")
        for col in summary_row.keys():
            sql_table.add_column(col)
        sql_table.add_row(*[str(val) for val in summary_row.values()])
        console.print(sql_table)

    # Generate RCA Report
    reporter = RCAReporter(metadata, db_manager=db_manager)
    rca_markdown = reporter.generate_markdown_rca()

    # Save artifacts
    artifacts_dir = WORKSPACE_DIR / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = artifacts_dir / "operational_metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata.to_dict(), f, indent=2)

    rca_path = artifacts_dir / "RCA_INCIDENT_REPORT.md"
    with open(rca_path, "w", encoding="utf-8") as f:
        f.write(rca_markdown)

    console.print("\n[bold green][SUCCESS] Artifacts & SQL Records generated successfully:[/bold green]")
    console.print(f"  * SQL Database:         [cyan]{db_manager.db_path.resolve()}[/cyan]")
    console.print(f"  * Operational Metadata: [cyan]{metadata_path}[/cyan]")
    console.print(f"  * RCA Incident Note:    [cyan]{rca_path}[/cyan]\n")

    return metadata, rca_markdown


if __name__ == "__main__":
    main()
