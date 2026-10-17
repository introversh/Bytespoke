"""
Comprehensive unit and integration test suite for the resilient pipeline engine.
Tests DAG dependencies, retry mechanisms, recovery handlers, conditional branching,
SQL persistence, AI interface isolation, edge cases, and RCA generation.
"""

import os
import unittest
from pathlib import Path

from ai.factory import get_rca_enhancer
from ai.heuristic_provider import HeuristicRCAEnhancer
from ai.llm_provider import LLMRCAEnhancer
from db.database import DatabaseManager
from pipeline.engine import Pipeline, PipelineContext, PipelineExecutionError, RetryPolicy, Step
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

TEST_DB_PATH = Path("test_pipeline_unit.db")


class TestResilientPipeline(unittest.TestCase):

    def setUp(self):
        reset_validation_tracker()
        if TEST_DB_PATH.exists():
            try:
                TEST_DB_PATH.unlink()
            except Exception:
                pass
        self.db = DatabaseManager(TEST_DB_PATH)

    def tearDown(self):
        if TEST_DB_PATH.exists():
            try:
                TEST_DB_PATH.unlink()
            except Exception:
                pass

    def test_topological_sort_and_dependencies(self):
        """Verifies topological sorting and dependency enforcement."""
        p = Pipeline("TestTopological")
        execution_order = []

        def step_action(name):
            def _action(ctx):
                execution_order.append(name)
                return {"done": name}
            return _action

        p.add_step(Step("StepA", step_action("StepA"), dependencies=[]))
        p.add_step(Step("StepC", step_action("StepC"), dependencies=["StepB"]))
        p.add_step(Step("StepB", step_action("StepB"), dependencies=["StepA"]))

        ctx, meta = p.execute()
        self.assertEqual(execution_order, ["StepA", "StepB", "StepC"])
        self.assertEqual(meta.overall_status, PipelineStatus.SUCCESS)

    def test_cyclic_dependency_detection(self):
        """Verifies that cycles in step dependencies raise an error."""
        p = Pipeline("CyclicPipeline")
        p.add_step(Step("A", lambda ctx: {}, dependencies=["B"]))
        p.add_step(Step("B", lambda ctx: {}, dependencies=["A"]))

        with self.assertRaises(ValueError) as context:
            p.execute()
        self.assertIn("Cyclic dependency detected", str(context.exception))

    def test_missing_dependency_detection(self):
        """Verifies that unknown prerequisite dependencies raise a descriptive error."""
        p = Pipeline("MissingDepPipeline")
        p.add_step(Step("A", lambda ctx: {}, dependencies=["NonExistentStep"]))

        with self.assertRaises(ValueError) as context:
            p.execute()
        self.assertIn("unknown dependency", str(context.exception))

    def test_retries_and_recovery_from_transient_failure(self):
        """Verifies that a step experiencing transient errors retries and successfully recovers."""
        p = Pipeline("RetryRecoveryPipeline")
        attempts_seen = []

        def flaky_action(ctx):
            attempts_seen.append(len(attempts_seen) + 1)
            if len(attempts_seen) < 3:
                raise ConnectionResetError(f"Transient connection glitch attempt {len(attempts_seen)}")
            return {"recovered": True, "attempts_needed": len(attempts_seen)}

        p.add_step(
            Step(
                name="FlakyStep",
                action=flaky_action,
                retry_policy=RetryPolicy(
                    max_retries=3,
                    initial_delay_sec=0.01,
                    backoff_factor=1.5,
                    jitter=False,
                    retryable_exceptions=(ConnectionResetError,),
                ),
            )
        )

        ctx, meta = p.execute()
        self.assertEqual(len(attempts_seen), 3)
        self.assertEqual(meta.overall_status, PipelineStatus.DEGRADED_SUCCESS)
        self.assertTrue(meta.recovery_occurred)
        self.assertEqual(meta.steps["FlakyStep"].status, StepStatus.RECOVERED)
        self.assertEqual(meta.steps["FlakyStep"].attempts, 3)
        self.assertEqual(len(meta.steps["FlakyStep"].retry_history), 2)

    def test_fallback_handler_on_retries_exhausted(self):
        """Verifies that when retries are exhausted, fallback handler restores degraded execution."""
        p = Pipeline("FallbackPipeline")

        def always_failing_action(ctx):
            raise TimeoutError("External API permanently down")

        def fallback_recovery(ctx, err):
            return {"cached_data": True, "recovered_via": "stale_cache"}

        p.add_step(
            Step(
                name="ExternalServiceCall",
                action=always_failing_action,
                retry_policy=RetryPolicy(max_retries=2, initial_delay_sec=0.01, jitter=False),
                fallback_handler=fallback_recovery,
            )
        )

        ctx, meta = p.execute()
        self.assertEqual(meta.overall_status, PipelineStatus.DEGRADED_SUCCESS)
        self.assertTrue(meta.recovery_occurred)
        self.assertEqual(meta.steps["ExternalServiceCall"].status, StepStatus.RECOVERED)
        self.assertEqual(ctx.get("recovered_via"), "stale_cache")

    def test_conditional_branching_clean_batch(self):
        """Verifies that clean data without recovery executes standard settlement and skips quarantine."""
        p = Pipeline("CleanBranchPipeline")

        p.add_step(Step("Step1", lambda ctx: {"has_anomalies": False}, dependencies=[]))

        def clean_step(ctx):
            ctx.set("has_anomalies", False)
            ctx.set_flag("recovery_occurred", False)
            return {"status": "CLEAN"}

        p.add_step(Step("Step2", clean_step, dependencies=["Step1"]))
        p.add_step(
            Step(
                "StandardPath",
                lambda ctx: {"path": "STANDARD"},
                dependencies=["Step2"],
                condition=lambda ctx: not ctx.get_flag("recovery_occurred", False) and not ctx.get("has_anomalies", False),
            )
        )
        p.add_step(
            Step(
                "QuarantinePath",
                lambda ctx: {"path": "QUARANTINE"},
                dependencies=["Step2"],
                condition=lambda ctx: ctx.get_flag("recovery_occurred", False) or ctx.get("has_anomalies", False),
            )
        )
        p.add_step(
            Step(
                "Finalize",
                lambda ctx: {"final": True},
                dependencies=["StandardPath", "QuarantinePath"],
                dependency_mode="ONE_OF",
            )
        )

        ctx, meta = p.execute()
        self.assertEqual(meta.steps["StandardPath"].status, StepStatus.SUCCESS)
        self.assertEqual(meta.steps["QuarantinePath"].status, StepStatus.SKIPPED)
        self.assertEqual(meta.steps["Finalize"].status, StepStatus.SUCCESS)
        self.assertEqual(meta.overall_status, PipelineStatus.SUCCESS)

    def test_sql_persistence_and_queries(self):
        """Verifies that operational metadata, transactions, and batches are recorded in SQL."""
        p = Pipeline("SqlPersistenceTest", pipeline_id="sql-run-101")
        p.add_step(Step("Ingest", ingest_raw_telemetry, dependencies=[]))
        p.add_step(
            Step(
                "Validate",
                validate_and_enrich,
                dependencies=["Ingest"],
                retry_policy=RetryPolicy(max_retries=1, initial_delay_sec=0.01),
            )
        )

        ctx, meta = p.execute(initial_data={"db_manager": self.db, "simulate_transient_failure": False})
        self.db.save_operational_metadata(meta, archive_seal="SEAL-TEST-101")

        with self.db.get_connection() as conn:
            run_row = conn.execute("SELECT * FROM pipeline_runs WHERE run_id = 'sql-run-101'").fetchone()
            self.assertIsNotNone(run_row)
            self.assertEqual(run_row["pipeline_name"], "SqlPersistenceTest")
            self.assertEqual(run_row["archive_seal"], "SEAL-TEST-101")

            txns = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            self.assertGreater(txns, 0)

            steps_count = conn.execute("SELECT COUNT(*) FROM pipeline_steps WHERE run_id = 'sql-run-101'").fetchone()[0]
            self.assertEqual(steps_count, 2)

    def test_ai_interface_isolation_and_fallback(self):
        """Verifies that AI calls are isolated behind an interface with reliable fallback."""
        meta_sample = Pipeline("SampleAI").metadata_collector.metadata

        # 1. Direct heuristic engine
        heuristic = HeuristicRCAEnhancer()
        insights_h = heuristic.analyze_incident(meta_sample)
        self.assertIn("INC-", insights_h.incident_reference)
        self.assertEqual(len(insights_h.five_whys), 5)

        # 2. LLM Provider with unconfigured/unreachable port falls back safely without raising
        llm = LLMRCAEnhancer(api_base="http://127.0.0.1:54321/v1", api_key="test-key")
        insights_l = llm.analyze_incident(meta_sample)
        self.assertIsNotNone(insights_l.executive_summary)
        self.assertIn("Fallback", insights_l.provider_name)

    def test_edge_case_empty_input_trapped_safely(self):
        """Verifies that an edge case (empty/missing input payload) raises graceful PipelineExecutionError."""
        p = Pipeline("EdgeCaseEmptyPayload")

        def corrupt_ingest(ctx):
            ctx.set("raw_records", [])
            return {}

        p.add_step(Step("CorruptIngest", corrupt_ingest, dependencies=[]))
        p.add_step(
            Step(
                "ValidateStep",
                validate_and_enrich,
                dependencies=["CorruptIngest"],
                retry_policy=RetryPolicy(max_retries=1, initial_delay_sec=0.01),
            )
        )

        with self.assertRaises(PipelineExecutionError) as err_ctx:
            p.execute(initial_data={"db_manager": self.db})
        self.assertIn("No raw records found", str(err_ctx.exception))

    def test_full_four_step_pipeline_scenario_and_rca(self):
        """Integration test for the full 4-step pipeline with transient recovery, branch selection, and RCA."""
        p = Pipeline("PaymentAndTelemetryIngestionEngine", pipeline_id="integration-test-run")

        step1 = Step("IngestRawTelemetry", ingest_raw_telemetry, dependencies=[])
        step2 = Step(
            "ValidateAndEnrich",
            validate_and_enrich,
            dependencies=["IngestRawTelemetry"],
            retry_policy=RetryPolicy(
                max_retries=3,
                initial_delay_sec=0.04,
                backoff_factor=1.5,
                jitter=False,
                retryable_exceptions=(TransientUpstreamTimeoutError,),
            ),
        )
        step3a = Step(
            "StandardSettlementDispatch",
            standard_settlement_dispatch,
            dependencies=["ValidateAndEnrich"],
            condition=standard_settlement_predicate,
        )
        step3b = Step(
            "QuarantineAndAuditDispatch",
            quarantine_and_audit_dispatch,
            dependencies=["ValidateAndEnrich"],
            condition=quarantine_and_audit_predicate,
        )
        step4 = Step(
            "FinalizeAndArchive",
            finalize_and_archive,
            dependencies=["StandardSettlementDispatch", "QuarantineAndAuditDispatch"],
            dependency_mode="ONE_OF",
        )

        p.add_step(step1)
        p.add_step(step2)
        p.add_step(step3a)
        p.add_step(step3b)
        p.add_step(step4)

        ctx, meta = p.execute(initial_data={"db_manager": self.db, "simulate_transient_failure": True})

        # Step 1 Assertions
        self.assertEqual(meta.steps["IngestRawTelemetry"].status, StepStatus.SUCCESS)
        self.assertEqual(meta.steps["IngestRawTelemetry"].attempts, 1)

        # Step 2 Assertions (Demonstrates Recovery)
        self.assertEqual(meta.steps["ValidateAndEnrich"].status, StepStatus.RECOVERED)
        self.assertEqual(meta.steps["ValidateAndEnrich"].attempts, 3)
        self.assertEqual(len(meta.steps["ValidateAndEnrich"].retry_history), 2)
        self.assertTrue(meta.recovery_occurred)

        # Step 3 Assertions (Demonstrates Conditional Branching)
        self.assertEqual(meta.steps["StandardSettlementDispatch"].status, StepStatus.SKIPPED)
        self.assertEqual(meta.steps["QuarantineAndAuditDispatch"].status, StepStatus.SUCCESS)

        # Step 4 Assertions (Finalization)
        self.assertEqual(meta.steps["FinalizeAndArchive"].status, StepStatus.SUCCESS)
        self.assertTrue(ctx.get("archive_seal").startswith("SEAL-"))

        # Overall Status
        self.assertEqual(meta.overall_status, PipelineStatus.DEGRADED_SUCCESS)

        # RCA Generation checks
        reporter = RCAReporter(meta, db_manager=self.db)
        rca_md = reporter.generate_markdown_rca()

        self.assertIn("# Incident Root Cause Analysis (RCA) Report", rca_md)
        self.assertIn("## 1. Executive Summary", rca_md)
        self.assertIn("## 2. Operational Impact & Blast Radius", rca_md)
        self.assertIn("## 3. Chronological Event Timeline", rca_md)
        self.assertIn("## 4. Technical Diagnostics & Failure Mechanism", rca_md)
        self.assertIn("## 5. Structured 5-Whys Root Cause Deductions", rca_md)
        self.assertIn("## 6. Recovery & Resilience Verification", rca_md)
        self.assertIn("## 7. Pipeline Step Execution Summary", rca_md)
        self.assertIn("## 8. Corrective and Preventive Actions (CAPA)", rca_md)


if __name__ == "__main__":
    unittest.main()
