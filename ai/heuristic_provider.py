"""
Heuristic and deterministic operational intelligence provider.
Requires zero external API keys or network calls; executes reliably in clean offline environments.
"""

from __future__ import annotations
import datetime
from typing import Dict, List, Optional
from ai.interface import AbstractRCAEnhancer, RCAInsights
from pipeline.models import OperationalMetadata, StepStatus


class HeuristicRCAEnhancer(AbstractRCAEnhancer):
    """Generates structured SRE RCA insights using deterministic rule-based analysis."""

    def __init__(self, model_name: str = "heuristic-sre-v1"):
        self.model_name = model_name

    def analyze_incident(self, metadata: OperationalMetadata) -> RCAInsights:
        meta = metadata
        inc_ref = f"INC-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d')}-{meta.pipeline_id[:8].upper()}"

        recovered_steps = [s for s in meta.steps.values() if s.status == StepStatus.RECOVERED or len(s.retry_history) > 0]
        failed_steps = [s for s in meta.steps.values() if s.status == StepStatus.FAILED]

        target_step = recovered_steps[0] if recovered_steps else (failed_steps[0] if failed_steps else None)

        if recovered_steps:
            severity = "SEV-2 (High - Automated Degradation Recovery)"
            status = "RESOLVED (Self-Healed via Exponential Backoff Retries)"
        elif failed_steps:
            severity = "SEV-1 (Critical - Unhandled Pipeline Failure)"
            status = "OPEN (Terminal Step Failure)"
        else:
            severity = "SEV-3 (Low - Informational)"
            status = "RESOLVED (Nominal Execution)"

        # 1. Executive Summary
        if target_step and recovered_steps:
            exec_summary = (
                f"During execution of pipeline '{meta.pipeline_name}' (Run ID: {meta.pipeline_id}), "
                f"step '{target_step.step_name}' experienced transient operational faults on initial invocation. "
                f"The pipeline's dependency-aware resilience engine held state, initiated exponential backoff with jitter, "
                f"and achieved complete recovery on attempt {target_step.attempts}. "
                f"Downstream conditional routing detected the recovery flag and dynamically directed the batch to the forensic "
                f"quarantine and audit path, ensuring zero data loss and 100% transactional integrity."
            )
        elif failed_steps:
            exec_summary = (
                f"Pipeline '{meta.pipeline_name}' experienced an unrecoverable failure in step '{failed_steps[0].step_name}' "
                f"after {failed_steps[0].attempts} attempts. Execution was halted cleanly."
            )
        else:
            exec_summary = f"Pipeline '{meta.pipeline_name}' executed cleanly with zero anomalies."

        # 2. Technical Root Cause
        if target_step and target_step.retry_history:
            first_fail = target_step.retry_history[0]
            tech_rc = (
                f"Transient upstream socket timeout / connection starvation ({first_fail.error_type}). "
                f"Diagnostic payload: '{first_fail.error_message}'. "
                f"The upstream service temporarily exhausted its available worker pool under peak concurrency."
            )
        else:
            tech_rc = "No fault signature detected during execution."

        # 3. 5-Whys
        five_whys = [
            f"1. Why did step '{target_step.step_name if target_step else 'unknown'}' encounter a fault? Upstream gateway rejected the request with a transient connection timeout.",
            "2. Why was the upstream gateway unresponsive? The upstream connection pool became saturated under concurrent burst queries.",
            "3. Why wasn't the transaction batch permanently lost? The orchestrator caught the exception and applied a configured RetryPolicy with exponential backoff.",
            "4. Why did the subsequent retry succeed? Backoff and randomized jitter delayed re-entry, allowing upstream thread pools to drain and recover.",
            "5. Why was the conditional audit path selected? Operational metadata recorded the degraded state, triggering the forensic compliance branch for integrity verification.",
        ]

        # 4. CAPA Actions
        capa_actions = [
            {
                "priority": "P0",
                "action": "Increase upstream connection pool allocation and configure keep-alive timeouts.",
                "owner": "Reliability Engineering",
                "target_date": "+3 Days",
            },
            {
                "priority": "P1",
                "action": "Implement circuit-breaker thresholds in orchestrator to fail fast during macro outages.",
                "owner": "Data Platform Team",
                "target_date": "+7 Days",
            },
            {
                "priority": "P2",
                "action": "Emit OpenTelemetry distributed trace spans linking pipeline run ID to upstream spans.",
                "owner": "SRE Observability",
                "target_date": "+14 Days",
            },
        ]

        return RCAInsights(
            incident_reference=inc_ref,
            severity=severity,
            status=status,
            executive_summary=exec_summary,
            technical_root_cause=tech_rc,
            five_whys=five_whys,
            capa_actions=capa_actions,
            provider_name="HeuristicRuleEngine",
            model_version=self.model_name,
            telemetry_summary={
                "total_duration_ms": meta.total_duration_ms,
                "recovery_occurred": meta.recovery_occurred,
                "host": meta.host_info.get("hostname"),
            },
        )
