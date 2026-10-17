"""
Core Directed Acyclic Graph (DAG) Pipeline Engine with Retries,
Conditional Branching, and Recovery capabilities.
"""

from __future__ import annotations
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Type

from pipeline.metadata_collector import MetadataCollector
from pipeline.models import OperationalMetadata, PipelineStatus, StepStatus


class PipelineExecutionError(Exception):
    """Raised when a step fails permanently and cannot recover."""
    pass


class DependencyNotMetError(Exception):
    """Raised when prerequisite dependencies fail or are missing."""
    pass


@dataclass
class RetryPolicy:
    max_retries: int = 3
    initial_delay_sec: float = 0.5
    backoff_factor: float = 2.0
    max_delay_sec: float = 5.0
    jitter: bool = True
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)

    def calculate_delay(self, attempt: int) -> float:
        """Computes exponential backoff with optional jitter."""
        delay = self.initial_delay_sec * (self.backoff_factor ** (attempt - 1))
        delay = min(delay, self.max_delay_sec)
        if self.jitter:
            delay = delay * (0.8 + 0.4 * random.random())
        return delay


class PipelineContext:
    """Thread-safe context for inter-step state sharing and operational telemetry."""

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.data: Dict[str, Any] = {}
        self.step_outputs: Dict[str, Dict[str, Any]] = {}
        self.operational_flags: Dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set_output(self, step_name: str, output: Dict[str, Any]) -> None:
        self.step_outputs[step_name] = output

    def get_output(self, step_name: str, default: Any = None) -> Any:
        return self.step_outputs.get(step_name, default)

    def set_flag(self, flag: str, value: Any) -> None:
        self.operational_flags[flag] = value

    def get_flag(self, flag: str, default: Any = None) -> Any:
        return self.operational_flags.get(flag, default)


@dataclass
class Step:
    """Represents a discrete executable node in the pipeline DAG."""
    name: str
    action: Callable[[PipelineContext], Dict[str, Any]]
    dependencies: List[str] = field(default_factory=list)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    condition: Optional[Callable[[PipelineContext], bool]] = None
    dependency_mode: Literal["ALL", "ONE_OF"] = "ALL"
    fallback_handler: Optional[Callable[[PipelineContext, Exception], Dict[str, Any]]] = None
    description: str = ""


class Pipeline:
    """Orchestrates DAG execution, enforces dependencies, handles retries and branching."""

    def __init__(self, name: str, pipeline_id: Optional[str] = None):
        self.name = name
        self.pipeline_id = pipeline_id or str(uuid.uuid4())
        self.steps: Dict[str, Step] = {}
        self.metadata_collector = MetadataCollector(self.pipeline_id, self.name)

    def add_step(self, step: Step) -> Pipeline:
        if step.name in self.steps:
            raise ValueError(f"Step with name '{step.name}' already registered.")
        self.steps[step.name] = step
        return self

    def _resolve_execution_order(self) -> List[Step]:
        """Resolves topological sort for DAG ordering."""
        in_degree: Dict[str, int] = {name: 0 for name in self.steps}
        graph: Dict[str, List[str]] = {name: [] for name in self.steps}

        for name, step in self.steps.items():
            for dep in step.dependencies:
                if dep not in self.steps:
                    raise ValueError(f"Step '{name}' specifies unknown dependency '{dep}'.")
                graph[dep].append(name)
                in_degree[name] += 1

        queue = [name for name, deg in in_degree.items() if deg == 0]
        ordered_names: List[str] = []

        while queue:
            node = queue.pop(0)
            ordered_names.append(node)
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(ordered_names) != len(self.steps):
            raise ValueError("Cyclic dependency detected in pipeline step definitions.")

        return [self.steps[name] for name in ordered_names]

    def _dependencies_satisfied(self, step: Step, step_statuses: Dict[str, StepStatus]) -> Tuple[bool, str]:
        """Evaluates whether step dependencies are satisfied."""
        if not step.dependencies:
            return True, "No dependencies required"

        successful_states = (StepStatus.SUCCESS, StepStatus.RECOVERED)

        if step.dependency_mode == "ALL":
            for dep in step.dependencies:
                dep_status = step_statuses.get(dep)
                if dep_status not in successful_states:
                    return False, f"Prerequisite step '{dep}' had status '{dep_status}', expected SUCCESS or RECOVERED"
            return True, "All prerequisite dependencies satisfied"

        elif step.dependency_mode == "ONE_OF":
            for dep in step.dependencies:
                if step_statuses.get(dep) in successful_states:
                    return True, f"Prerequisite dependency '{dep}' satisfied under ONE_OF mode"
            return False, f"None of prerequisite dependencies {step.dependencies} were successful"

        return False, "Unknown dependency mode"

    def execute(self, initial_data: Optional[Dict[str, Any]] = None) -> Tuple[PipelineContext, OperationalMetadata]:
        """Executes the pipeline, tracking operational telemetry and handling recoveries."""
        context = PipelineContext(self.pipeline_id)
        if initial_data:
            for k, v in initial_data.items():
                context.set(k, v)

        ordered_steps = self._resolve_execution_order()
        step_statuses: Dict[str, StepStatus] = {}

        # Register all steps with metadata collector
        for idx, step in enumerate(ordered_steps, start=1):
            self.metadata_collector.register_step(
                step_name=step.name,
                step_index=idx,
                dependencies=step.dependencies,
                max_retries=step.retry_policy.max_retries,
            )

        start_time_mono = time.monotonic()
        pipeline_recovered = False

        self.metadata_collector.log_event(
            "INFO", None, f"Pipeline '{self.name}' (ID: {self.pipeline_id}) started execution."
        )

        for step in ordered_steps:
            # 1. Dependency validation
            deps_met, dep_reason = self._dependencies_satisfied(step, step_statuses)
            if not deps_met:
                reason = f"Dependency criteria not met: {dep_reason}"
                self.metadata_collector.record_step_skipped(step.name, reason)
                step_statuses[step.name] = StepStatus.SKIPPED
                continue

            # 2. Conditional branching evaluation
            if step.condition is not None:
                try:
                    should_run = step.condition(context)
                except Exception as cond_err:
                    reason = f"Condition evaluation raised error: {cond_err}"
                    self.metadata_collector.record_step_skipped(step.name, reason)
                    step_statuses[step.name] = StepStatus.SKIPPED
                    continue

                if not should_run:
                    reason = "Branch predicate evaluated to False"
                    self.metadata_collector.record_step_skipped(step.name, reason)
                    step_statuses[step.name] = StepStatus.SKIPPED
                    continue

            # 3. Execution with Retries & Recovery
            self.metadata_collector.record_step_start(step.name, context.data)
            step_start_mono = time.monotonic()
            attempt = 0
            success = False
            last_error: Optional[Exception] = None
            step_output: Dict[str, Any] = {}

            while attempt <= step.retry_policy.max_retries:
                attempt += 1
                try:
                    step_output = step.action(context) or {}
                    success = True
                    break
                except step.retry_policy.retryable_exceptions as err:
                    last_error = err
                    if attempt <= step.retry_policy.max_retries:
                        delay = step.retry_policy.calculate_delay(attempt)
                        self.metadata_collector.record_retry(
                            step_name=step.name,
                            attempt=attempt,
                            error=err,
                            delay_seconds=delay,
                        )
                        time.sleep(delay)
                    else:
                        # Retries exhausted
                        break

            step_duration_ms = (time.monotonic() - step_start_mono) * 1000

            if success:
                context.set_output(step.name, step_output)
                for k, v in step_output.items():
                    context.set(k, v)

                if attempt > 1:
                    # Successful recovery through retries
                    pipeline_recovered = True
                    recovery_notes = (
                        f"Step recovered on attempt {attempt}/{step.retry_policy.max_retries + 1} "
                        f"after transient {last_error.__class__.__name__}."
                    )
                    self.metadata_collector.record_step_recovery(
                        step_name=step.name,
                        attempts=attempt,
                        recovery_notes=recovery_notes,
                        outputs=step_output,
                        duration_ms=step_duration_ms,
                    )
                    step_statuses[step.name] = StepStatus.RECOVERED
                else:
                    self.metadata_collector.record_step_success(
                        step_name=step.name,
                        attempts=attempt,
                        outputs=step_output,
                        duration_ms=step_duration_ms,
                    )
                    step_statuses[step.name] = StepStatus.SUCCESS

            else:
                # All retries failed - check for fallback recovery handler
                if step.fallback_handler and last_error:
                    try:
                        self.metadata_collector.log_event(
                            "WARN",
                            step.name,
                            f"Retries exhausted. Invoking fallback recovery handler for '{step.name}'.",
                        )
                        fallback_output = step.fallback_handler(context, last_error) or {}
                        context.set_output(step.name, fallback_output)
                        for k, v in fallback_output.items():
                            context.set(k, v)

                        pipeline_recovered = True
                        recovery_notes = (
                            f"Step recovered via fallback handler after {attempt} failed attempts. "
                            f"Terminal error was {last_error.__class__.__name__}: {str(last_error)}."
                        )
                        self.metadata_collector.record_step_recovery(
                            step_name=step.name,
                            attempts=attempt,
                            recovery_notes=recovery_notes,
                            outputs=fallback_output,
                            duration_ms=step_duration_ms,
                        )
                        step_statuses[step.name] = StepStatus.RECOVERED
                    except Exception as fallback_err:
                        self.metadata_collector.record_step_failure(
                            step.name, attempt, fallback_err, step_duration_ms
                        )
                        step_statuses[step.name] = StepStatus.FAILED
                        total_duration_ms = (time.monotonic() - start_time_mono) * 1000
                        metadata = self.metadata_collector.finalize_pipeline(
                            PipelineStatus.FAILED, total_duration_ms
                        )
                        raise PipelineExecutionError(
                            f"Step '{step.name}' failed and fallback handler failed: {fallback_err}"
                        ) from fallback_err
                else:
                    # Permanent failure
                    self.metadata_collector.record_step_failure(
                        step.name, attempt, last_error or Exception("Unknown error"), step_duration_ms
                    )
                    step_statuses[step.name] = StepStatus.FAILED
                    total_duration_ms = (time.monotonic() - start_time_mono) * 1000
                    metadata = self.metadata_collector.finalize_pipeline(
                        PipelineStatus.FAILED, total_duration_ms
                    )
                    raise PipelineExecutionError(
                        f"Step '{step.name}' failed permanently after {attempt} attempts: {last_error}"
                    ) from last_error

        total_duration_ms = (time.monotonic() - start_time_mono) * 1000
        overall_status = PipelineStatus.DEGRADED_SUCCESS if pipeline_recovered else PipelineStatus.SUCCESS
        metadata = self.metadata_collector.finalize_pipeline(overall_status, total_duration_ms)
        return context, metadata
