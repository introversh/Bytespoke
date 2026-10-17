"""
Metadata collector and operational telemetry aggregator.
"""

from __future__ import annotations
import datetime
import os
import platform
import sys
import traceback
from typing import Any, Dict, Optional

from pipeline.models import (
    OperationalMetadata,
    PipelineEvent,
    PipelineStatus,
    RetryAttempt,
    StepMetadata,
    StepStatus,
)


def get_utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def collect_host_info() -> Dict[str, Any]:
    memory_info: Dict[str, Any] = {"rss_mb": "N/A"}
    try:
        if platform.system() == "Windows":
            import ctypes
            import ctypes.wintypes as wt

            class ProcessMemoryCounters(ctypes.Structure):
                _fields_ = [
                    ("cb", wt.DWORD),
                    ("PageFaultCount", wt.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            pmc = ProcessMemoryCounters()
            pmc.cb = ctypes.sizeof(ProcessMemoryCounters)
            ctypes.windll.psapi.GetProcessMemoryInfo.argtypes = [wt.HANDLE, ctypes.POINTER(ProcessMemoryCounters), wt.DWORD]
            ctypes.windll.psapi.GetProcessMemoryInfo.restype = wt.BOOL
            h_proc = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(h_proc, ctypes.byref(pmc), pmc.cb):
                memory_info = {
                    "rss_mb": round(pmc.WorkingSetSize / (1024 * 1024), 2),
                    "peak_rss_mb": round(pmc.PeakWorkingSetSize / (1024 * 1024), 2),
                    "pagefile_mb": round(pmc.PagefileUsage / (1024 * 1024), 2),
                }
        else:
            import resource
            usage = resource.getrusage(resource.RUSAGE_SELF)
            rss_mb = usage.ru_maxrss / (1024 if platform.system() == "Darwin" else 1024 * 1024)
            memory_info = {"rss_mb": round(rss_mb, 2)}
    except Exception:
        pass

    return {
        "hostname": platform.node(),
        "os": f"{platform.system()} {platform.release()} ({platform.version()})",
        "architecture": platform.machine(),
        "python_version": sys.version.split()[0],
        "process_id": os.getpid(),
        "cpu_count": os.cpu_count() or 1,
        "memory": memory_info,
    }


def _safe_summarize(v: Any) -> Any:
    if isinstance(v, (int, float, bool, type(None))):
        return v
    s = str(v)
    return s[:150] + "..." if len(s) > 150 else s


class MetadataCollector:
    """Manages real-time capture of pipeline and step operational metadata."""

    def __init__(self, pipeline_id: str, pipeline_name: str):
        self.metadata = OperationalMetadata(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            overall_status=PipelineStatus.NOT_STARTED,
            start_time=get_utc_now_iso(),
            host_info=collect_host_info(),
        )

    def log_event(
        self,
        level: str,
        step_name: Optional[str],
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        event = PipelineEvent(
            timestamp=get_utc_now_iso(),
            level=level,
            step_name=step_name,
            message=message,
            details=details or {},
        )
        self.metadata.events.append(event)

    def register_step(self, step_name: str, step_index: int, dependencies: list[str], max_retries: int) -> StepMetadata:
        step_meta = StepMetadata(
            step_name=step_name,
            step_index=step_index,
            dependencies=dependencies,
            max_retries=max_retries,
            status=StepStatus.PENDING,
        )
        self.metadata.steps[step_name] = step_meta
        return step_meta

    def record_step_start(self, step_name: str, inputs: Dict[str, Any]) -> None:
        step = self.metadata.steps[step_name]
        step.status = StepStatus.RUNNING
        step.start_time = get_utc_now_iso()
        step.inputs_summary = {k: _safe_summarize(v) for k, v in inputs.items()}
        self.log_event("INFO", step_name, f"Step '{step_name}' started execution.")

    def record_retry(
        self,
        step_name: str,
        attempt: int,
        error: Exception,
        delay_seconds: float,
    ) -> None:
        step = self.metadata.steps[step_name]
        step.status = StepStatus.RETRYING
        step.attempts = attempt
        formatted_tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        
        retry_record = RetryAttempt(
            attempt_number=attempt,
            timestamp=get_utc_now_iso(),
            error_type=error.__class__.__name__,
            error_message=str(error),
            delay_seconds=delay_seconds,
            stack_trace=formatted_tb,
        )
        step.retry_history.append(retry_record)
        step.error = f"{error.__class__.__name__}: {str(error)}"
        
        self.log_event(
            "RETRY",
            step_name,
            f"Attempt {attempt} failed: {error.__class__.__name__} - {str(error)}. Retrying in {delay_seconds:.2f}s",
            {"attempt": attempt, "error_type": error.__class__.__name__, "delay_seconds": delay_seconds},
        )

    def record_step_recovery(
        self,
        step_name: str,
        attempts: int,
        recovery_notes: str,
        outputs: Dict[str, Any],
        duration_ms: float,
    ) -> None:
        step = self.metadata.steps[step_name]
        step.status = StepStatus.RECOVERED
        step.attempts = attempts
        step.end_time = get_utc_now_iso()
        step.duration_ms = duration_ms
        step.recovery_notes = recovery_notes
        step.outputs_summary = {k: _safe_summarize(v) for k, v in outputs.items()}
        self.metadata.recovery_occurred = True
        self.metadata.recovery_details = {
            "recovered_step": step_name,
            "total_attempts": attempts,
            "mechanism": recovery_notes,
            "timestamp": step.end_time,
        }
        self.log_event(
            "RECOVERY",
            step_name,
            f"Step '{step_name}' successfully RECOVERED after {attempts} attempts. Notes: {recovery_notes}",
            {"attempts": attempts, "recovery_notes": recovery_notes},
        )

    def record_step_success(
        self,
        step_name: str,
        attempts: int,
        outputs: Dict[str, Any],
        duration_ms: float,
    ) -> None:
        step = self.metadata.steps[step_name]
        if step.status != StepStatus.RECOVERED:
            step.status = StepStatus.SUCCESS
        step.attempts = attempts
        step.end_time = get_utc_now_iso()
        step.duration_ms = duration_ms
        step.outputs_summary = {k: _safe_summarize(v) for k, v in outputs.items()}
        self.log_event("INFO", step_name, f"Step '{step_name}' completed successfully in {duration_ms:.2f}ms.")

    def record_step_skipped(self, step_name: str, reason: str) -> None:
        step = self.metadata.steps[step_name]
        step.status = StepStatus.SKIPPED
        step.start_time = get_utc_now_iso()
        step.end_time = step.start_time
        step.duration_ms = 0.0
        step.skip_reason = reason
        self.log_event("INFO", step_name, f"Step '{step_name}' SKIPPED. Reason: {reason}", {"skip_reason": reason})

    def record_step_failure(
        self,
        step_name: str,
        attempts: int,
        error: Exception,
        duration_ms: float,
    ) -> None:
        step = self.metadata.steps[step_name]
        step.status = StepStatus.FAILED
        step.attempts = attempts
        step.end_time = get_utc_now_iso()
        step.duration_ms = duration_ms
        step.error = f"{error.__class__.__name__}: {str(error)}"
        self.log_event(
            "ERROR",
            step_name,
            f"Step '{step_name}' FAILED permanently after {attempts} attempts: {step.error}",
            {"error": step.error},
        )

    def finalize_pipeline(self, status: PipelineStatus, total_duration_ms: float) -> OperationalMetadata:
        self.metadata.overall_status = status
        self.metadata.end_time = get_utc_now_iso()
        self.metadata.total_duration_ms = total_duration_ms
        self.log_event(
            "INFO" if status in (PipelineStatus.SUCCESS, PipelineStatus.DEGRADED_SUCCESS) else "ERROR",
            None,
            f"Pipeline finished with status: {status.value} (Total Duration: {total_duration_ms:.2f}ms)",
        )
        return self.metadata
