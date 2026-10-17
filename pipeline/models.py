"""
Operational models and data structures for the pipeline engine.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
import datetime


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    RETRYING = "RETRYING"
    RECOVERED = "RECOVERED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class PipelineStatus(str, Enum):
    NOT_STARTED = "NOT_STARTED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    DEGRADED_SUCCESS = "DEGRADED_SUCCESS"
    FAILED = "FAILED"


@dataclass
class RetryAttempt:
    attempt_number: int
    timestamp: str
    error_type: str
    error_message: str
    delay_seconds: float
    stack_trace: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "attempt_number": self.attempt_number,
            "timestamp": self.timestamp,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "delay_seconds": round(self.delay_seconds, 3),
            "stack_trace": self.stack_trace,
        }


@dataclass
class StepMetadata:
    step_name: str
    step_index: int
    dependencies: List[str]
    status: StepStatus = StepStatus.PENDING
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_ms: float = 0.0
    attempts: int = 0
    max_retries: int = 0
    retry_history: List[RetryAttempt] = field(default_factory=list)
    inputs_summary: Dict[str, Any] = field(default_factory=dict)
    outputs_summary: Dict[str, Any] = field(default_factory=dict)
    recovery_notes: Optional[str] = None
    skip_reason: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_name": self.step_name,
            "step_index": self.step_index,
            "dependencies": self.dependencies,
            "status": self.status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": round(self.duration_ms, 2),
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "retry_history": [r.to_dict() for r in self.retry_history],
            "inputs_summary": self.inputs_summary,
            "outputs_summary": self.outputs_summary,
            "recovery_notes": self.recovery_notes,
            "skip_reason": self.skip_reason,
            "error": self.error,
        }


@dataclass
class PipelineEvent:
    timestamp: str
    level: str  # INFO, WARN, ERROR, RETRY, RECOVERY
    step_name: Optional[str]
    message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "level": self.level,
            "step_name": self.step_name,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class OperationalMetadata:
    pipeline_id: str
    pipeline_name: str
    overall_status: PipelineStatus
    start_time: str
    end_time: Optional[str] = None
    total_duration_ms: float = 0.0
    host_info: Dict[str, Any] = field(default_factory=dict)
    steps: Dict[str, StepMetadata] = field(default_factory=dict)
    events: List[PipelineEvent] = field(default_factory=list)
    recovery_occurred: bool = False
    recovery_details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "overall_status": self.overall_status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "total_duration_ms": round(self.total_duration_ms, 2),
            "host_info": self.host_info,
            "recovery_occurred": self.recovery_occurred,
            "recovery_details": self.recovery_details,
            "steps": {name: step.to_dict() for name, step in self.steps.items()},
            "events": [event.to_dict() for event in self.events],
        }
