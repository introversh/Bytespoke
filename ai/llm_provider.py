"""
LLM-based RCA generator supporting local models (Ollama, vLLM, llama.cpp)
and remote OpenAI-compatible APIs, with automatic fallback to heuristics.
"""

from __future__ import annotations
import json
import logging
import os
import urllib.request
import urllib.error
from typing import Any, Dict, Optional

from ai.heuristic_provider import HeuristicRCAEnhancer
from ai.interface import AbstractRCAEnhancer, RCAInsights
from pipeline.models import OperationalMetadata

logger = logging.getLogger(__name__)


class LLMRCAEnhancer(AbstractRCAEnhancer):
    """
    Invokes an LLM (Ollama or OpenAI-compatible endpoint) behind a clear interface.
    Gracefully falls back to HeuristicRCAEnhancer if the LLM is unreachable or unconfigured.
    """

    def __init__(
        self,
        api_base: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self.api_base = api_base or os.environ.get("AI_API_BASE", "http://localhost:11434/v1")
        self.api_key = api_key or os.environ.get("AI_API_KEY", "")
        self.model = model or os.environ.get("AI_MODEL", "llama3.2")
        self.fallback = HeuristicRCAEnhancer()

    def analyze_incident(self, metadata: OperationalMetadata) -> RCAInsights:
        """Attempts LLM synthesis; on failure/timeout gracefully falls back to Heuristic provider."""
        if not self.api_base:
            return self.fallback.analyze_incident(metadata)

        prompt = (
            f"You are an SRE Incident Commander. Analyze the following operational metadata from a data pipeline "
            f"and produce a JSON Root Cause Analysis with keys: executive_summary, technical_root_cause, five_whys (list of 5 strings), "
            f"and capa_actions (list of dicts with priority, action, owner, target_date).\n\n"
            f"METADATA:\n{json.dumps(metadata.to_dict(), indent=2)}"
        )

        try:
            url = f"{self.api_base.rstrip('/')}/chat/completions"
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "You are a senior SRE specialist. Respond strictly in valid JSON."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
            }

            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers=headers,
                method="POST",
            )

            with urllib.request.urlopen(req, timeout=3.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                content = data["choices"][0]["message"]["content"]
                parsed = json.loads(content)

                return RCAInsights(
                    incident_reference=f"INC-{metadata.pipeline_id[:8].upper()}",
                    severity="SEV-2 (High - Degraded Auto-Recovery)",
                    status="RESOLVED (Self-Healed via Retries)",
                    executive_summary=parsed.get("executive_summary", ""),
                    technical_root_cause=parsed.get("technical_root_cause", ""),
                    five_whys=parsed.get("five_whys", []),
                    capa_actions=parsed.get("capa_actions", []),
                    provider_name=f"LLMClient ({self.model})",
                    model_version=self.model,
                    telemetry_summary={"api_base": self.api_base},
                )

        except Exception as e:
            logger.info("LLM provider unavailable or timed out (%s). Using fallback heuristic.", e)
            res = self.fallback.analyze_incident(metadata)
            res.provider_name = f"HeuristicFallback (LLM Unavailable: {e.__class__.__name__})"
            return res
