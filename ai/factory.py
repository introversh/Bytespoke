"""
Factory for obtaining the active RCA AI enhancer.
"""

from __future__ import annotations
import os
from ai.heuristic_provider import HeuristicRCAEnhancer
from ai.interface import AbstractRCAEnhancer
from ai.llm_provider import LLMRCAEnhancer


def get_rca_enhancer() -> AbstractRCAEnhancer:
    """Returns the configured AI / Heuristic RCA enhancer based on environment."""
    provider = os.environ.get("AI_PROVIDER", "heuristic").lower()
    if provider in ("llm", "openai", "ollama"):
        return LLMRCAEnhancer()
    return HeuristicRCAEnhancer()
