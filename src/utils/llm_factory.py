"""
Backward-compatible LLMFactory — delegates to src.providers.factory.

New code should import from src.providers.factory directly.
"""

import logging

from langchain_openai import ChatOpenAI
from src.providers.factory import get_llm

logger = logging.getLogger(__name__)


class LLMFactory:
    @staticmethod
    def create_llm(
        temperature: float = 0.7,
        model_name: str | None = None,
        json_mode: bool = False,
    ) -> ChatOpenAI:
        return get_llm(model=model_name, temperature=temperature)

