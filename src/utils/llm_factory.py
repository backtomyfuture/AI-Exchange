import logging
from langchain_openai import ChatOpenAI
from src.config import get_settings

logger = logging.getLogger(__name__)

class LLMFactory:
    @staticmethod
    def create_llm(temperature: float = 0.7, model_name: str = None, json_mode: bool = False) -> ChatOpenAI:
        """
        Create a configured ChatOpenAI instance.
        """
        settings = get_settings()
        model = model_name or settings.LLM_MODEL
        api_key = settings.OPENAI_API_KEY
        base_url = settings.OPENAI_API_BASE
        
        # Optional: Validate credentials
        if not api_key:
            logger.warning("OPENAI_API_KEY is not set.")

        kwargs = {
            "model": model,
            "temperature": temperature,
            "base_url": base_url,
            "api_key": api_key,
            "max_retries": 2,
            "timeout": 60
        }

        if json_mode:
             # Some providers support json_mode via model_kwargs or specific params
             # For standard OpenAI/Gemini adapter, usually purely prompt-based, 
             # but keeping hook here for future specific parameters.
             pass

        return ChatOpenAI(**kwargs)

