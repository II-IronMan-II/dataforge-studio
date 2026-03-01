from __future__ import annotations

import os

from app.services.llm.base import BaseLLMProvider


def get_llm_provider() -> BaseLLMProvider:
    provider = os.getenv("LLM_PROVIDER", "phi3")

    if provider == "phi3":
        from app.services.llm.providers.phi3 import Phi3Provider
        return Phi3Provider()

    if provider == "ollama":
        from app.services.llm.providers.ollama import OllamaProvider
        return OllamaProvider(
            model=os.getenv("OLLAMA_MODEL", "mistral"),
            host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
        )

    if provider == "openai":
        from app.services.llm.providers.openai_compat import OpenAICompatProvider
        return OpenAICompatProvider(
            base_url=os.getenv("OPENAI_BASE_URL", ""),
            api_key=os.getenv("OPENAI_API_KEY", ""),
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        )

    raise ValueError(
        f"Unknown LLM_PROVIDER: {provider}. Valid options: phi3, ollama, openai"
    )
