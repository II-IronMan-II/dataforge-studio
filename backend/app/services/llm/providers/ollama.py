from __future__ import annotations

from app.services.llm.base import BaseLLMProvider, PromptBuilderMixin


class OllamaProvider(BaseLLMProvider, PromptBuilderMixin):
    def __init__(
        self,
        model: str = "mistral",
        host: str = "http://localhost:11434",
    ) -> None:
        self.model = model
        self.host = host

    def generate_synthetic_data(
        self,
        schema: list[dict],
        column_notes: dict,
        row_count: int,
    ) -> list[dict]:
        import httpx

        prompt = self.build_data_prompt(schema, column_notes, row_count)
        response = httpx.post(
            f"{self.host}/api/generate",
            json={"model": self.model, "prompt": prompt, "stream": False},
            timeout=120,
        )
        response.raise_for_status()
        raw = response.json()["response"]
        return self.parse_json_response(raw)

    def is_available(self) -> bool:
        try:
            import httpx

            resp = httpx.get(f"{self.host}/api/tags", timeout=3)
            return resp.status_code == 200
        except Exception:
            return False

    def get_provider_info(self) -> dict:
        return {
            "name": "Ollama",
            "model": self.model,
            "type": "local",
            "requires_api_key": False,
            "status": "available" if self.is_available() else "not_running",
        }
