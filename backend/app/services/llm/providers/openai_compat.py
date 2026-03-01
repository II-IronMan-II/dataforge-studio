from __future__ import annotations

from app.services.llm.base import BaseLLMProvider, PromptBuilderMixin


class OpenAICompatProvider(BaseLLMProvider, PromptBuilderMixin):
    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str = "gpt-4o-mini",
    ) -> None:
        self.base_url = base_url
        self.api_key = api_key
        self.model = model

    def generate_synthetic_data(
        self,
        schema: list[dict],
        column_notes: dict,
        row_count: int,
    ) -> list[dict]:
        import httpx

        prompt = self.build_data_prompt(schema, column_notes, row_count)
        response = httpx.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7,
                "max_tokens": 4096,
            },
            timeout=60,
        )
        response.raise_for_status()
        raw = response.json()["choices"][0]["message"]["content"]
        return self.parse_json_response(raw)

    def is_available(self) -> bool:
        return bool(self.api_key)

    def get_provider_info(self) -> dict:
        return {
            "name": "OpenAI Compatible",
            "model": self.model,
            "type": "api",
            "requires_api_key": True,
            "status": "configured" if self.api_key else "missing_api_key",
        }
