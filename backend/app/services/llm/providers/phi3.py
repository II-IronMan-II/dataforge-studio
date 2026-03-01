from __future__ import annotations

from app.services.llm.base import BaseLLMProvider, PromptBuilderMixin


class Phi3Provider(BaseLLMProvider, PromptBuilderMixin):
    MODEL_ID = "microsoft/Phi-3-mini-4k-instruct"

    def __init__(self) -> None:
        self._pipe = None

    def _load(self) -> None:
        if self._pipe is None:
            from transformers import pipeline
            import torch

            self._pipe = pipeline(
                "text-generation",
                model=self.MODEL_ID,
                torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
                device_map="auto",
                trust_remote_code=True,
            )

    def generate_synthetic_data(
        self,
        schema: list[dict],
        column_notes: dict,
        row_count: int,
    ) -> list[dict]:
        self._load()
        prompt = self.build_data_prompt(schema, column_notes, row_count)
        chat_prompt = f"<|user|>{prompt}<|end|>\n<|assistant|>"
        output = self._pipe(chat_prompt, max_new_tokens=4096, temperature=0.7)
        generated_text = output[0]["generated_text"]
        remainder = generated_text[len(chat_prompt):]
        return self.parse_json_response(remainder)

    def is_available(self) -> bool:
        try:
            import transformers  # noqa: F401
            return True
        except ImportError:
            return False

    def get_provider_info(self) -> dict:
        return {
            "name": "Phi-3 Mini",
            "model": self.MODEL_ID,
            "type": "local",
            "requires_api_key": False,
            "status": "available" if self.is_available() else "not_installed",
        }
