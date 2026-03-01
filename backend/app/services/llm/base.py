from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod


class BaseLLMProvider(ABC):
    @abstractmethod
    def generate_synthetic_data(
        self,
        schema: list[dict],
        column_notes: dict,
        row_count: int,
    ) -> list[dict]:
        ...

    @abstractmethod
    def is_available(self) -> bool:
        ...

    @abstractmethod
    def get_provider_info(self) -> dict:
        """Must return: name, model, type, requires_api_key, status"""
        ...


class PromptBuilderMixin:
    def build_data_prompt(
        self,
        schema: list[dict],
        column_notes: dict,
        row_count: int,
    ) -> str:
        lines = [
            f"Generate exactly {row_count} rows of JSON test data for a database table",
            "Return ONLY a valid JSON array",
            "No explanation, no markdown, no code blocks, no text before [",
            "",
            "Columns:",
        ]
        for col in schema:
            name = col["name"]
            dtype = col.get("data_type", "string")
            nullable = col.get("nullable", True)
            nullability = "nullable" if nullable else "required"
            note = column_notes.get(name) or "generate realistic data"
            lines.append(f"  {name} ({dtype}, {nullability}): {note}")

        lines.append("")
        lines.append("Include realistic edge cases based on the instructions")
        lines.append("Return only the JSON array starting with [ and ending with ]")
        return "\n".join(lines)

    def parse_json_response(self, raw: str) -> list[dict]:
        # Try 1: direct parse
        try:
            result = json.loads(raw.strip())
            if isinstance(result, list) and all(isinstance(r, dict) for r in result):
                return result
        except (json.JSONDecodeError, ValueError):
            pass

        # Try 2: regex find first [...] block
        match = re.search(r"\[.*?\]", raw, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group(0))
                if isinstance(result, list) and all(isinstance(r, dict) for r in result):
                    return result
            except (json.JSONDecodeError, ValueError):
                pass

        # Try 3: find content between first [ and last ]
        first = raw.find("[")
        last = raw.rfind("]")
        if first != -1 and last != -1 and last > first:
            try:
                result = json.loads(raw[first : last + 1])
                if isinstance(result, list) and all(isinstance(r, dict) for r in result):
                    return result
            except (json.JSONDecodeError, ValueError):
                pass

        raise ValueError(f"LLM returned invalid JSON: {raw[:200]}")
