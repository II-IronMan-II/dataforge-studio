from __future__ import annotations

import pytest

from app.services.llm.base import PromptBuilderMixin
from app.services.llm.factory import get_llm_provider
from app.services.llm.providers.ollama import OllamaProvider
from app.services.llm.providers.openai_compat import OpenAICompatProvider
from app.services.llm.providers.phi3 import Phi3Provider


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Builder(PromptBuilderMixin):
    """Concrete class used to test mixin methods directly."""


# ---------------------------------------------------------------------------
# build_data_prompt
# ---------------------------------------------------------------------------

def test_build_prompt_contains_columns():
    builder = _Builder()
    schema = [{"name": "phone", "data_type": "string", "nullable": True}]
    notes = {"phone": "include nulls and invalid formats"}
    prompt = builder.build_data_prompt(schema, notes, 50)
    assert "phone" in prompt
    assert "include nulls" in prompt
    assert "50" in prompt


# ---------------------------------------------------------------------------
# parse_json_response
# ---------------------------------------------------------------------------

def test_parse_clean_json():
    builder = _Builder()
    result = builder.parse_json_response('[{"id": 1, "name": "Alice"}]')
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_parse_json_with_markdown():
    builder = _Builder()
    result = builder.parse_json_response('```json\n[{"id":1}]\n```')
    assert isinstance(result, list)
    assert len(result) == 1


def test_parse_json_embedded_in_text():
    builder = _Builder()
    result = builder.parse_json_response('Here is data: [{"id": 1}] done')
    assert isinstance(result, list)
    assert len(result) == 1


def test_parse_invalid_raises():
    builder = _Builder()
    with pytest.raises(ValueError) as exc_info:
        builder.parse_json_response("This is not JSON at all !@#")
    assert "LLM returned invalid JSON" in str(exc_info.value)


# ---------------------------------------------------------------------------
# factory
# ---------------------------------------------------------------------------

def test_factory_returns_phi3(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "phi3")
    provider = get_llm_provider()
    assert isinstance(provider, Phi3Provider)


def test_factory_returns_ollama(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    provider = get_llm_provider()
    assert isinstance(provider, OllamaProvider)


def test_factory_returns_openai(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    provider = get_llm_provider()
    assert isinstance(provider, OpenAICompatProvider)


def test_factory_unknown_raises(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "unknown_xyz")
    with pytest.raises(ValueError) as exc_info:
        get_llm_provider()
    assert "unknown_xyz" in str(exc_info.value)
