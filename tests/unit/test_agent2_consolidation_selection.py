"""Unit tests for Agent2 consolidation candidate guardrails."""

import json
from types import SimpleNamespace

from agents import agent2_consolidation


def _row(id_: int, parameter: str, data: str, source: str) -> dict:
    return {
        "ID": id_,
        "Category": "Contact Info",
        "A/C": "Atomic",
        "Parameter": parameter,
        "Research Output / Data": data,
        "Source": source,
    }


class _FakeConsolidationLLM:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def invoke(self, _prompt: str):
        return SimpleNamespace(content=json.dumps(self._rows))


def test_consolidation_prefers_non_missing_candidate_over_llm_missing_choice(monkeypatch):
    # LLM claims Not Found rows from 3.3 are best.
    llm_choice = [
        _row(51, "Primary Contact Name", "Not Found", "LLaMA-3.3-70b"),
        _row(52, "Primary Contact Title", "Not Found", "LLaMA-3.3-70b"),
        _row(53, "Primary Contact Email", "Not Found", "LLaMA-3.3-70b"),
    ]

    # Candidate pool includes better values from 3.1-8b.
    combined_raw = [
        _row(51, "Primary Contact Name", "Not Found", "LLaMA-3.3-70b"),
        _row(51, "Primary Contact Name", "Not Found", "LLaMA-3.1-70b"),
        _row(51, "Primary Contact Name", "Jenny Li", "LLaMA-3.1-8b"),
        _row(52, "Primary Contact Title", "Not Found", "LLaMA-3.3-70b"),
        _row(52, "Primary Contact Title", "Not Found", "LLaMA-3.1-70b"),
        _row(52, "Primary Contact Title", "Data Director of Marketing", "LLaMA-3.1-8b"),
        _row(53, "Primary Contact Email", "Not Found", "LLaMA-3.3-70b"),
        _row(53, "Primary Contact Email", "Not Found", "LLaMA-3.1-70b"),
        _row(53, "Primary Contact Email", "jenny.li@lenovo.com", "LLaMA-3.1-8b"),
    ]

    monkeypatch.setattr(
        agent2_consolidation,
        "get_llm_consolidation",
        lambda: _FakeConsolidationLLM(llm_choice),
    )
    monkeypatch.setattr(agent2_consolidation.time, "sleep", lambda *_args, **_kwargs: None)

    out = agent2_consolidation.run_consolidation(
        {
            "company_name": "Lenovo",
            "combined_raw": combined_raw,
            "retry_consolidation": 0,
        }
    )
    golden = out["golden_record"]
    sources = out["golden_record_sources"]

    assert golden["primary_contact_name"] == "Jenny Li"
    assert sources["primary_contact_name"] == "LLaMA-3.1-8b"
    assert golden["primary_contact_title"] == "Data Director of Marketing"
    assert sources["primary_contact_title"] == "LLaMA-3.1-8b"
    assert golden["primary_contact_email"] == "jenny.li@lenovo.com"
    assert sources["primary_contact_email"] == "LLaMA-3.1-8b"


def test_consolidation_prefers_not_found_over_placeholder_contact_identity(monkeypatch):
    llm_choice = [_row(53, "Primary Contact Email", "john.smith@lenovo.com", "LLaMA-3.1-8b")]
    combined_raw = [
        _row(53, "Primary Contact Email", "Not Found", "LLaMA-3.3-70b"),
        _row(53, "Primary Contact Email", "Not Found", "LLaMA-3.1-70b"),
        _row(53, "Primary Contact Email", "john.smith@lenovo.com", "LLaMA-3.1-8b"),
    ]

    monkeypatch.setattr(
        agent2_consolidation,
        "get_llm_consolidation",
        lambda: _FakeConsolidationLLM(llm_choice),
    )
    monkeypatch.setattr(agent2_consolidation.time, "sleep", lambda *_args, **_kwargs: None)

    out = agent2_consolidation.run_consolidation(
        {
            "company_name": "Lenovo",
            "combined_raw": combined_raw,
            "retry_consolidation": 0,
        }
    )
    assert out["golden_record"]["primary_contact_email"] == "Not Found"
