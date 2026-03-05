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
    by_id = {int(row["ID"]): row for row in out["golden_record"]}

    assert by_id[51]["Research Output / Data"] == "Jenny Li"
    assert by_id[51]["Source"] == "LLaMA-3.1-8b"
    assert by_id[52]["Research Output / Data"] == "Data Director of Marketing"
    assert by_id[52]["Source"] == "LLaMA-3.1-8b"
    assert by_id[53]["Research Output / Data"] == "jenny.li@lenovo.com"
    assert by_id[53]["Source"] == "LLaMA-3.1-8b"


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
    by_id = {int(row["ID"]): row for row in out["golden_record"]}

    assert by_id[53]["Research Output / Data"] == "Not Found"
