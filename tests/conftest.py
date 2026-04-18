"""
tests/conftest.py — Global fixtures
"""
import copy
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

# ── CRITICAL: seed sys.path so `from core.models import ...` works ─────────────
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TESTS_DIR = Path(__file__).parent
DATA_DIR  = TESTS_DIR / "data" / "golden_records"

EMPTY_SENTINEL_VALUES = {"not found", "n/a", "unknown", "none", "null", "", "-"}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_fixture(filename: str) -> Optional[List[Dict[str, Any]]]:
    path = DATA_DIR / filename
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


# ── Session-scoped golden record fixtures ─────────────────────────────────────

@pytest.fixture(scope="session")
def tcs_golden_record() -> List[Dict[str, Any]]:
    data = _load_fixture("tcs_golden_record.json")
    if data is None:
        pytest.skip("TCS fixture not found in tests/data/golden_records/")
    return data


@pytest.fixture(scope="session")
def google_golden_record() -> List[Dict[str, Any]]:
    data = _load_fixture("google_golden_record.json")
    if data is None:
        pytest.skip("Google fixture not found")
    return data


@pytest.fixture(scope="session")
def infosys_golden_record() -> List[Dict[str, Any]]:
    data = _load_fixture("infosys_golden_record.json")
    if data is None:
        pytest.skip("Infosys fixture not found")
    return data


@pytest.fixture(scope="session")
def wipro_golden_record() -> List[Dict[str, Any]]:
    data = _load_fixture("wipro_golden_record.json")
    if data is None:
        pytest.skip("Wipro fixture not found")
    return data


@pytest.fixture(scope="session")
def all_golden_records(
    tcs_golden_record, google_golden_record,
    infosys_golden_record, wipro_golden_record,
) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "TCS":      tcs_golden_record,
        "Google":   google_golden_record,
        "Infosys":  infosys_golden_record,
        "Wipro":    wipro_golden_record,
    }


# ── Mutable deep-copy fixtures ─────────────────────────────────────────────────

@pytest.fixture
def tcs_record_copy(tcs_golden_record):
    return copy.deepcopy(tcs_golden_record)


@pytest.fixture
def google_record_copy(google_golden_record):
    return copy.deepcopy(google_golden_record)


# ── O(1) ID-lookup fixtures ────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def tcs_by_id(tcs_golden_record) -> Dict[int, Dict[str, Any]]:
    return {int(r["ID"]): r for r in tcs_golden_record}


@pytest.fixture(scope="session")
def google_by_id(google_golden_record) -> Dict[int, Dict[str, Any]]:
    return {int(r["ID"]): r for r in google_golden_record}


# ── Mock graph fixtures ────────────────────────────────────────────────────────

@pytest.fixture
def mock_graph(tcs_golden_record):
    mock = MagicMock()
    mock.invoke.return_value = {
        "company_name":  "TCS",
        "golden_record": tcs_golden_record,
    }
    with patch("graph.graph", mock):
        yield mock


@pytest.fixture
def mock_graph_factory(all_golden_records):
    def _factory(company_name: str) -> MagicMock:
        mock = MagicMock()
        mock.invoke.return_value = {
            "company_name":  company_name,
            "golden_record": copy.deepcopy(all_golden_records[company_name]),
        }
        return mock
    return _factory