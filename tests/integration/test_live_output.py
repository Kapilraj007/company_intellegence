"""
tests/integration/test_live_output.py
======================================
Live integration tests: run against the golden record file that Agent 3
just saved in the current pipeline run.

These tests are injected with the live file path via the
LIVE_GOLDEN_RECORD_PATH environment variable, set by agent4_test_runner.py.
They skip gracefully if run standalone without a live path.
"""
import sys, os, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[2]))

import pytest
from core.models import validate_flat_golden_record
from core.prompts import _FLAT_KEY_TO_ID

LIVE_PATH = os.environ.get("LIVE_GOLDEN_RECORD_PATH", "")


def load_live_record():
    if not LIVE_PATH or not Path(LIVE_PATH).exists():
        return None
    with open(LIVE_PATH, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def live_record():
    data = load_live_record()
    if data is None:
        pytest.skip("LIVE_GOLDEN_RECORD_PATH not set — run via pipeline")
    return data


@pytest.fixture(scope="module")
def live_by_id(live_record):
    return {field_id: live_record.get(key, "Not Found") for key, field_id in _FLAT_KEY_TO_ID.items()}


@pytest.fixture(scope="module")
def live_report(live_record):
    company = os.environ.get("LIVE_COMPANY_NAME", "Pipeline Output")
    _, report = validate_flat_golden_record(live_record, company)
    return report


# ── Live validation ───────────────────────────────────────────────────────────

class TestLivePipelineOutput:

    def test_163_rows_present(self, live_record):
        assert isinstance(live_record, dict), "Live output must be a JSON object"
        assert len(live_record) == 163, \
            f"Expected 163 keys, got {len(live_record)}"

    def test_no_duplicate_ids(self, live_record):
        keys = set(live_record.keys())
        expected = set(_FLAT_KEY_TO_ID.keys())
        assert keys == expected, "Live output keys do not match schema keys"

    def test_all_ids_in_range(self, live_record):
        for key in live_record:
            id_ = int(_FLAT_KEY_TO_ID[key])
            assert 1 <= id_ <= 163, f"ID out of range: {id_}"

    def test_completeness_above_threshold(self, live_report):
        assert live_report.completeness_pct >= 80.0, \
            f"Completeness {live_report.completeness_pct}% below 80% threshold"

    def test_zero_failed_rows(self, live_report):
        failed = [r for r in live_report.results if r.status == "❌ FAIL"]
        assert not failed, \
            f"{len(failed)} rows FAILED validation:\n" + \
            "\n".join(f"  ID={r.ID} {r.Parameter}: {r.issue}" for r in failed)

    def test_report_counts_sum_to_163(self, live_report):
        total = live_report.total_passed + live_report.total_missing + live_report.total_failed
        assert total == 163

    # ── Risk fields (Section 12.5) ────────────────────────────────────────
    @pytest.mark.parametrize("field_id,label", [
        (77, "Customer Concentration Risk"),
        (78, "Burn Rate"),
        (87, "Geopolitical Risks"),
        (88, "Macro Risks"),
    ])
    def test_risk_field_passes(self, live_report, field_id, label):
        result = next((r for r in live_report.results if r.ID == field_id), None)
        assert result is not None, f"ID={field_id} ({label}) not in report"
        assert result.status in ("✅ PASS", "⚠️  MISSING"), \
            f"ID={field_id} ({label}) FAILED: {result.issue}"

    # ── Null handling (Section 14.1) ──────────────────────────────────────
    def test_no_raw_sentinel_values_in_output(self, live_record):
        """Pydantic must have normalised all sentinels — no raw empty strings."""
        raw_sentinels = {"n/a", "unknown", "none", "null", "-"}
        for key, value in live_record.items():
            data = str(value).strip().lower()
            assert data not in raw_sentinels, \
                f"Raw sentinel '{data}' survived in key={key}"

    # ── Structure ─────────────────────────────────────────────────────────
    def test_all_rows_have_required_keys(self, live_record):
        required = set(_FLAT_KEY_TO_ID.keys())
        missing = required - set(live_record.keys())
        assert not missing, f"Missing keys: {sorted(missing)}"

    def test_all_ac_values_valid(self, live_record):
        for key, value in live_record.items():
            assert isinstance(value, str), f"Value for {key} must be a string"
