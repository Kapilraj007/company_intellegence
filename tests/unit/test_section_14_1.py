"""
test_section_14_1.py — Unavailable Data (Section 14.1)
Every EMPTY_VALUES sentinel → "Not Found". All 163 fields tested.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[2]))

import pytest
from pydantic import ValidationError
from core.models import CompanyField, validate_golden_record, EXPECTED_PARAMETERS, EMPTY_VALUES
from tests.factories.row_factory import NullRowFactory, ScaleRowFactory, make_row

SENTINELS = list(EMPTY_VALUES)


def _all_field_params():
    return [
        pytest.param(id_, cat, param, ac, id=f"id_{id_}")
        for id_, (cat, param, ac) in EXPECTED_PARAMETERS.items()
    ]


# ── A: Sentinel Normalisation ─────────────────────────────────────────────────

class TestSentinelNormalisation:

    @pytest.mark.parametrize("sentinel", SENTINELS)
    def test_sentinel_normalises_to_not_found(self, sentinel):
        row   = make_row(60, "Financials", "Atomic", "Annual Revenues", sentinel)
        field = CompanyField.model_validate(row)
        assert field.Data == "Not Found", \
            f"Sentinel '{sentinel}' → expected 'Not Found', got '{field.Data}'"

    @pytest.mark.parametrize("sentinel", SENTINELS)
    def test_case_insensitive_sentinel(self, sentinel):
        for variant in [sentinel, sentinel.upper(), sentinel.capitalize()]:
            row   = make_row(60, "Financials", "Atomic", "Annual Revenues", variant)
            field = CompanyField.model_validate(row)
            assert field.Data == "Not Found", f"Case variant '{variant}' not normalised"

    def test_whitespace_only_normalises(self):
        field = CompanyField.model_validate(
            make_row(60, "Financials", "Atomic", "Annual Revenues", "   "))
        assert field.Data == "Not Found"

    def test_real_value_preserved(self):
        field = CompanyField.model_validate(
            make_row(60, "Financials", "Atomic", "Annual Revenues", "$25.7B"))
        assert field.Data == "$25.7B"

    def test_not_found_passthrough(self):
        field = CompanyField.model_validate(
            make_row(60, "Financials", "Atomic", "Annual Revenues", "Not Found"))
        assert field.Data == "Not Found"


# ── B: Private Company Financials ─────────────────────────────────────────────

class TestPrivateCompanyFinancials:

    FINANCIAL = [
        (60, "Financials", "Atomic",  "Annual Revenues"),
        (61, "Financials", "Atomic",  "Annual Profits"),
        (63, "Financials", "Atomic",  "Company Valuation"),
        (64, "Financials", "Atomic",  "Year-over-Year Growth Rate"),
        (65, "Financials", "Atomic",  "Profitability Status"),
        (66, "Financials", "Atomic",  "Market Share"),
    ]

    @pytest.mark.parametrize("id_,cat,ac,param", FINANCIAL)
    def test_not_found_passes_pydantic(self, id_, cat, ac, param):
        field = CompanyField.model_validate(make_row(id_, cat, ac, param, "Not Found"))
        assert field.Data == "Not Found"

    @pytest.mark.parametrize("id_,cat,ac,param", FINANCIAL)
    def test_not_found_passes_full_validation(self, id_, cat, ac, param):
        record = ScaleRowFactory.minimal_163_record("PrivateCo")
        for row in record:
            if int(row["ID"]) == id_:
                row["Research Output / Data"] = "Not Found"
                break
        _, report = validate_golden_record(record, "PrivateCo")
        result = next(r for r in report.results if r.ID == id_)
        assert result.status == "✅ PASS", \
            f"ID={id_} Not Found got '{result.status}' — must PASS for unavailable private data"

    def test_factory_private_revenue(self):
        field = CompanyField.model_validate(NullRowFactory.private_company_revenue())
        assert field.Data == "Not Found"

    def test_factory_empty_string_normalises(self):
        field = CompanyField.model_validate(
            NullRowFactory.private_company_revenue_with_empty_string())
        assert field.Data == "Not Found"


# ── C: All 163 Fields Accept "Not Found" ─────────────────────────────────────

@pytest.mark.parametrize("field_id,cat,param,ac", _all_field_params())
def test_not_found_accepted_for_every_field(field_id, cat, param, ac):
    """163 parametrized cases — every field must accept 'Not Found'."""
    field = CompanyField.model_validate(make_row(field_id, cat, ac, param, "Not Found"))
    assert field.Data == "Not Found"


@pytest.mark.parametrize("field_id,cat,param,ac", _all_field_params())
def test_empty_string_normalises_for_every_field(field_id, cat, param, ac):
    """163 parametrized cases — empty string must normalise for every field."""
    field = CompanyField.model_validate(make_row(field_id, cat, ac, param, ""))
    assert field.Data == "Not Found"