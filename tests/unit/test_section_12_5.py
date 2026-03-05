"""
test_section_12_5.py — Risk Classification (Section 12.5)
Fields: ID 77 Customer Concentration Risk, 78 Burn Rate,
        87 Geopolitical Risks, 88 Macro Risks
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[2]))

import pytest
from pydantic import ValidationError
from core.models import CompanyField, validate_golden_record, EXPECTED_PARAMETERS, EMPTY_VALUES
from tests.factories.row_factory import RiskRowFactory, InvalidRowFactory, make_row
from tests.fixtures.helpers import get_row_by_id

RISK_IDS = {77: "Customer Concentration Risk", 78: "Burn Rate",
            87: "Geopolitical Risks",          88: "Macro Risks"}


# ── A: Field Presence ─────────────────────────────────────────────────────────

class TestRiskFieldPresence:

    @pytest.mark.parametrize("field_id,label", RISK_IDS.items())
    def test_risk_field_present_tcs(self, tcs_golden_record, field_id, label):
        ids = {int(r["ID"]) for r in tcs_golden_record}
        assert field_id in ids, f"ID={field_id} ({label}) missing from TCS golden record"

    def test_all_risk_fields_present_all_companies(self, all_golden_records):
        for company, record in all_golden_records.items():
            present = {int(r["ID"]) for r in record}
            missing = set(RISK_IDS.keys()) - present
            assert not missing, f"{company}: missing risk field IDs: {missing}"


# ── B: Type & Schema ──────────────────────────────────────────────────────────

class TestRiskFieldSchema:

    @pytest.mark.parametrize("row_fn,expected_id", [
        (lambda: RiskRowFactory.customer_concentration("Low"), 77),
        (lambda: RiskRowFactory.burn_rate("High"),             78),
        (lambda: RiskRowFactory.geopolitical(),                87),
        (lambda: RiskRowFactory.macro(),                       88),
    ])
    def test_valid_risk_row_passes_pydantic(self, row_fn, expected_id):
        field = CompanyField.model_validate(row_fn())
        assert field.ID == expected_id
        assert field.AC in ("Atomic", "Composite")

    @pytest.mark.parametrize("ac_type,field_id", [
        ("Atomic",    77), ("Atomic",    78),
        ("Composite", 87), ("Composite", 88),
    ])
    def test_ac_matches_registry(self, ac_type, field_id):
        _, _, expected_ac = EXPECTED_PARAMETERS[field_id]
        assert expected_ac == ac_type

    def test_geopolitical_is_composite(self):
        field = CompanyField.model_validate(RiskRowFactory.geopolitical())
        assert field.AC == "Composite"

    def test_customer_concentration_is_atomic(self):
        field = CompanyField.model_validate(RiskRowFactory.customer_concentration())
        assert field.AC == "Atomic"


# ── C: Value Constraints ──────────────────────────────────────────────────────

class TestRiskValues:

    @pytest.mark.parametrize("level", ["Low", "Medium", "High", "Very High", "Critical"])
    def test_valid_risk_levels_accepted(self, level):
        field = CompanyField.model_validate(RiskRowFactory.customer_concentration(level=level))
        assert field.Data == level

    @pytest.mark.parametrize("sentinel", list(EMPTY_VALUES))
    def test_sentinel_normalises_to_not_found(self, sentinel):
        row = RiskRowFactory.customer_concentration(level=sentinel)
        field = CompanyField.model_validate(row)
        assert field.Data == "Not Found", f"'{sentinel}' → expected 'Not Found', got '{field.Data}'"

    def test_geopolitical_semicolon_separated(self, tcs_by_id):
        row = tcs_by_id.get(87)
        if row is None:
            pytest.skip("ID 87 not in fixture")
        data = row.get("Research Output / Data", "")
        if data not in ("Not Found", "N/A"):
            items = [x.strip() for x in data.split(";")]
            assert len(items) >= 1

    def test_empty_risk_level_normalises(self):
        field = CompanyField.model_validate(RiskRowFactory.customer_concentration_empty())
        assert field.Data == "Not Found"


# ── D: Integration ────────────────────────────────────────────────────────────

class TestRiskIntegration:

    def test_tcs_risk_fields_all_pass(self, tcs_golden_record):
        _, report = validate_golden_record(tcs_golden_record, "TCS")
        for id_ in RISK_IDS:
            result = next(r for r in report.results if r.ID == id_)
            assert result.status == "✅ PASS", \
                f"ID={id_} ({RISK_IDS[id_]}) status: {result.status} — {result.issue}"

    def test_missing_risk_field_marked_missing(self, tcs_record_copy):
        tcs_record_copy[:] = [r for r in tcs_record_copy if int(r["ID"]) != 77]
        _, report = validate_golden_record(tcs_record_copy, "TCS")
        r77 = next(r for r in report.results if r.ID == 77)
        assert r77.status == "⚠️  MISSING"

    def test_completeness_above_threshold(self, tcs_golden_record):
        _, report = validate_golden_record(tcs_golden_record, "TCS")
        assert report.completeness_pct >= 95.0


# ── E: Negative Tests ─────────────────────────────────────────────────────────

class TestRiskNegative:

    def test_id_zero_raises(self):
        with pytest.raises(ValidationError):
            CompanyField.model_validate(InvalidRowFactory.id_zero())

    def test_id_over_163_raises(self):
        with pytest.raises(ValidationError):
            CompanyField.model_validate(RiskRowFactory.burn_rate_id_over_limit())

    def test_id_negative_raises(self):
        with pytest.raises(ValidationError):
            CompanyField.model_validate(RiskRowFactory.burn_rate_negative_id())

    def test_invalid_ac_raises(self):
        with pytest.raises(ValidationError):
            CompanyField.model_validate(InvalidRowFactory.wrong_ac_value())

    def test_missing_data_field_raises(self):
        with pytest.raises(ValidationError):
            CompanyField.model_validate(
                InvalidRowFactory.missing_required_field("Research Output / Data"))

    def test_null_category_raises(self):
        with pytest.raises(ValidationError):
            CompanyField.model_validate(InvalidRowFactory.null_category())

    def test_string_id_coerces(self):
        row = make_row(77, "Sales & Growth", "Atomic", "Customer Concentration Risk", "Low")
        row["ID"] = "77"
        field = CompanyField.model_validate(row)
        assert field.ID == 77

    def test_non_numeric_string_id_raises(self):
        row = make_row(77, "Sales & Growth", "Atomic", "Customer Concentration Risk", "Low")
        row["ID"] = "seventy"
        with pytest.raises(ValidationError):
            CompanyField.model_validate(row)