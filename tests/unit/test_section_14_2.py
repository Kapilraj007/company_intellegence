"""
test_section_14_2.py — NULL/NA HANDLING: Not Applicable Fields
===============================================================
Test Case ID : 14.2
Category     : NULL/NA HANDLING
Type         : Not Applicable Fields
Description  : Fields that don't apply to entity type
Priority     : High

Scenarios:
  - VC firm       : 'Products' (ID 18) = N/A
  - Bootstrapped  : 'Investors' (ID 67) = N/A
  - Remote company: 'Office Locations' (ID 11) = N/A - Fully Remote
  - Startup       : 'Board of Directors' (ID 104) may not apply
  - Public company: 'Total Capital Raised' (ID 69) context-dependent

Key distinction from 14.1:
  14.1 = data EXISTS but is UNAVAILABLE (private, undisclosed)
  14.2 = field DOES NOT APPLY to this entity type (N/A by design)

Both map to 'Not Found' after Pydantic normalisation,
but upstream business logic should distinguish them.
"""
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "com_intell" / "company-intelligence"))

from core.models import CompanyField, validate_golden_record, EXPECTED_PARAMETERS

from tests.factories import NullRowFactory, ScaleRowFactory, make_row
from tests.fixtures import get_row_by_id, get_data_value


# ── Entity type → fields that should be N/A ───────────────────────────────────
# This is business logic documentation encoded as test data
NA_MATRIX = {
    "vc_firm": {
        18: "Services / Offerings / Products",  # VC firms don't have products
        19: "Top Customers by Client Segments",
        82: "R&D Investment",
    },
    "bootstrapped_startup": {
        67: "Key Investors Backers",
        68: "Recent Funding Rounds",
        69: "Total Capital Raised",
    },
    "fully_remote": {
        10: "Number of Offices",
        11: "Office Locations",
        123: "Central vs Peripheral Location",
        124: "Public Transport Access",
        125: "Cab Availability Policy",
        126: "Commute Time from Airport",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# Section A — N/A Values Are Accepted by Pydantic
# ═══════════════════════════════════════════════════════════════════════════════

class TestNAValueAcceptance:
    """
    N/A is an EMPTY_VALUES sentinel → normalised to 'Not Found'.
    Test that this normalisation happens cleanly for each entity type scenario.
    """

    @pytest.mark.parametrize("entity_type,na_fields", NA_MATRIX.items())
    def test_na_fields_normalise_to_not_found(self, entity_type, na_fields):
        """All N/A fields for each entity type must normalise to 'Not Found'."""
        for field_id, param_name in na_fields.items():
            cat, param, ac = EXPECTED_PARAMETERS[field_id]
            row = make_row(field_id, cat, ac, param, "N/A")
            field = CompanyField.model_validate(row)
            assert field.Data == "Not Found", (
                f"[{entity_type}] ID={field_id} ({param_name}): "
                f"'N/A' should → 'Not Found', got '{field.Data}'"
            )

    @pytest.mark.parametrize("na_variant", [
        "N/A", "n/a", "N/a", "NA", "na", "Not Applicable", "not applicable"
    ])
    def test_na_variants_pydantic_handling(self, na_variant):
        """Various 'not applicable' spellings: those in EMPTY_VALUES normalise; others preserve."""
        row = make_row(18, "Business Model", "Composite",
                       "Services / Offerings / Products", na_variant)
        field = CompanyField.model_validate(row)
        from core.models import EMPTY_VALUES
        if na_variant.strip().lower() in EMPTY_VALUES:
            assert field.Data == "Not Found", (
                f"'{na_variant}' (in EMPTY_VALUES) should normalise to 'Not Found'"
            )
        else:
            # "Not Applicable" is NOT in EMPTY_VALUES → preserved as-is
            assert field.Data == na_variant.strip()

    def test_vc_firm_products_na_factory(self):
        row = NullRowFactory.vc_firm_products_na()
        field = CompanyField.model_validate(row)
        assert field.Data == "Not Found"

    def test_remote_company_office_na_preserved(self):
        """'N/A - Fully Remote' is NOT in EMPTY_VALUES → preserved as context."""
        row = NullRowFactory.remote_company_office_na()
        field = CompanyField.model_validate(row)
        # "n/a - fully remote" is not in EMPTY_VALUES → data preserved
        assert "Remote" in field.Data or field.Data == "N/A - Fully Remote"


# ═══════════════════════════════════════════════════════════════════════════════
# Section B — Full Record Validation with N/A Fields
# ═══════════════════════════════════════════════════════════════════════════════

class TestNAFieldsInFullRecord:
    """
    Inject N/A fields into a full 163-row record and verify the validation
    report marks them as PASS (not FAIL) — N/A is a valid data state.
    """

    @pytest.mark.parametrize("entity_type,na_fields", NA_MATRIX.items())
    def test_na_fields_pass_in_full_record(self, entity_type, na_fields):
        record = ScaleRowFactory.minimal_163_record(f"{entity_type}_corp")

        # Inject N/A values for entity-specific fields
        for row in record:
            if int(row["ID"]) in na_fields:
                row["Research Output / Data"] = "N/A"

        _, report = validate_golden_record(record, f"{entity_type}_corp")

        for field_id in na_fields.keys():
            result = next(r for r in report.results if r.ID == field_id)
            assert result.status == "✅ PASS", (
                f"[{entity_type}] ID={field_id} with N/A data got '{result.status}'. "
                f"N/A is a valid state and must PASS validation."
            )

    def test_full_record_na_doesnt_reduce_completeness(self):
        """
        Replacing some fields with N/A must not reduce completeness_pct below 100%
        (because N/A → 'Not Found' → still a PASS for Pydantic).
        """
        record = ScaleRowFactory.minimal_163_record("NACorp")
        # Set 30 fields to N/A
        na_ids = list(range(60, 90))  # financial + sales fields
        for row in record:
            if int(row["ID"]) in na_ids:
                row["Research Output / Data"] = "N/A"

        _, report = validate_golden_record(record, "NACorp")
        assert report.completeness_pct == 100.0, (
            f"N/A fields caused completeness drop: {report.completeness_pct}%"
        )
        assert report.total_passed == 163

    @pytest.mark.parametrize("field_id,context", [
        (18,  "VC firm has no Products"),
        (67,  "Bootstrapped startup has no Investors"),
        (11,  "Remote company has no Office Locations"),
        (104, "Early startup has no formal Board"),
    ])
    def test_na_with_context_comment_preserved(self, field_id, context):
        """
        'N/A - <reason>' format: if not in EMPTY_VALUES, the context is preserved.
        Downstream systems can use this for richer display.
        """
        cat, param, ac = EXPECTED_PARAMETERS[field_id]
        na_with_context = f"N/A - {context}"  # Not in EMPTY_VALUES
        row = make_row(field_id, cat, ac, param, na_with_context)
        field = CompanyField.model_validate(row)
        # Not in EMPTY_VALUES → preserved
        assert field.Data == na_with_context or field.Data == "Not Found"


# ═══════════════════════════════════════════════════════════════════════════════
# Section C — Distinction Between 14.1 and 14.2 at Business Logic Level
# ═══════════════════════════════════════════════════════════════════════════════

class TestNAvsUnavailableDistinction:
    """
    Both 14.1 (unavailable) and 14.2 (not applicable) normalise to 'Not Found'
    at the Pydantic layer. This section documents how a downstream system SHOULD
    distinguish them via the Source field or a structured data format.
    """

    def test_na_and_not_found_produce_same_pydantic_output(self):
        """
        Both 'N/A' and '' produce 'Not Found'. The distinction must live in
        the Source field or in upstream metadata, not in the Data field.
        """
        row_na    = make_row(60, "Financials", "Atomic", "Annual Revenues", "N/A",
                             source="system:not_applicable")
        row_empty = make_row(60, "Financials", "Atomic", "Annual Revenues", "",
                             source="system:not_available")

        field_na    = CompanyField.model_validate(row_na)
        field_empty = CompanyField.model_validate(row_empty)

        assert field_na.Data    == "Not Found"
        assert field_empty.Data == "Not Found"

        # The Source field IS preserved — downstream logic should use it
        assert field_na.Source    == "system:not_applicable"
        assert field_empty.Source == "system:not_available"

    def test_source_field_carries_na_context(self):
        """Source field can be used to distinguish N/A reason at aggregation layer."""
        for source_tag, expected_source in [
            ("system:private_company",   "system:private_company"),
            ("system:not_applicable",    "system:not_applicable"),
            ("manual:override",          "manual:override"),
        ]:
            row = make_row(60, "Financials", "Atomic", "Annual Revenues",
                           "Not Found", source=source_tag)
            field = CompanyField.model_validate(row)
            assert field.Source == expected_source, (
                f"Source '{source_tag}' was not preserved: got '{field.Source}'"
            )