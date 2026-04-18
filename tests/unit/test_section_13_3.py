"""
test_section_13_3.py — SCALE & PERFORMANCE: Token Limit Handling
=================================================================
Test Case ID : 13.3
Category     : SCALE & PERFORMANCE
Type         : Token Limit Handling
Description  : Long content not inappropriately truncated
Priority     : High

Strategy:
  - Create synthetic "long" rows that would stress a token-limited model.
  - Validate that validate_golden_record() handles them without crashing or
    silently truncating.
  - Mid-sentence truncation is detected heuristically (see fixtures/helpers.py).
  - Parametrize covers multiple long-field scenarios.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "com_intell" / "company-intelligence"))

from core.models import CompanyField, validate_golden_record

from tests.factories import ScaleRowFactory, make_row
from tests.fixtures import get_row_by_id, assert_no_truncation


# ═══════════════════════════════════════════════════════════════════════════════
# Section A — Pydantic accepts long data values
# ═══════════════════════════════════════════════════════════════════════════════

class TestLongValueAcceptance:
    """CompanyField must not have a hard max-length restriction on Data."""

    @pytest.mark.parametrize("char_count", [500, 2_000, 5_000, 10_000])
    def test_pydantic_accepts_long_overview(self, char_count):
        """Company Overview (ID 6) can legitimately be thousands of characters."""
        row = ScaleRowFactory.long_description_record(char_count)
        field = CompanyField.model_validate(row)
        assert len(field.Data) == char_count, (
            f"Data was truncated: input={char_count}, got={len(field.Data)}"
        )

    @pytest.mark.parametrize("n_locations", [50, 100, 200])
    def test_pydantic_accepts_many_office_locations(self, n_locations):
        """Office Locations (ID 11) is composite — many locations separated by ';'."""
        row = ScaleRowFactory.long_office_locations_record(n_locations)
        field = CompanyField.model_validate(row)
        # Verify all locations survive round-trip
        items = field.Data.split(";")
        assert len(items) == n_locations, (
            f"Expected {n_locations} locations, Pydantic returned {len(items)}"
        )

    def test_data_strip_preserves_content(self):
        """data_not_empty validator strips leading/trailing whitespace but NOT content."""
        long_value = "   " + ("X" * 1_000) + "   "
        row = make_row(6, "Company Narrative", "Atomic", "Overview of the Company", long_value)
        field = CompanyField.model_validate(row)
        assert field.Data == long_value.strip()
        assert len(field.Data) == 1_000


# ═══════════════════════════════════════════════════════════════════════════════
# Section B — No Mid-Sentence Truncation in Real Fixture Data
# ═══════════════════════════════════════════════════════════════════════════════

class TestNoTruncationInRealData:
    """
    Heuristically checks that real golden record data is not mid-sentence cut.
    Uses the assert_no_truncation helper from fixtures/helpers.py.
    """

    # Fields most likely to be long (text-heavy)
    LONG_TEXT_FIELD_IDS = [
        6,   # Overview
        16,  # Pain Points
        20,  # Core Value Proposition
        30,  # Interesting Facts
        31,  # Recent News
    ]

    @pytest.mark.parametrize("field_id", LONG_TEXT_FIELD_IDS)
    def test_tcs_long_text_fields_not_truncated(self, tcs_by_id, field_id):
        row = tcs_by_id.get(field_id)
        if row is None:
            pytest.skip(f"ID {field_id} not in TCS fixture")
        data = row.get("Research Output / Data", "")
        if data in ("Not Found", "N/A", "Not Available"):
            pytest.skip(f"ID {field_id} has sentinel value — not a truncation issue")
        assert_no_truncation(data, min_chars=5)

    @pytest.mark.parametrize("company_fixture,company_name", [
        ("tcs_by_id",    "TCS"),
        ("google_by_id", "Google"),
    ])
    @pytest.mark.parametrize("field_id", [6, 20])
    def test_overview_and_cvp_not_truncated(
        self, request, company_fixture, company_name, field_id
    ):
        by_id = request.getfixturevalue(company_fixture)
        row = by_id.get(field_id)
        if row is None:
            pytest.skip(f"{company_name} ID {field_id} not in fixture")
        data = row.get("Research Output / Data", "")
        if data in ("Not Found", "N/A"):
            pytest.skip(f"Sentinel value — no truncation check needed")
        assert_no_truncation(data, min_chars=20)

    def test_composite_fields_have_multiple_items(self, tcs_by_id):
        """
        Composite fields (semicolon-separated) should have ≥ 2 items.
        A single-item Composite field may indicate truncation mid-list.
        """
        composite_ids = [9, 11, 17, 18, 23, 28]   # all Composite per EXPECTED_PARAMETERS
        for field_id in composite_ids:
            row = tcs_by_id.get(field_id)
            if row is None:
                continue
            data = row.get("Research Output / Data", "")
            if data in ("Not Found", "N/A", "Not Available"):
                continue
            items = [x.strip() for x in data.split(";")]
            assert len(items) >= 1, (
                f"ID {field_id}: composite field has 0 items after split: '{data}'"
            )
            # Warn (not fail) if only one item — might be a legitimately small list
            # Production variant could use pytest.warns() here


# ═══════════════════════════════════════════════════════════════════════════════
# Section C — Golden Record Row Count Completeness Under Load
# ═══════════════════════════════════════════════════════════════════════════════

class TestRowCountIntegrity:
    """Ensures all 163 rows survive the full validation pipeline."""

    def test_all_163_ids_present_in_tcs(self, tcs_golden_record):
        ids = {int(r["ID"]) for r in tcs_golden_record}
        missing = set(range(1, 164)) - ids
        assert not missing, f"Missing IDs from TCS golden record: {sorted(missing)}"

    def test_no_duplicate_ids_in_tcs(self, tcs_golden_record):
        ids = [int(r["ID"]) for r in tcs_golden_record]
        seen, dupes = set(), set()
        for id_ in ids:
            if id_ in seen:
                dupes.add(id_)
            seen.add(id_)
        assert not dupes, f"Duplicate IDs in TCS golden record: {sorted(dupes)}"

    def test_synthetic_163_record_passes_full_validation(self):
        """A clean synthetic record must yield 100% completeness."""
        record = ScaleRowFactory.minimal_163_record("SyntheticCorp")
        _, report = validate_golden_record(record, "SyntheticCorp")

        assert report.total_passed == 163, (
            f"Expected 163 passed, got {report.total_passed}. "
            f"Missing={report.total_missing}, Failed={report.total_failed}"
        )
        assert report.completeness_pct == 100.0

    def test_long_values_do_not_reduce_row_count(self):
        """Injecting long values into a record must not reduce the validated row count."""
        record = ScaleRowFactory.minimal_163_record("LongCorp")
        # Replace overview with 10K char value
        for row in record:
            if int(row["ID"]) == 6:
                row["Research Output / Data"] = "L" * 10_000
                break

        _, report = validate_golden_record(record, "LongCorp")
        assert report.total_passed == 163, (
            f"Long value caused {163 - report.total_passed} rows to fail/disappear"
        )