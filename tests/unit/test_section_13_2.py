"""
test_section_13_2.py — SCALE & PERFORMANCE: Response Time
==========================================================
Test Case ID : 13.2
Category     : SCALE & PERFORMANCE
Type         : Response Time
Description  : Generation time for different company types
Priority     : Medium

Strategy:
  - Real LLM calls are NEVER made — graph.invoke() is mocked.
  - Response time is measured around the validate_golden_record() function,
    which is the deterministic part we own and can meaningfully benchmark.
  - Tests assert soft SLA thresholds (configurable via pytest ini or markers).
  - Parametrize covers Fortune 500 companies, startups, and edge-case inputs.

SLA Thresholds (override with --response-time-sla=N via conftest):
  validate_golden_record()  : < 2.0s  for 163-row payload
  Full pipeline (mocked)    : < 0.5s  (no real I/O)
"""
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "com_intell" / "company-intelligence"))

from core.models import validate_golden_record

from tests.factories import ScaleRowFactory
from tests.fixtures import assert_record_completeness

# ── SLA constants (seconds) ────────────────────────────────────────────────────
SLA_VALIDATION_163_ROWS  = 2.0   # validate_golden_record() for a full record
SLA_PIPELINE_MOCKED      = 0.5   # mocked graph.invoke() overhead


# ═══════════════════════════════════════════════════════════════════════════════
# Section A — Baseline Response Time: validate_golden_record()
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidationResponseTime:
    """
    Validate that validate_golden_record() meets SLA for all company types.
    These tests are deterministic because they use fixture data, not real LLMs.
    """

    @pytest.mark.parametrize("company_name,fixture_key", [
        ("TCS",      "tcs_golden_record"),
        ("Google",   "google_golden_record"),
        ("Infosys",  "infosys_golden_record"),
        ("Wipro",    "wipro_golden_record"),
    ])
    def test_validation_sla_per_company(
        self, request, company_name, fixture_key
    ):
        """validate_golden_record() must complete within SLA for each company."""
        record = request.getfixturevalue(fixture_key)

        start = time.perf_counter()
        valid_rows, report = validate_golden_record(record, company_name)
        elapsed = time.perf_counter() - start

        assert elapsed < SLA_VALIDATION_163_ROWS, (
            f"{company_name}: validation took {elapsed:.3f}s, "
            f"SLA is {SLA_VALIDATION_163_ROWS}s"
        )
        # Also sanity-check the output is not empty
        assert report.total_passed > 0, f"{company_name}: No fields passed validation"

    def test_validation_sla_minimal_synthetic_record(self):
        """Synthetic minimal 163-row record: validation SLA must hold."""
        record = ScaleRowFactory.minimal_163_record("SyntheticCorp")
        assert_record_completeness(record, 163, "SyntheticCorp")

        start = time.perf_counter()
        valid_rows, report = validate_golden_record(record, "SyntheticCorp")
        elapsed = time.perf_counter() - start

        assert elapsed < SLA_VALIDATION_163_ROWS, (
            f"Synthetic record validation: {elapsed:.3f}s > SLA {SLA_VALIDATION_163_ROWS}s"
        )
        assert report.total_passed == 163

    def test_validation_sla_partial_record(self):
        """Partial (50-row) record should validate faster than full record."""
        record = ScaleRowFactory.minimal_163_record("PartialCorp")[:50]

        start = time.perf_counter()
        _, report = validate_golden_record(record, "PartialCorp")
        elapsed = time.perf_counter() - start

        # Partial records are cheap — use 1/3 of full SLA
        assert elapsed < SLA_VALIDATION_163_ROWS / 3, (
            f"Partial record took {elapsed:.3f}s, expected < {SLA_VALIDATION_163_ROWS/3:.3f}s"
        )
        assert report.total_missing == 163 - 50

    def test_validation_sla_empty_record(self):
        """Empty record must still return a complete report instantly."""
        start = time.perf_counter()
        _, report = validate_golden_record([], "EmptyCorp")
        elapsed = time.perf_counter() - start

        assert elapsed < 0.5, f"Empty record validation took {elapsed:.3f}s"
        assert report.total_missing == 163
        assert report.total_passed == 0
        assert report.completeness_pct == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# Section B — Mocked Pipeline Overhead
# ═══════════════════════════════════════════════════════════════════════════════

class TestMockedPipelineResponseTime:
    """
    Validates that the mocked graph.invoke() call itself is fast.
    This provides a baseline: if tests are slow, the mock is the bottleneck,
    not the business logic.
    """

    def test_mocked_graph_invoke_is_fast(self, mock_graph, tcs_golden_record):
        """Mock invoke() should return in < 500ms (zero I/O overhead)."""
        import graph  # noqa: F401 — patched by mock_graph fixture

        start = time.perf_counter()
        result = mock_graph.invoke({"company_name": "TCS"})
        elapsed = time.perf_counter() - start

        assert elapsed < SLA_PIPELINE_MOCKED, (
            f"Mocked graph.invoke() took {elapsed:.3f}s, expected < {SLA_PIPELINE_MOCKED}s"
        )
        assert result["golden_record"] == tcs_golden_record

    @pytest.mark.parametrize("company", ["TCS", "Google", "Infosys", "Wipro"])
    def test_mocked_pipeline_sla_multiple_companies(
        self, mock_graph_factory, company
    ):
        """Each company's mocked pipeline should be fast."""
        mock = mock_graph_factory(company)

        start = time.perf_counter()
        result = mock.invoke({"company_name": company})
        elapsed = time.perf_counter() - start

        assert elapsed < SLA_PIPELINE_MOCKED, (
            f"{company} mock invoke: {elapsed:.3f}s > SLA {SLA_PIPELINE_MOCKED}s"
        )
        assert result["golden_record"], f"{company}: golden_record should not be empty"


# ═══════════════════════════════════════════════════════════════════════════════
# Section C — Validation Output Correctness Under Time Constraint
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidationCorrectnessWithTiming:
    """
    Combined: ensures both SLA and correctness are satisfied simultaneously.
    A fast but wrong result is not acceptable.
    """

    @pytest.mark.parametrize("company_name,fixture_key,min_completeness", [
        ("TCS",     "tcs_golden_record",    99.0),
        ("Google",  "google_golden_record",  10.0),   # partial fixture; lower threshold
        ("Wipro",   "wipro_golden_record",   10.0),
    ])
    def test_completeness_within_sla(
        self, request, company_name, fixture_key, min_completeness
    ):
        record = request.getfixturevalue(fixture_key)

        start = time.perf_counter()
        _, report = validate_golden_record(record, company_name)
        elapsed = time.perf_counter() - start

        # SLA check
        assert elapsed < SLA_VALIDATION_163_ROWS, (
            f"{company_name}: took {elapsed:.3f}s > SLA"
        )
        # Correctness check
        assert report.completeness_pct >= min_completeness, (
            f"{company_name}: completeness {report.completeness_pct}% "
            f"< minimum {min_completeness}%"
        )

    def test_report_structure_complete(self, tcs_golden_record):
        """ValidationReport must always have exactly 163 result entries."""
        _, report = validate_golden_record(tcs_golden_record, "TCS")

        assert len(report.results) == 163, (
            f"Report has {len(report.results)} results, expected 163"
        )
        assert report.total_expected == 163
        assert (
            report.total_passed + report.total_missing + report.total_failed == 163
        ), "passed + missing + failed must sum to 163"