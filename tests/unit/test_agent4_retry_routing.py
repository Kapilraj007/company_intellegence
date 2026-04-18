"""Unit tests for Agent 4 remediation routing helpers."""

from agents.agent4_test_runner import (
    extract_failed_parameter_ids,
    route_after_tests,
)


def test_extract_failed_parameter_ids_with_explicit_id_patterns():
    results = {
        "failed_tests": [
            {
                "name": "tests/unit/test_section_14_1.py::test_not_found_accepted_for_every_field[id_77]",
                "message": "AssertionError: ID=77 (Customer Concentration Risk) failed",
            },
            {
                "name": "tests/integration/test_live_output.py::TestLivePipelineOutput::test_risk_field_passes[77-Customer Concentration Risk]",
                "message": "ID 88 not in report",
            },
        ],
        "error_tests": [
            {
                "name": "tests/integration/test_live_output.py::test_zero_failed_rows",
                "message": "RuntimeError: ID=164 should be ignored; ID=87 should be kept",
            }
        ],
    }
    assert extract_failed_parameter_ids(results) == [77, 87, 88]


def test_extract_failed_parameter_ids_ignores_non_id_numbers():
    results = {
        "failed_tests": [
            {
                "name": "tests/integration/test_live_output.py::test_163_rows_present",
                "message": "Expected 163 rows, got 160",
            }
        ],
        "error_tests": [],
    }
    assert extract_failed_parameter_ids(results) == []


def test_route_after_tests_passed_goes_to_end():
    state = {
        "test_results": {"all_passed": True, "failed_parameter_ids": []},
        "pytest_retry_count": 0,
    }
    assert route_after_tests(state) == "end"


def test_route_after_tests_retry_when_ids_available():
    state = {
        "test_results": {"all_passed": False, "failed_parameter_ids": [77, 88]},
        "pytest_retry_count": 1,
    }
    assert route_after_tests(state) == "retry_via_llm1"


def test_route_after_tests_end_when_no_parameter_ids():
    state = {
        "test_results": {"all_passed": False, "failed_parameter_ids": []},
        "pytest_retry_count": 0,
    }
    assert route_after_tests(state) == "end"
