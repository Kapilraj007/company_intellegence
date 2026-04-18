"""Rule-based hallucination guardrail tests for golden-record validation."""

from core.models import validate_golden_record
from tests.factories import ScaleRowFactory
from tests.fixtures import get_row_by_id


def _set_data(record: list[dict], field_id: int, value: str) -> None:
    row = get_row_by_id(record, field_id)
    assert row is not None, f"Missing ID={field_id} in fixture record"
    row["Research Output / Data"] = value


def test_placeholder_contact_name_is_normalized_to_not_found():
    record = ScaleRowFactory.minimal_163_record("Microsoft")
    _set_data(record, 51, "John Smith")

    valid_rows, report = validate_golden_record(record, "Microsoft")
    row_51 = get_row_by_id(valid_rows, 51)

    assert row_51 is not None
    assert row_51["Research Output / Data"] == "Not Found"
    assert any(i.rule == "placeholder_identity" and i.ID == 51 for i in report.hallucination_issues)


def test_vague_numeric_value_is_normalized_to_not_found():
    record = ScaleRowFactory.minimal_163_record("Microsoft")
    _set_data(record, 60, "Significant")

    valid_rows, report = validate_golden_record(record, "Microsoft")
    row_60 = get_row_by_id(valid_rows, 60)

    assert row_60 is not None
    assert row_60["Research Output / Data"] == "Not Found"
    assert any(
        i.rule == "vague_value_in_numeric_field" and i.ID == 60
        for i in report.hallucination_issues
    )


def test_public_company_startup_metric_is_normalized():
    record = ScaleRowFactory.minimal_163_record("Microsoft")
    _set_data(record, 7, "Public listed company (NASDAQ)")
    _set_data(record, 74, "Not Found")
    _set_data(record, 79, "Not Found")
    _set_data(record, 80, "Not Found")
    _set_data(record, 67, "Not Found")
    _set_data(record, 68, "Not Found")
    _set_data(record, 69, "Not Found")
    _set_data(record, 78, "$100M per month")

    valid_rows, report = validate_golden_record(record, "Microsoft")
    row_78 = get_row_by_id(valid_rows, 78)

    assert row_78 is not None
    assert row_78["Research Output / Data"] == "Not Found"
    assert any(
        i.rule == "public_company_startup_metric" and i.ID == 78 and i.severity == "critical"
        for i in report.hallucination_issues
    )


def test_tam_less_than_revenue_is_normalized():
    record = ScaleRowFactory.minimal_163_record("Microsoft")
    _set_data(record, 60, "$230B")
    _set_data(record, 108, "$100M")

    valid_rows, report = validate_golden_record(record, "Microsoft")
    row_108 = get_row_by_id(valid_rows, 108)

    assert row_108 is not None
    assert row_108["Research Output / Data"] == "Not Found"
    assert any(i.rule == "tam_less_than_revenue" and i.ID == 108 for i in report.hallucination_issues)


def test_revenue_per_employee_outlier_is_flagged():
    record = ScaleRowFactory.minimal_163_record("Microsoft")
    _set_data(record, 60, "$230B")
    _set_data(record, 12, "200")

    _, report = validate_golden_record(record, "Microsoft")

    assert any(
        i.rule == "revenue_per_employee_outlier" and i.ID in {12, 60}
        for i in report.hallucination_issues
    )
