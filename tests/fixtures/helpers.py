"""Shared helper assertions and lookups for tests."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


def get_row_by_id(record: Iterable[Dict[str, Any]], field_id: int) -> Optional[Dict[str, Any]]:
    for row in record:
        try:
            if int(row.get("ID")) == int(field_id):
                return row
        except (TypeError, ValueError):
            continue
    return None


def get_data_value(record: Iterable[Dict[str, Any]], field_id: int) -> Optional[str]:
    row = get_row_by_id(record, field_id)
    if not row:
        return None
    return row.get("Research Output / Data")


def assert_record_completeness(
    record: List[Dict[str, Any]],
    expected_count: int = 163,
    company_name: str = "record",
) -> None:
    assert len(record) == expected_count, (
        f"{company_name}: expected {expected_count} rows, got {len(record)}"
    )

    ids: List[int] = []
    for row in record:
        assert "ID" in row, f"{company_name}: row missing ID key: {row}"
        ids.append(int(row["ID"]))

    assert len(set(ids)) == expected_count, (
        f"{company_name}: duplicate IDs found in record"
    )


def assert_no_truncation(text: str, *, min_chars: int = 20) -> None:
    assert isinstance(text, str), "Expected text value"
    cleaned = text.strip()
    assert cleaned, "Text is empty after trimming"
    assert len(cleaned) >= min_chars, (
        f"Text length {len(cleaned)} is below minimum {min_chars}"
    )


def assert_no_cross_contamination(
    record_a: Iterable[Dict[str, Any]],
    record_b: Iterable[Dict[str, Any]],
    *,
    key_id: int = 1,
) -> None:
    a_val = get_data_value(record_a, key_id)
    b_val = get_data_value(record_b, key_id)
    if a_val is None or b_val is None:
        return
    assert a_val != b_val, (
        f"Potential cross-contamination for ID={key_id}: both values are '{a_val}'"
    )
