"""Rule-based hallucination guardrails for golden-record rows."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

EMPTY_VALUES = {"not found", "n/a", "unknown", "none", "null", "", "-", "na"}
PLACEHOLDER_NAMES = {
    "john smith",
    "jane doe",
    "test user",
    "example person",
}
GENERIC_EMAIL_LOCALS = {
    "john.smith",
    "jane.doe",
    "test",
    "sample",
    "demo",
    "placeholder",
}
VAGUE_VALUES = {
    "significant",
    "substantial",
    "standard",
    "reduced",
    "various",
    "multiple",
    "high",
    "low",
    "moderate",
}

STRUCTURED_NUMERIC_IDS = {12, 60, 61, 63, 64, 66, 69, 72, 73, 74, 78, 79, 80, 108, 109, 110}
PUBLIC_STARTUP_METRIC_IDS = {74, 78, 79, 80}
PUBLIC_FUNDING_RELATED_IDS = {67, 68, 69}

_NUMBER_RE = re.compile(
    r"(-?\d+(?:,\d{3})*(?:\.\d+)?)\s*(k|m|b|t|thousand|million|billion|trillion)?",
    flags=re.IGNORECASE,
)
_EMAIL_RE = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", flags=re.IGNORECASE)


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _data(row: Dict[str, Any] | None) -> str:
    if not row:
        return ""
    value = row.get("Research Output / Data")
    return "" if value is None else str(value).strip()


def _is_missing(value: str) -> bool:
    return value.strip().lower() in EMPTY_VALUES


def _parse_scaled_number(raw: str) -> float | None:
    text = raw.replace(",", "").strip().lower()
    match = _NUMBER_RE.search(text)
    if not match:
        return None

    number = float(match.group(1))
    suffix = (match.group(2) or "").lower()

    if not suffix:
        if "trillion" in text:
            suffix = "trillion"
        elif "billion" in text:
            suffix = "billion"
        elif "million" in text:
            suffix = "million"
        elif "thousand" in text:
            suffix = "thousand"

    factor = {
        "": 1.0,
        "k": 1e3,
        "thousand": 1e3,
        "m": 1e6,
        "million": 1e6,
        "b": 1e9,
        "billion": 1e9,
        "t": 1e12,
        "trillion": 1e12,
    }.get(suffix, 1.0)

    return number * factor


def _is_public_company(nature_value: str) -> bool:
    text = nature_value.lower()
    public_markers = ("public", "listed", "nasdaq", "nyse", "stock exchange")
    return any(marker in text for marker in public_markers)


def _looks_placeholder_identity(parameter: str, value: str) -> bool:
    text = value.lower().strip()
    if not text:
        return False

    if text in PLACEHOLDER_NAMES:
        return True

    if _EMAIL_RE.match(text):
        local = text.split("@", 1)[0]
        local_alpha = re.sub(r"[^a-z]", "", local)
        if local in GENERIC_EMAIL_LOCALS:
            return True
        if local_alpha in {"johnsmith", "janedoe", "testuser", "exampleperson"}:
            return True

    # Tightest checks for high-risk personal contact fields.
    if "primary contact" in parameter.lower():
        if "john smith" in text or "jane doe" in text:
            return True

    return False


def _contains_vague_token(value: str) -> bool:
    lower = value.lower()
    for token in VAGUE_VALUES:
        if re.search(rf"\b{re.escape(token)}\b", lower):
            return True
    return False


def apply_hallucination_guardrails(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    """
    Apply deterministic rule checks and conservative sanitization.

    Returns:
      sanitized_rows
      issues (dict list)
      sanitized_count
    """
    sanitized_rows = [dict(row) for row in rows]
    by_id: Dict[int, Dict[str, Any]] = {}
    for row in sanitized_rows:
        row_id = _to_int(row.get("ID"))
        if row_id is not None:
            by_id[row_id] = row

    issues: List[Dict[str, Any]] = []
    seen = set()
    sanitized_count = 0

    def add_issue(
        row: Dict[str, Any],
        *,
        rule: str,
        severity: str,
        message: str,
        sanitize: bool = False,
    ) -> None:
        nonlocal sanitized_count

        row_id = _to_int(row.get("ID")) or -1
        key = (row_id, rule, message)
        if key in seen:
            return
        seen.add(key)

        value = _data(row)
        action = "none"
        if sanitize and not _is_missing(value):
            row["Research Output / Data"] = "Not Found"
            action = "normalized_to_not_found"
            sanitized_count += 1
            value = "Not Found"

        issues.append(
            {
                "ID": row_id,
                "Parameter": str(row.get("Parameter", "")),
                "severity": severity,
                "rule": rule,
                "message": message,
                "value": value,
                "action": action,
            }
        )

    # Rule 1: Placeholder identity detection.
    for row in sanitized_rows:
        value = _data(row)
        if _is_missing(value):
            continue
        parameter = str(row.get("Parameter", ""))
        if _looks_placeholder_identity(parameter, value):
            add_issue(
                row,
                rule="placeholder_identity",
                severity="critical",
                message="Detected placeholder identity-like value.",
                sanitize=True,
            )

    # Rule 2: Vague values in numeric/structured fields.
    for row in sanitized_rows:
        row_id = _to_int(row.get("ID"))
        if row_id not in STRUCTURED_NUMERIC_IDS:
            continue
        value = _data(row)
        if _is_missing(value):
            continue
        if _contains_vague_token(value) and not re.search(r"\d", value):
            add_issue(
                row,
                rule="vague_value_in_numeric_field",
                severity="warning",
                message="Vague non-numeric value in structured numeric field.",
                sanitize=True,
            )

    # Rule 3: Startup-only metrics on public companies.
    nature_row = by_id.get(7)
    is_public = _is_public_company(_data(nature_row))
    if is_public:
        for row_id in PUBLIC_STARTUP_METRIC_IDS:
            row = by_id.get(row_id)
            if not row:
                continue
            value = _data(row)
            if _is_missing(value):
                continue
            add_issue(
                row,
                rule="public_company_startup_metric",
                severity="critical",
                message="Startup metric populated for public company.",
                sanitize=True,
            )
        for row_id in PUBLIC_FUNDING_RELATED_IDS:
            row = by_id.get(row_id)
            if not row:
                continue
            value = _data(row)
            if _is_missing(value):
                continue
            add_issue(
                row,
                rule="public_company_funding_signal",
                severity="warning",
                message="Funding-related field populated for public company; verify manually.",
            )

    # Rule 4: Numeric sanity checks.
    revenue = _parse_scaled_number(_data(by_id.get(60)))
    tam = _parse_scaled_number(_data(by_id.get(108)))
    sam = _parse_scaled_number(_data(by_id.get(109)))
    som = _parse_scaled_number(_data(by_id.get(110)))

    if revenue and tam and tam < revenue and by_id.get(108):
        add_issue(
            by_id[108],
            rule="tam_less_than_revenue",
            severity="critical",
            message="TAM is lower than annual revenue; value likely inconsistent.",
            sanitize=True,
        )

    if tam and sam and sam > tam and by_id.get(109):
        add_issue(
            by_id[109],
            rule="sam_exceeds_tam",
            severity="warning",
            message="SAM exceeds TAM; verify market size values.",
            sanitize=True,
        )

    if sam and som and som > sam and by_id.get(110):
        add_issue(
            by_id[110],
            rule="som_exceeds_sam",
            severity="warning",
            message="SOM exceeds SAM; verify market size values.",
            sanitize=True,
        )

    # Rule 5: Cross-field consistency (revenue per employee).
    employee_count = _parse_scaled_number(_data(by_id.get(12)))
    if revenue and employee_count and employee_count > 0:
        ratio = revenue / employee_count
        if ratio > 5_000_000 or ratio < 1_000:
            # Flag both fields for easier downstream triage.
            if by_id.get(60):
                add_issue(
                    by_id[60],
                    rule="revenue_per_employee_outlier",
                    severity="warning",
                    message=f"Revenue per employee outlier: ${ratio:,.0f}.",
                )
            if by_id.get(12):
                add_issue(
                    by_id[12],
                    rule="revenue_per_employee_outlier",
                    severity="warning",
                    message=f"Revenue per employee outlier: ${ratio:,.0f}.",
                )

    return sanitized_rows, issues, sanitized_count
