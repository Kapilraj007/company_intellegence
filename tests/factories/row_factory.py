"""Factories for test rows used across unit test modules."""

from __future__ import annotations

from typing import Any, Dict, List

from core.models import EXPECTED_PARAMETERS


def make_row(
    field_id: int,
    category: str,
    ac: str,
    parameter: str,
    data: str,
    *,
    source: str = "tests:factory",
) -> Dict[str, Any]:
    """Build a row using the schema expected by CompanyField."""
    return {
        "ID": field_id,
        "Category": category,
        "A/C": ac,
        "Parameter": parameter,
        "Research Output / Data": data,
        "Source": source,
    }


class ScaleRowFactory:
    """Factories used by scale/performance tests."""

    @staticmethod
    def minimal_163_record(company_name: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for field_id in range(1, 164):
            category, parameter, ac = EXPECTED_PARAMETERS[field_id]
            if field_id == 1:
                data = company_name
            elif ac == "Composite":
                data = f"{parameter} - {company_name};{parameter} detail"
            else:
                data = f"{parameter} - {company_name}"
            rows.append(make_row(field_id, category, ac, parameter, data))
        return rows

    @staticmethod
    def long_description_record(char_count: int) -> Dict[str, Any]:
        category, parameter, ac = EXPECTED_PARAMETERS[6]
        return make_row(6, category, ac, parameter, "X" * char_count)

    @staticmethod
    def long_office_locations_record(n_locations: int) -> Dict[str, Any]:
        category, parameter, ac = EXPECTED_PARAMETERS[11]
        locations = ";".join(f"Location {i}" for i in range(1, n_locations + 1))
        return make_row(11, category, ac, parameter, locations)


class RiskRowFactory:
    """Factories for Section 12.5 risk-related field test data."""

    @staticmethod
    def customer_concentration(level: str = "Low") -> Dict[str, Any]:
        return make_row(77, "Sales & Growth", "Atomic", "Customer Concentration Risk", level)

    @staticmethod
    def customer_concentration_empty() -> Dict[str, Any]:
        return make_row(77, "Sales & Growth", "Atomic", "Customer Concentration Risk", "")

    @staticmethod
    def burn_rate(level: str = "High") -> Dict[str, Any]:
        return make_row(78, "Sales & Growth", "Atomic", "Burn Rate", level)

    @staticmethod
    def geopolitical(value: str = "Trade sanctions;Regional conflict risk") -> Dict[str, Any]:
        return make_row(87, "Operations", "Composite", "Geopolitical Risks", value)

    @staticmethod
    def macro(value: str = "Inflation volatility;Interest rate uncertainty") -> Dict[str, Any]:
        return make_row(88, "Operations", "Composite", "Macro Risks", value)

    @staticmethod
    def burn_rate_id_over_limit() -> Dict[str, Any]:
        return make_row(164, "Sales & Growth", "Atomic", "Burn Rate", "High")

    @staticmethod
    def burn_rate_negative_id() -> Dict[str, Any]:
        return make_row(-1, "Sales & Growth", "Atomic", "Burn Rate", "High")


class InvalidRowFactory:
    """Factories for intentionally invalid rows."""

    @staticmethod
    def id_zero() -> Dict[str, Any]:
        return make_row(0, "Sales & Growth", "Atomic", "Customer Concentration Risk", "Low")

    @staticmethod
    def wrong_ac_value() -> Dict[str, Any]:
        return make_row(77, "Sales & Growth", "SomethingElse", "Customer Concentration Risk", "Low")

    @staticmethod
    def missing_required_field(field_name: str) -> Dict[str, Any]:
        row = make_row(77, "Sales & Growth", "Atomic", "Customer Concentration Risk", "Low")
        row.pop(field_name, None)
        return row

    @staticmethod
    def null_category() -> Dict[str, Any]:
        row = make_row(77, "Sales & Growth", "Atomic", "Customer Concentration Risk", "Low")
        row["Category"] = None
        return row


class NullRowFactory:
    """Factories for 14.1/14.2 null and N/A scenarios."""

    @staticmethod
    def private_company_revenue() -> Dict[str, Any]:
        return make_row(60, "Financials", "Atomic", "Annual Revenues", "Not Found")

    @staticmethod
    def private_company_revenue_with_empty_string() -> Dict[str, Any]:
        return make_row(60, "Financials", "Atomic", "Annual Revenues", "")

    @staticmethod
    def vc_firm_products_na() -> Dict[str, Any]:
        return make_row(18, "Business Model", "Composite", "Services / Offerings / Products", "N/A")

    @staticmethod
    def remote_company_office_na() -> Dict[str, Any]:
        return make_row(
            11,
            "Geographic Presence",
            "Composite",
            "Office Locations",
            "N/A - Fully Remote",
        )
