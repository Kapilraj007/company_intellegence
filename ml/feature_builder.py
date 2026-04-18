from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Sequence

import numpy as np
from sklearn.decomposition import PCA

from core.local_store import get_local_store_client
from core.user_scope import require_user_id
from ml.clustering.cluster_pipeline import get_innovation_cluster_pipeline

_EMPTY_VALUES = {"not found", "n/a", "na", "unknown", "none", "null", "", "-", "not applicable"}
_MULTIPLIERS = {
    "thousand": 1_000.0,
    "k": 1_000.0,
    "million": 1_000_000.0,
    "m": 1_000_000.0,
    "billion": 1_000_000_000.0,
    "b": 1_000_000_000.0,
    "trillion": 1_000_000_000_000.0,
    "t": 1_000_000_000_000.0,
}
_NUMBER_RE = re.compile(r"-?\d+(?:,\d{3})*(?:\.\d+)?")

_LEVEL_SCORES = {
    "rapidly expanding": 1.0,
    "industry leader": 0.95,
    "leader": 0.9,
    "excellent": 0.9,
    "high": 0.85,
    "strong": 0.82,
    "positive": 0.8,
    "good": 0.7,
    "above average": 0.72,
    "moderate": 0.55,
    "medium": 0.55,
    "balanced": 0.6,
    "stable": 0.55,
    "average": 0.5,
    "low": 0.25,
    "weak": 0.22,
    "negative": 0.18,
    "poor": 0.15,
}
_PROFITABILITY_SCORES = {
    "profitable": 0.95,
    "break even": 0.55,
    "breakeven": 0.55,
    "loss making": 0.2,
    "unprofitable": 0.2,
}
_FUTURE_SCORES = {
    "growth": 0.85,
    "positive": 0.8,
    "expansion": 0.85,
    "expanding": 0.82,
    "strong": 0.8,
    "stable": 0.55,
    "moderate": 0.5,
    "decline": 0.2,
    "negative": 0.15,
}
_AI_SCORES = {
    "very high": 1.0,
    "high": 0.85,
    "moderate": 0.55,
    "medium": 0.55,
    "average": 0.5,
    "low": 0.2,
}


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in _EMPTY_VALUES else text


def _split_multi(value: Any) -> List[str]:
    text = _clean_text(value)
    if not text:
        return []
    pieces = re.split(r"[;,|]+", text)
    cleaned = [piece.strip() for piece in pieces if piece.strip()]
    return cleaned


def _extract_numbers(text: str) -> List[float]:
    return [float(match.replace(",", "")) for match in _NUMBER_RE.findall(text)]


def _numeric_multiplier(text: str) -> float:
    lowered = text.lower()
    if "trillion" in lowered or re.search(r"\d[\d,.]*\s*t\b", lowered) or re.search(r"\d[\d,.]*t\b", lowered):
        return _MULTIPLIERS["trillion"]
    if "billion" in lowered or re.search(r"\d[\d,.]*\s*b\b", lowered) or re.search(r"\d[\d,.]*b\b", lowered):
        return _MULTIPLIERS["billion"]
    if "million" in lowered or re.search(r"\d[\d,.]*\s*m\b", lowered) or re.search(r"\d[\d,.]*m\b", lowered):
        return _MULTIPLIERS["million"]
    if "thousand" in lowered or re.search(r"\d[\d,.]*\s*k\b", lowered) or re.search(r"\d[\d,.]*k\b", lowered):
        return _MULTIPLIERS["thousand"]
    return 1.0


def _parse_numeric(value: Any) -> float | None:
    text = _clean_text(value)
    if not text:
        return None

    numbers = _extract_numbers(text)
    if not numbers:
        return None

    if "/5" in text and numbers:
        return numbers[0]

    base_value = float(sum(numbers) / len(numbers))
    return base_value * _numeric_multiplier(text)


def _parse_percent(value: Any) -> float | None:
    numeric = _parse_numeric(value)
    if numeric is None:
        return None
    text = _clean_text(value)
    if "%" in text or numeric > 1.0:
        return numeric
    return numeric * 100.0


def _safe_log(value: float | None) -> float:
    return float(np.log1p(max(value or 0.0, 0.0)))


def _keyword_score(value: Any, mapping: Dict[str, float], default: float = 0.0) -> float:
    text = _clean_text(value).lower()
    if not text:
        return default

    matches = [score for phrase, score in mapping.items() if phrase in text]
    if not matches:
        return default
    return round(max(matches), 4)


def _normalize_vector(vector: Sequence[float] | None) -> List[float]:
    arr = np.asarray(vector or [], dtype="float32")
    if arr.ndim != 1 or arr.size == 0:
        return []
    norm = float(np.linalg.norm(arr)) or 1.0
    return (arr / norm).tolist()


def _median_impute(matrix: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return matrix
    imputed = matrix.copy()
    for idx in range(imputed.shape[1]):
        column = imputed[:, idx]
        mask = np.isnan(column)
        if not mask.any():
            continue
        valid = column[~mask]
        fill = float(np.median(valid)) if valid.size else 0.0
        column[mask] = fill
        imputed[:, idx] = column
    return imputed


class CompanyFeatureBuilder:
    def __init__(
        self,
        *,
        local_store: Any | None = None,
        vector_collector: Callable[..., List[Dict[str, Any]]] | None = None,
    ) -> None:
        self._local_store = local_store or get_local_store_client()
        self._vector_collector = vector_collector or get_innovation_cluster_pipeline().collect_company_vectors

    def build_feature_rows(
        self,
        *,
        company_ids: Sequence[str] | None = None,
        company_names: Sequence[str] | None = None,
        limit: int | None = None,
        user_id: str,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="feature row build")
        docs = self._select_company_docs(
            company_ids=company_ids,
            company_names=company_names,
            limit=limit,
            user_id=user_id,
        )
        vector_rows = self._vector_collector(
            company_ids=company_ids,
            company_names=company_names,
            limit=limit,
            user_id=user_id,
        )
        vector_map = {
            str(row.get("company_id") or "").strip(): row
            for row in vector_rows
            if isinstance(row, dict) and str(row.get("company_id") or "").strip()
        }

        feature_rows: List[Dict[str, Any]] = []
        for doc in docs:
            row = self._build_feature_row(doc, vector_map.get(str(doc.get("company_id") or "").strip()))
            if row is not None:
                feature_rows.append(row)

        feature_rows.sort(key=lambda item: str(item.get("company_name") or "").lower())
        return feature_rows

    def build_model_matrix(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        vector_components: int = 6,
    ) -> Dict[str, Any]:
        numeric_features = [
            "employee_size_log",
            "office_count_log",
            "country_count",
            "industry_count",
            "product_count",
            "customer_segment_count",
            "technology_term_count",
            "innovation_term_count",
            "ai_adoption_score",
            "hiring_velocity_score",
            "benchmark_score",
            "future_projection_score",
            "profitability_score",
            "brand_sentiment_score_numeric",
            "global_exposure_score",
            "partnership_strength_score",
            "annual_revenue_log",
            "annual_profit_log",
            "profit_margin_pct",
            "growth_rate_pct",
            "market_share_pct",
            "net_promoter_score",
            "churn_rate_pct",
            "rd_investment_log",
            "tam_log",
            "sam_log",
            "som_log",
            "valuation_log",
            "vector_count",
            "vector_strength",
            "category_diversity",
        ]

        matrix = np.asarray(
            [
                [self._to_float(row.get(feature_name)) for feature_name in numeric_features]
                for row in rows
            ],
            dtype="float32",
        )
        matrix = _median_impute(matrix)
        feature_names = list(numeric_features)

        raw_vectors = [
            np.asarray(row.get("vector") or [], dtype="float32")
            for row in rows
            if isinstance(row.get("vector"), list) and row.get("vector")
        ]
        vector_length = raw_vectors[0].shape[0] if raw_vectors else 0
        if vector_length and all(vector.ndim == 1 and vector.shape[0] == vector_length for vector in raw_vectors):
            vectors = np.vstack(raw_vectors)
        else:
            vectors = np.asarray([], dtype="float32")

        if vectors.ndim == 2 and vectors.shape[0] == len(rows) and vectors.shape[0] >= 2 and vectors.shape[1] >= 2:
            component_count = max(1, min(int(vector_components), vectors.shape[0], vectors.shape[1]))
            reducer = PCA(n_components=component_count, random_state=42)
            reduced = reducer.fit_transform(vectors)
            matrix = np.hstack([matrix, reduced.astype("float32")])
            feature_names.extend([f"vector_component_{index + 1:02d}" for index in range(component_count)])

        return {
            "rows": list(rows),
            "matrix": matrix.astype("float32"),
            "feature_names": feature_names,
        }

    def _select_company_docs(
        self,
        *,
        company_ids: Sequence[str] | None = None,
        company_names: Sequence[str] | None = None,
        limit: int | None = None,
        user_id: str,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="feature company selection")
        if hasattr(self._local_store, "list_companies"):
            rows = list(self._local_store.list_companies(user_id=user_id))
        elif hasattr(self._local_store, "get_companies_full_data"):
            fetched = self._local_store.get_companies_full_data(
                company_ids=list(company_ids or []),
                company_names=list(company_names or []),
                user_id=user_id,
            )
            rows = list(fetched.values()) if isinstance(fetched, dict) else []
        else:
            rows = []

        wanted_ids = {str(company_id).strip() for company_id in company_ids or [] if str(company_id).strip()}
        wanted_names = {str(company_name).strip().lower() for company_name in company_names or [] if str(company_name).strip()}

        filtered = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            company_id = str(row.get("company_id") or "").strip()
            company_name = str(row.get("company_name") or "").strip()
            if wanted_ids and company_id not in wanted_ids:
                continue
            if wanted_names and company_name.lower() not in wanted_names:
                continue
            filtered.append(row)

        filtered.sort(key=lambda item: str(item.get("company_name") or item.get("company_id") or "").lower())
        return filtered[: max(1, int(limit))] if limit is not None else filtered

    def _build_feature_row(
        self,
        doc: Dict[str, Any],
        vector_row: Dict[str, Any] | None,
    ) -> Dict[str, Any] | None:
        company_id = str(doc.get("company_id") or "").strip()
        company_name = str(doc.get("company_name") or company_id or "Unknown").strip()
        consolidated = doc.get("consolidated") if isinstance(doc.get("consolidated"), dict) else {}
        consolidated_json = consolidated.get("json") if isinstance(consolidated.get("json"), dict) else {}
        if not company_id or not consolidated_json:
            return None

        category = _clean_text(consolidated_json.get("category")) or "Unknown"
        industries = _split_multi(consolidated_json.get("focus_sectors_industries"))
        products = _split_multi(consolidated_json.get("services_offerings_products"))
        customer_segments = _split_multi(consolidated_json.get("top_customers_by_client_segments"))
        technology_terms = list(
            dict.fromkeys(
                _split_multi(consolidated_json.get("tech_stack_tools_used"))
                + _split_multi(consolidated_json.get("innovation_roadmap"))
                + _split_multi(consolidated_json.get("industry_benchmark_tech_adoption"))
            )
        )
        innovation_terms = list(
            dict.fromkeys(
                _split_multi(consolidated_json.get("product_pipeline"))
                + _split_multi(consolidated_json.get("strategic_priorities"))
                + _split_multi(consolidated_json.get("unique_differentiators"))
                + _split_multi(consolidated_json.get("core_value_proposition"))
            )
        )
        dominant_categories = list(vector_row.get("dominant_categories") or []) if isinstance(vector_row, dict) else []
        vector = _normalize_vector(vector_row.get("vector") if isinstance(vector_row, dict) else [])

        annual_revenue = _parse_numeric(consolidated_json.get("annual_revenues"))
        annual_profit = _parse_numeric(consolidated_json.get("annual_profits"))
        employee_size = _parse_numeric(consolidated_json.get("employee_size"))
        office_count = _parse_numeric(consolidated_json.get("number_of_offices"))
        rd_investment = _parse_numeric(consolidated_json.get("rd_investment"))
        tam = _parse_numeric(consolidated_json.get("total_addressable_market_tam"))
        sam = _parse_numeric(consolidated_json.get("serviceable_addressable_market"))
        som = _parse_numeric(consolidated_json.get("serviceable_obtainable_market"))
        valuation = _parse_numeric(consolidated_json.get("company_valuation"))
        growth_rate_pct = _parse_percent(consolidated_json.get("year_over_year_growth_rate"))
        market_share_pct = _parse_percent(consolidated_json.get("market_share"))
        churn_rate_pct = _parse_percent(consolidated_json.get("churn_rate"))
        employee_turnover_pct = _parse_percent(consolidated_json.get("employee_turnover"))
        brand_sentiment_numeric = _parse_percent(consolidated_json.get("brand_sentiment_score"))
        net_promoter_score = _parse_numeric(consolidated_json.get("net_promoter_score"))
        customer_acquisition_cost = _parse_numeric(consolidated_json.get("customer_acquisition_cost"))
        customer_lifetime_value = _parse_numeric(consolidated_json.get("customer_lifetime_value"))
        retention_tenure = _parse_numeric(consolidated_json.get("average_retention_tenure"))

        profit_margin_pct = None
        if annual_revenue and annual_profit is not None and annual_revenue > 0:
            profit_margin_pct = (annual_profit / annual_revenue) * 100.0

        return {
            "company_id": company_id,
            "company_name": company_name,
            "sector": category,
            "industries": industries,
            "products": products,
            "customer_segments": customer_segments,
            "technology_terms": technology_terms,
            "innovation_terms": innovation_terms,
            "countries": _split_multi(consolidated_json.get("countries_operating_in")),
            "dominant_categories": dominant_categories,
            "vector_source": str(vector_row.get("vector_source") or "unknown") if isinstance(vector_row, dict) else "unknown",
            "vector_count": int(vector_row.get("vector_count") or 0) if isinstance(vector_row, dict) else 0,
            "vector": vector,
            "vector_strength": round(float(np.linalg.norm(np.asarray(vector or [0.0], dtype="float32"))), 4) if vector else 0.0,
            "category_diversity": len(set(dominant_categories)),
            "annual_revenue": annual_revenue,
            "annual_profit": annual_profit,
            "employee_size": employee_size,
            "office_count": office_count,
            "rd_investment": rd_investment,
            "tam": tam,
            "sam": sam,
            "som": som,
            "valuation": valuation,
            "growth_rate_pct": growth_rate_pct or 0.0,
            "market_share_pct": market_share_pct or 0.0,
            "churn_rate_pct": churn_rate_pct or 0.0,
            "employee_turnover_pct": employee_turnover_pct or 0.0,
            "brand_sentiment_score_numeric": (brand_sentiment_numeric or 0.0),
            "net_promoter_score": net_promoter_score or 0.0,
            "customer_acquisition_cost": customer_acquisition_cost or 0.0,
            "customer_lifetime_value": customer_lifetime_value or 0.0,
            "retention_tenure_years": retention_tenure or 0.0,
            "ai_adoption_score": _keyword_score(consolidated_json.get("ai_ml_adoption_level"), _AI_SCORES, default=0.0),
            "hiring_velocity_score": _keyword_score(consolidated_json.get("hiring_velocity"), _LEVEL_SCORES, default=0.0),
            "benchmark_score": _keyword_score(consolidated_json.get("benchmark_vs_peers"), _LEVEL_SCORES, default=0.0),
            "future_projection_score": _keyword_score(consolidated_json.get("future_projections"), _FUTURE_SCORES, default=0.0),
            "profitability_score": max(
                _keyword_score(consolidated_json.get("profitability_status"), _PROFITABILITY_SCORES, default=0.0),
                _keyword_score(consolidated_json.get("profitability_status"), _LEVEL_SCORES, default=0.0),
            ),
            "global_exposure_score": _keyword_score(consolidated_json.get("global_exposure"), _LEVEL_SCORES, default=0.0),
            "partnership_strength_score": _keyword_score(consolidated_json.get("partnership_ecosystem"), _LEVEL_SCORES, default=0.0),
            "innovation_benchmark_score": _keyword_score(consolidated_json.get("industry_benchmark_tech_adoption"), _LEVEL_SCORES, default=0.0),
            "country_count": len(_split_multi(consolidated_json.get("countries_operating_in"))),
            "industry_count": len(industries),
            "product_count": len(products),
            "customer_segment_count": len(customer_segments),
            "technology_term_count": len(technology_terms),
            "innovation_term_count": len(innovation_terms),
            "annual_revenue_log": _safe_log(annual_revenue),
            "annual_profit_log": _safe_log(annual_profit),
            "employee_size_log": _safe_log(employee_size),
            "office_count_log": _safe_log(office_count),
            "rd_investment_log": _safe_log(rd_investment),
            "tam_log": _safe_log(tam),
            "sam_log": _safe_log(sam),
            "som_log": _safe_log(som),
            "valuation_log": _safe_log(valuation),
            "profit_margin_pct": profit_margin_pct or 0.0,
        }

    @staticmethod
    def _to_float(value: Any) -> float:
        if value is None:
            return float("nan")
        try:
            return float(value)
        except (TypeError, ValueError):
            return float("nan")
