from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Sequence

from core.user_scope import require_user_id
from ml.feature_builder import CompanyFeatureBuilder


def _normalize_metric(rows: Sequence[Dict[str, Any]], key: str) -> Dict[str, float]:
    values = [float(row.get(key) or 0.0) for row in rows]
    if not values:
        return {}
    minimum = min(values)
    maximum = max(values)
    if maximum <= minimum:
        return {str(row.get("company_id") or ""): 0.5 for row in rows}
    return {
        str(row.get("company_id") or ""): round((float(row.get(key) or 0.0) - minimum) / (maximum - minimum), 4)
        for row in rows
    }


class DescriptiveAnalyticsService:
    def __init__(self, *, feature_builder: CompanyFeatureBuilder | None = None) -> None:
        self._feature_builder = feature_builder or CompanyFeatureBuilder()

    def run(
        self,
        *,
        company_ids: Sequence[str] | None = None,
        company_names: Sequence[str] | None = None,
        limit: int | None = None,
        top_n: int = 5,
        user_id: str,
    ) -> Dict[str, Any]:
        user_id = require_user_id(user_id, context="descriptive analytics")
        rows = self._feature_builder.build_feature_rows(
            company_ids=company_ids,
            company_names=company_names,
            limit=limit,
            user_id=user_id,
        )
        if not rows:
            raise ValueError("No company analytics data is available.")

        enriched = self._enrich_rows(rows)
        return {
            "company_count": len(enriched),
            "top_n": max(1, int(top_n)),
            "vector_source_breakdown": dict(Counter(row.get("vector_source") or "unknown" for row in enriched)),
            "top_ai_adopting_industries": self._rank_industries(
                enriched,
                metric_key="ai_readiness_score",
                top_n=top_n,
                minimum_threshold=0.45,
            ),
            "most_innovative_industries": self._rank_industries(
                enriched,
                metric_key="innovation_intensity_score",
                top_n=top_n,
            ),
            "top_growing_sectors": self._rank_sectors(
                enriched,
                metric_key="growth_momentum_score",
                top_n=top_n,
            ),
            "technology_adoption_patterns": self._technology_patterns(
                enriched,
                top_n=top_n,
            ),
            "innovation_leaders": self._top_companies(
                enriched,
                metric_key="innovation_intensity_score",
                label="innovation_score",
                top_n=top_n,
            ),
            "growth_leaders": self._top_companies(
                enriched,
                metric_key="growth_momentum_score",
                label="growth_score",
                top_n=top_n,
            ),
        }

    def _enrich_rows(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        growth_norm = _normalize_metric(rows, "growth_rate_pct")
        revenue_norm = _normalize_metric(rows, "annual_revenue_log")
        rd_norm = _normalize_metric(rows, "rd_investment_log")
        valuation_norm = _normalize_metric(rows, "valuation_log")
        tech_term_norm = _normalize_metric(rows, "technology_term_count")
        innovation_term_norm = _normalize_metric(rows, "innovation_term_count")
        expansion_norm = _normalize_metric(rows, "country_count")

        enriched = []
        for row in rows:
            company_id = str(row.get("company_id") or "")
            ai_readiness_score = self._weighted_average(
                [
                    (float(row.get("ai_adoption_score") or 0.0), 0.55),
                    (float(row.get("benchmark_score") or 0.0), 0.2),
                    (tech_term_norm.get(company_id, 0.0), 0.15),
                    (rd_norm.get(company_id, 0.0), 0.1),
                ]
            )
            innovation_intensity_score = self._weighted_average(
                [
                    (float(row.get("ai_adoption_score") or 0.0), 0.18),
                    (rd_norm.get(company_id, 0.0), 0.18),
                    (tech_term_norm.get(company_id, 0.0), 0.14),
                    (innovation_term_norm.get(company_id, 0.0), 0.14),
                    (float(row.get("benchmark_score") or 0.0), 0.12),
                    (float(row.get("future_projection_score") or 0.0), 0.1),
                    (valuation_norm.get(company_id, 0.0), 0.08),
                    (revenue_norm.get(company_id, 0.0), 0.06),
                ]
            )
            growth_momentum_score = self._weighted_average(
                [
                    (growth_norm.get(company_id, 0.0), 0.3),
                    (float(row.get("hiring_velocity_score") or 0.0), 0.18),
                    (float(row.get("profitability_score") or 0.0), 0.15),
                    (float(row.get("future_projection_score") or 0.0), 0.14),
                    (innovation_intensity_score, 0.13),
                    (expansion_norm.get(company_id, 0.0), 0.1),
                ]
            )
            enriched.append(
                {
                    **row,
                    "ai_readiness_score": ai_readiness_score,
                    "innovation_intensity_score": innovation_intensity_score,
                    "growth_momentum_score": growth_momentum_score,
                }
            )
        return enriched

    @staticmethod
    def _weighted_average(pairs: Iterable[tuple[float, float]]) -> float:
        numerator = 0.0
        denominator = 0.0
        for value, weight in pairs:
            numerator += float(value) * float(weight)
            denominator += float(weight)
        if denominator <= 0:
            return 0.0
        return round(numerator / denominator, 4)

    def _rank_industries(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        metric_key: str,
        top_n: int,
        minimum_threshold: float = 0.0,
    ) -> List[Dict[str, Any]]:
        bucket: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"score_total": 0.0, "company_ids": set(), "companies": []})
        for row in rows:
            score = float(row.get(metric_key) or 0.0)
            if score < minimum_threshold:
                continue
            for industry in row.get("industries") or []:
                entry = bucket[industry]
                entry["score_total"] += score
                entry["company_ids"].add(row["company_id"])
                entry["companies"].append((row["company_name"], score))

        ranked = []
        for industry, payload in bucket.items():
            company_count = len(payload["company_ids"])
            if company_count == 0:
                continue
            companies = sorted(payload["companies"], key=lambda item: item[1], reverse=True)
            average_score = payload["score_total"] / company_count
            # Industry analytics should reward repeated strength across multiple companies,
            # not just a single exceptional outlier.
            support_adjusted_score = average_score * (1.0 + min(max(company_count - 1, 0), 4) * 0.25)
            ranked.append(
                {
                    "industry": industry,
                    "score": round(support_adjusted_score, 4),
                    "average_score": round(average_score, 4),
                    "company_count": company_count,
                    "top_companies": [name for name, _ in companies[:3]],
                }
            )

        ranked.sort(key=lambda item: (item["score"], item["company_count"], item["industry"]), reverse=True)
        return ranked[: max(1, int(top_n))]

    def _rank_sectors(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        metric_key: str,
        top_n: int,
    ) -> List[Dict[str, Any]]:
        bucket: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"score_total": 0.0, "companies": []})
        for row in rows:
            sector = str(row.get("sector") or "Unknown").strip()
            score = float(row.get(metric_key) or 0.0)
            bucket[sector]["score_total"] += score
            bucket[sector]["companies"].append((row["company_name"], score))

        ranked = []
        for sector, payload in bucket.items():
            companies = payload["companies"]
            ranked.append(
                {
                    "sector": sector,
                    "score": round(payload["score_total"] / max(len(companies), 1), 4),
                    "company_count": len(companies),
                    "top_companies": [name for name, _ in sorted(companies, key=lambda item: item[1], reverse=True)[:3]],
                }
            )

        ranked.sort(key=lambda item: (item["score"], item["company_count"], item["sector"]), reverse=True)
        return ranked[: max(1, int(top_n))]

    def _technology_patterns(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        top_n: int,
    ) -> Dict[str, Any]:
        technology_scores: Dict[str, float] = defaultdict(float)
        technology_companies: Dict[str, set[str]] = defaultdict(set)
        industry_patterns: Dict[str, Counter[str]] = defaultdict(Counter)

        for row in rows:
            weight = 0.5 * float(row.get("ai_readiness_score") or 0.0) + 0.5 * float(row.get("innovation_intensity_score") or 0.0)
            for term in row.get("technology_terms") or []:
                technology_scores[term] += weight
                technology_companies[term].add(row["company_id"])
                for industry in row.get("industries") or []:
                    industry_patterns[industry][term] += 1

        top_technologies = [
            {
                "technology": term,
                "score": round(score, 4),
                "company_count": len(technology_companies[term]),
            }
            for term, score in sorted(technology_scores.items(), key=lambda item: (item[1], item[0]), reverse=True)[: max(1, int(top_n))]
        ]

        top_industry_patterns = []
        for industry, counter in industry_patterns.items():
            if not counter:
                continue
            top_industry_patterns.append(
                {
                    "industry": industry,
                    "top_technologies": [term for term, _ in counter.most_common(3)],
                }
            )
        top_industry_patterns.sort(key=lambda item: item["industry"])

        adoption_leaders = self._top_companies(
            rows,
            metric_key="ai_readiness_score",
            label="adoption_score",
            top_n=top_n,
        )

        return {
            "top_technologies": top_technologies,
            "industry_patterns": top_industry_patterns[: max(1, int(top_n))],
            "adoption_leaders": adoption_leaders,
        }

    @staticmethod
    def _top_companies(
        rows: Sequence[Dict[str, Any]],
        *,
        metric_key: str,
        label: str,
        top_n: int,
    ) -> List[Dict[str, Any]]:
        ranked = sorted(
            rows,
            key=lambda row: (
                float(row.get(metric_key) or 0.0),
                float(row.get("annual_revenue_log") or 0.0),
                str(row.get("company_name") or "").lower(),
            ),
            reverse=True,
        )
        return [
            {
                "company_id": row["company_id"],
                "company_name": row["company_name"],
                label: round(float(row.get(metric_key) or 0.0), 4),
                "sector": row.get("sector"),
                "industries": list(row.get("industries") or [])[:3],
            }
            for row in ranked[: max(1, int(top_n))]
        ]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run descriptive analytics on company vectors and metadata.")
    parser.add_argument("--company-id", action="append", dest="company_ids", default=[])
    parser.add_argument("--company-name", action="append", dest="company_names", default=[])
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--user-id", default="", help="Authenticated user id for scoped analytics")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = DescriptiveAnalyticsService().run(
        company_ids=args.company_ids,
        company_names=args.company_names,
        limit=args.limit,
        top_n=args.top_n,
        user_id=args.user_id,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
