from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Iterable, List, Sequence

import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from core.user_scope import require_user_id
from ml.feature_builder import CompanyFeatureBuilder


def _normalize(values: Sequence[float]) -> np.ndarray:
    array = np.asarray(list(values), dtype="float32")
    if array.size == 0:
        return array
    minimum = float(np.min(array))
    maximum = float(np.max(array))
    if maximum <= minimum:
        return np.full_like(array, 0.5, dtype="float32")
    return (array - minimum) / (maximum - minimum)


class PredictiveAnalyticsService:
    def __init__(self, *, feature_builder: CompanyFeatureBuilder | None = None) -> None:
        self._feature_builder = feature_builder or CompanyFeatureBuilder()

    def run(
        self,
        *,
        company_ids: Sequence[str] | None = None,
        company_names: Sequence[str] | None = None,
        limit: int | None = None,
        top_n: int = 5,
        min_training_samples: int = 6,
        user_id: str,
    ) -> Dict[str, Any]:
        user_id = require_user_id(user_id, context="predictive analytics")
        rows = self._feature_builder.build_feature_rows(
            company_ids=company_ids,
            company_names=company_names,
            limit=limit,
            user_id=user_id,
        )
        if not rows:
            raise ValueError("No company analytics data is available.")

        payload = self._feature_builder.build_model_matrix(rows)
        matrix = np.asarray(payload["matrix"], dtype="float32")
        feature_names = list(payload["feature_names"])

        growth_target = self._growth_target(rows)
        leadership_target = self._leadership_target(rows)
        expansion_target = self._expansion_target(rows)

        growth_predictions, growth_meta = self._fit_target_model(
            matrix,
            feature_names,
            growth_target,
            min_training_samples=min_training_samples,
            seed=42,
        )
        leadership_predictions, leadership_meta = self._fit_target_model(
            matrix,
            feature_names,
            leadership_target,
            min_training_samples=min_training_samples,
            seed=43,
        )
        expansion_predictions, expansion_meta = self._fit_target_model(
            matrix,
            feature_names,
            expansion_target,
            min_training_samples=min_training_samples,
            seed=44,
        )

        predictions = []
        for index, row in enumerate(rows):
            composite = float(
                np.mean(
                    [
                        growth_predictions[index],
                        leadership_predictions[index],
                        expansion_predictions[index],
                    ]
                )
            )
            predictions.append(
                {
                    "company_id": row["company_id"],
                    "company_name": row["company_name"],
                    "sector": row.get("sector"),
                    "industries": list(row.get("industries") or [])[:3],
                    "growth_prediction_score": round(float(growth_predictions[index]) * 100.0, 1),
                    "technology_leadership_score": round(float(leadership_predictions[index]) * 100.0, 1),
                    "market_expansion_potential": round(float(expansion_predictions[index]) * 100.0, 1),
                    "composite_prediction_score": round(float(composite) * 100.0, 1),
                    "model_input_summary": {
                        "vector_source": row.get("vector_source"),
                        "vector_count": row.get("vector_count"),
                        "annual_revenue_log": round(float(row.get("annual_revenue_log") or 0.0), 4),
                        "growth_rate_pct": round(float(row.get("growth_rate_pct") or 0.0), 4),
                        "ai_adoption_score": round(float(row.get("ai_adoption_score") or 0.0), 4),
                    },
                }
            )

        predictions.sort(
            key=lambda item: (
                float(item["composite_prediction_score"]),
                float(item["growth_prediction_score"]),
                item["company_name"].lower(),
            ),
            reverse=True,
        )

        return {
            "company_count": len(rows),
            "training_sample_count": len(rows),
            "models": {
                "growth": growth_meta,
                "technology_leadership": leadership_meta,
                "market_expansion": expansion_meta,
            },
            "predictions": predictions,
            "high_growth_startups": predictions[: max(1, int(top_n))],
            "future_technology_leaders": sorted(predictions, key=lambda item: item["technology_leadership_score"], reverse=True)[: max(1, int(top_n))],
            "market_expansion_candidates": sorted(predictions, key=lambda item: item["market_expansion_potential"], reverse=True)[: max(1, int(top_n))],
        }

    def _growth_target(self, rows: Sequence[Dict[str, Any]]) -> np.ndarray:
        growth = _normalize([float(row.get("growth_rate_pct") or 0.0) for row in rows])
        hiring = np.asarray([float(row.get("hiring_velocity_score") or 0.0) for row in rows], dtype="float32")
        profitability = np.asarray([float(row.get("profitability_score") or 0.0) for row in rows], dtype="float32")
        future = np.asarray([float(row.get("future_projection_score") or 0.0) for row in rows], dtype="float32")
        revenue = _normalize([float(row.get("annual_revenue_log") or 0.0) for row in rows])
        ai_signal = np.asarray([float(row.get("ai_adoption_score") or 0.0) for row in rows], dtype="float32")
        return (
            0.3 * growth
            + 0.18 * hiring
            + 0.16 * profitability
            + 0.14 * future
            + 0.12 * revenue
            + 0.10 * ai_signal
        )

    def _leadership_target(self, rows: Sequence[Dict[str, Any]]) -> np.ndarray:
        ai = np.asarray([float(row.get("ai_adoption_score") or 0.0) for row in rows], dtype="float32")
        rd = _normalize([float(row.get("rd_investment_log") or 0.0) for row in rows])
        benchmark = np.asarray([float(row.get("benchmark_score") or 0.0) for row in rows], dtype="float32")
        tech_breadth = _normalize([float(row.get("technology_term_count") or 0.0) for row in rows])
        innovation_breadth = _normalize([float(row.get("innovation_term_count") or 0.0) for row in rows])
        valuation = _normalize([float(row.get("valuation_log") or 0.0) for row in rows])
        return (
            0.25 * ai
            + 0.2 * rd
            + 0.18 * benchmark
            + 0.15 * tech_breadth
            + 0.12 * innovation_breadth
            + 0.10 * valuation
        )

    def _expansion_target(self, rows: Sequence[Dict[str, Any]]) -> np.ndarray:
        country_count = _normalize([float(row.get("country_count") or 0.0) for row in rows])
        office_count = _normalize([float(row.get("office_count_log") or 0.0) for row in rows])
        tam = _normalize([float(row.get("tam_log") or 0.0) for row in rows])
        sam = _normalize([float(row.get("sam_log") or 0.0) for row in rows])
        som = _normalize([float(row.get("som_log") or 0.0) for row in rows])
        partnership = np.asarray([float(row.get("partnership_strength_score") or 0.0) for row in rows], dtype="float32")
        global_exposure = np.asarray([float(row.get("global_exposure_score") or 0.0) for row in rows], dtype="float32")
        customer_diversity = _normalize([float(row.get("customer_segment_count") or 0.0) for row in rows])
        return (
            0.18 * country_count
            + 0.15 * office_count
            + 0.16 * tam
            + 0.12 * sam
            + 0.1 * som
            + 0.12 * partnership
            + 0.1 * global_exposure
            + 0.07 * customer_diversity
        )

    def _fit_target_model(
        self,
        matrix: np.ndarray,
        feature_names: Sequence[str],
        target: np.ndarray,
        *,
        min_training_samples: int,
        seed: int,
    ) -> tuple[np.ndarray, Dict[str, Any]]:
        proxy_target = _normalize(target)
        sample_count = int(matrix.shape[0]) if matrix.ndim == 2 else 0
        models_used: List[str] = ["proxy_target"]

        if sample_count < max(2, int(min_training_samples)):
            return proxy_target, {
                "training_mode": "proxy_only",
                "models_used": models_used,
                "top_features": [],
            }

        combined_predictions: List[np.ndarray] = [proxy_target]
        top_features = []

        try:
            forest = RandomForestRegressor(
                n_estimators=200,
                random_state=seed,
                min_samples_leaf=1,
            )
            forest.fit(matrix, proxy_target)
            forest_predictions = _normalize(forest.predict(matrix))
            combined_predictions.append(forest_predictions)
            models_used.append("random_forest")

            importances = getattr(forest, "feature_importances_", None)
            if importances is not None:
                ranked = sorted(
                    zip(feature_names, list(importances)),
                    key=lambda item: item[1],
                    reverse=True,
                )
                top_features = [
                    {"feature": name, "importance": round(float(score), 4)}
                    for name, score in ranked[:6]
                    if float(score) > 0
                ]
        except Exception:
            pass

        if sample_count >= max(10, int(min_training_samples) + 2):
            try:
                network = Pipeline(
                    steps=[
                        ("scaler", StandardScaler()),
                        (
                            "mlp",
                            MLPRegressor(
                                hidden_layer_sizes=(32, 16),
                                max_iter=800,
                                random_state=seed,
                            ),
                        ),
                    ]
                )
                network.fit(matrix, proxy_target)
                network_predictions = _normalize(network.predict(matrix))
                combined_predictions.append(network_predictions)
                models_used.append("neural_network")
            except Exception:
                pass

        final_predictions = np.mean(np.vstack(combined_predictions), axis=0)
        return final_predictions.astype("float32"), {
            "training_mode": "proxy_ensemble" if len(models_used) > 1 else "proxy_only",
            "models_used": models_used,
            "top_features": top_features,
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run predictive analytics on company vectors and metadata.")
    parser.add_argument("--company-id", action="append", dest="company_ids", default=[])
    parser.add_argument("--company-name", action="append", dest="company_names", default=[])
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--min-training-samples", type=int, default=6)
    parser.add_argument("--user-id", default="", help="Authenticated user id for scoped analytics")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = PredictiveAnalyticsService().run(
        company_ids=args.company_ids,
        company_names=args.company_names,
        limit=args.limit,
        top_n=args.top_n,
        min_training_samples=args.min_training_samples,
        user_id=args.user_id,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
