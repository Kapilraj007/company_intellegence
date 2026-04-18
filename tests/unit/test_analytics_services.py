import numpy as np

from ml.analytics.descriptive_analytics import DescriptiveAnalyticsService
from ml.analytics.predictive_models import PredictiveAnalyticsService


class StubFeatureBuilder:
    def __init__(self, rows, matrix, feature_names):
        self._rows = list(rows)
        self._matrix = np.asarray(matrix, dtype="float32")
        self._feature_names = list(feature_names)
        self.calls = []

    def build_feature_rows(self, *, company_ids=None, company_names=None, limit=None, user_id):
        self.calls.append({"user_id": user_id, "company_ids": company_ids, "company_names": company_names})
        del company_ids, company_names
        rows = list(self._rows)
        return rows[:limit] if limit is not None else rows

    def build_model_matrix(self, rows, *, vector_components=6):
        del rows, vector_components
        return {
            "rows": list(self._rows),
            "matrix": self._matrix,
            "feature_names": list(self._feature_names),
        }


def _sample_rows():
    return [
        {
            "company_id": "openai",
            "company_name": "OpenAI",
            "sector": "Technology",
            "industries": ["Healthcare", "Technology"],
            "technology_terms": ["AI", "LLM", "GPU"],
            "innovation_terms": ["Agents", "Platform", "Inference"],
            "vector_source": "pinecone",
            "vector_count": 8,
            "annual_revenue_log": 7.8,
            "annual_profit_log": 6.3,
            "rd_investment_log": 7.4,
            "valuation_log": 8.2,
            "office_count_log": 3.4,
            "tam_log": 8.5,
            "sam_log": 7.6,
            "som_log": 6.8,
            "growth_rate_pct": 42.0,
            "market_share_pct": 18.0,
            "churn_rate_pct": 4.0,
            "net_promoter_score": 71.0,
            "ai_adoption_score": 0.98,
            "hiring_velocity_score": 0.94,
            "benchmark_score": 0.93,
            "future_projection_score": 0.9,
            "profitability_score": 0.82,
            "brand_sentiment_score_numeric": 88.0,
            "global_exposure_score": 0.76,
            "partnership_strength_score": 0.82,
            "technology_term_count": 8,
            "innovation_term_count": 7,
            "country_count": 12,
            "industry_count": 2,
            "product_count": 5,
            "customer_segment_count": 4,
            "vector": [1.0, 0.1, 0.0],
            "vector_strength": 1.0,
            "category_diversity": 3,
        },
        {
            "company_id": "tempus",
            "company_name": "Tempus",
            "sector": "Healthcare",
            "industries": ["Healthcare"],
            "technology_terms": ["AI", "Clinical Data", "Platform"],
            "innovation_terms": ["Precision Medicine", "Diagnostics"],
            "vector_source": "pinecone",
            "vector_count": 6,
            "annual_revenue_log": 6.7,
            "annual_profit_log": 4.9,
            "rd_investment_log": 6.5,
            "valuation_log": 6.6,
            "office_count_log": 2.8,
            "tam_log": 7.6,
            "sam_log": 6.4,
            "som_log": 5.8,
            "growth_rate_pct": 28.0,
            "market_share_pct": 9.0,
            "churn_rate_pct": 6.0,
            "net_promoter_score": 63.0,
            "ai_adoption_score": 0.92,
            "hiring_velocity_score": 0.82,
            "benchmark_score": 0.84,
            "future_projection_score": 0.84,
            "profitability_score": 0.64,
            "brand_sentiment_score_numeric": 79.0,
            "global_exposure_score": 0.55,
            "partnership_strength_score": 0.68,
            "technology_term_count": 6,
            "innovation_term_count": 5,
            "country_count": 5,
            "industry_count": 1,
            "product_count": 3,
            "customer_segment_count": 3,
            "vector": [0.88, 0.18, 0.0],
            "vector_strength": 1.0,
            "category_diversity": 3,
        },
        {
            "company_id": "stripe",
            "company_name": "Stripe",
            "sector": "FinTech",
            "industries": ["Finance"],
            "technology_terms": ["Payments", "APIs", "Fraud Detection"],
            "innovation_terms": ["Platform", "Expansion"],
            "vector_source": "pinecone",
            "vector_count": 7,
            "annual_revenue_log": 7.2,
            "annual_profit_log": 5.2,
            "rd_investment_log": 5.9,
            "valuation_log": 7.4,
            "office_count_log": 3.1,
            "tam_log": 8.0,
            "sam_log": 7.1,
            "som_log": 6.2,
            "growth_rate_pct": 24.0,
            "market_share_pct": 16.0,
            "churn_rate_pct": 5.0,
            "net_promoter_score": 60.0,
            "ai_adoption_score": 0.72,
            "hiring_velocity_score": 0.78,
            "benchmark_score": 0.76,
            "future_projection_score": 0.82,
            "profitability_score": 0.74,
            "brand_sentiment_score_numeric": 75.0,
            "global_exposure_score": 0.72,
            "partnership_strength_score": 0.8,
            "technology_term_count": 5,
            "innovation_term_count": 4,
            "country_count": 9,
            "industry_count": 1,
            "product_count": 4,
            "customer_segment_count": 4,
            "vector": [0.1, 1.0, 0.0],
            "vector_strength": 1.0,
            "category_diversity": 3,
        },
        {
            "company_id": "paypal",
            "company_name": "PayPal",
            "sector": "FinTech",
            "industries": ["Finance"],
            "technology_terms": ["Payments", "Checkout", "Fraud Detection"],
            "innovation_terms": ["Merchant Tools", "Expansion"],
            "vector_source": "pinecone",
            "vector_count": 7,
            "annual_revenue_log": 7.0,
            "annual_profit_log": 5.5,
            "rd_investment_log": 5.6,
            "valuation_log": 7.0,
            "office_count_log": 3.0,
            "tam_log": 7.8,
            "sam_log": 7.0,
            "som_log": 6.0,
            "growth_rate_pct": 18.0,
            "market_share_pct": 13.0,
            "churn_rate_pct": 5.0,
            "net_promoter_score": 56.0,
            "ai_adoption_score": 0.66,
            "hiring_velocity_score": 0.7,
            "benchmark_score": 0.7,
            "future_projection_score": 0.74,
            "profitability_score": 0.76,
            "brand_sentiment_score_numeric": 73.0,
            "global_exposure_score": 0.7,
            "partnership_strength_score": 0.76,
            "technology_term_count": 5,
            "innovation_term_count": 4,
            "country_count": 8,
            "industry_count": 1,
            "product_count": 4,
            "customer_segment_count": 4,
            "vector": [0.15, 0.92, 0.0],
            "vector_strength": 1.0,
            "category_diversity": 3,
        },
        {
            "company_id": "siemens",
            "company_name": "Siemens",
            "sector": "Industrial",
            "industries": ["Manufacturing"],
            "technology_terms": ["Automation", "IoT", "AI"],
            "innovation_terms": ["Digital Twin", "Smart Factory"],
            "vector_source": "pinecone",
            "vector_count": 6,
            "annual_revenue_log": 7.6,
            "annual_profit_log": 5.8,
            "rd_investment_log": 6.2,
            "valuation_log": 7.3,
            "office_count_log": 3.7,
            "tam_log": 8.1,
            "sam_log": 7.2,
            "som_log": 6.4,
            "growth_rate_pct": 16.0,
            "market_share_pct": 11.0,
            "churn_rate_pct": 7.0,
            "net_promoter_score": 54.0,
            "ai_adoption_score": 0.48,
            "hiring_velocity_score": 0.62,
            "benchmark_score": 0.7,
            "future_projection_score": 0.72,
            "profitability_score": 0.82,
            "brand_sentiment_score_numeric": 70.0,
            "global_exposure_score": 0.78,
            "partnership_strength_score": 0.7,
            "technology_term_count": 4,
            "innovation_term_count": 4,
            "country_count": 11,
            "industry_count": 1,
            "product_count": 4,
            "customer_segment_count": 3,
            "vector": [0.0, 0.15, 1.0],
            "vector_strength": 1.0,
            "category_diversity": 3,
        },
        {
            "company_id": "service_now",
            "company_name": "ServiceNow",
            "sector": "Software",
            "industries": ["Healthcare", "Finance"],
            "technology_terms": ["AI", "Automation", "Workflow"],
            "innovation_terms": ["Agents", "Platform", "Enterprise"],
            "vector_source": "pinecone",
            "vector_count": 6,
            "annual_revenue_log": 6.9,
            "annual_profit_log": 5.0,
            "rd_investment_log": 6.0,
            "valuation_log": 7.1,
            "office_count_log": 3.0,
            "tam_log": 7.7,
            "sam_log": 6.9,
            "som_log": 6.1,
            "growth_rate_pct": 20.0,
            "market_share_pct": 10.0,
            "churn_rate_pct": 4.0,
            "net_promoter_score": 58.0,
            "ai_adoption_score": 0.8,
            "hiring_velocity_score": 0.74,
            "benchmark_score": 0.78,
            "future_projection_score": 0.8,
            "profitability_score": 0.7,
            "brand_sentiment_score_numeric": 77.0,
            "global_exposure_score": 0.68,
            "partnership_strength_score": 0.74,
            "technology_term_count": 6,
            "innovation_term_count": 5,
            "country_count": 7,
            "industry_count": 2,
            "product_count": 4,
            "customer_segment_count": 4,
            "vector": [0.76, 0.2, 0.12],
            "vector_strength": 1.0,
            "category_diversity": 3,
        },
    ]


def test_descriptive_analytics_ranks_ai_industries_and_growth_sectors():
    rows = _sample_rows()
    matrix = [[float(index + 1), row["growth_rate_pct"], row["ai_adoption_score"]] for index, row in enumerate(rows)]
    builder = StubFeatureBuilder(rows, matrix, ["feature_a", "feature_b", "feature_c"])

    result = DescriptiveAnalyticsService(feature_builder=builder).run(top_n=3, user_id="user-123")

    assert result["company_count"] == 6
    assert builder.calls[0]["user_id"] == "user-123"
    assert result["top_ai_adopting_industries"][0]["industry"] == "Healthcare"
    assert result["most_innovative_industries"][0]["industry"] == "Healthcare"
    assert result["top_growing_sectors"][0]["sector"] in {"Technology", "Healthcare"}
    assert result["technology_adoption_patterns"]["top_technologies"][0]["technology"] == "AI"


def test_predictive_analytics_returns_scores_and_uses_models():
    rows = _sample_rows()
    matrix = [
        [
            row["growth_rate_pct"],
            row["ai_adoption_score"],
            row["rd_investment_log"],
            row["annual_revenue_log"],
            row["country_count"],
            row["technology_term_count"],
        ]
        for row in rows
    ]
    builder = StubFeatureBuilder(
        rows,
        matrix,
        ["growth_rate_pct", "ai_adoption_score", "rd_investment_log", "annual_revenue_log", "country_count", "technology_term_count"],
    )

    result = PredictiveAnalyticsService(feature_builder=builder).run(
        top_n=3,
        min_training_samples=4,
        user_id="user-123",
    )

    assert result["company_count"] == 6
    assert builder.calls[0]["user_id"] == "user-123"
    assert result["models"]["growth"]["training_mode"] == "proxy_ensemble"
    assert "random_forest" in result["models"]["growth"]["models_used"]
    assert result["high_growth_startups"][0]["company_id"] == "openai"
    assert result["future_technology_leaders"][0]["company_id"] == "openai"
    assert result["market_expansion_candidates"][0]["company_id"] in {"openai", "siemens"}
