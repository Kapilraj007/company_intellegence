"""Analytics services built on top of company vectors and metadata."""

from ml.analytics.descriptive_analytics import DescriptiveAnalyticsService
from ml.analytics.predictive_models import PredictiveAnalyticsService

__all__ = [
    "DescriptiveAnalyticsService",
    "PredictiveAnalyticsService",
]
