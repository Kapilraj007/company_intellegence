from __future__ import annotations

import math
import os
import re
from typing import Any, Dict, Iterable, List

from logger import get_logger

logger = get_logger("search_reranker")

RERANKER_MODEL_NAME = os.getenv(
    "SEARCH_RERANKER_MODEL",
    "cross-encoder/ms-marco-MiniLM-L-6-v2",
)
DEFAULT_RERANK_LIMIT = int(os.getenv("SEARCH_RERANK_LIMIT", "60"))

_MODEL = None


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-zA-Z0-9]+", str(text or "").lower())
        if len(token) >= 3
    }


def _sigmoid(value: float) -> float:
    if value >= 0:
        exp_value = math.exp(-value)
        return 1.0 / (1.0 + exp_value)
    exp_value = math.exp(value)
    return exp_value / (1.0 + exp_value)


class OpenSourceReranker:
    def __init__(self, model_name: str = RERANKER_MODEL_NAME, limit: int = DEFAULT_RERANK_LIMIT) -> None:
        self.model_name = model_name
        self.limit = max(1, int(limit))

    def _get_model(self):
        global _MODEL
        if _MODEL is None:
            from sentence_transformers import CrossEncoder

            logger.info(f"[Search] Loading reranker model '{self.model_name}'...")
            _MODEL = CrossEncoder(self.model_name)
        return _MODEL

    def rerank(self, *, query: str, matches: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        candidates = [dict(match) for match in matches if isinstance(match, dict)]
        if not candidates:
            return []

        candidates.sort(key=lambda row: float(row.get("score") or 0.0), reverse=True)
        rerank_candidates = candidates[: self.limit]
        untouched_candidates = candidates[self.limit :]

        query_tokens = _tokenize(query)
        pairs = []
        for row in rerank_candidates:
            text = str(row.get("chunk_text") or row.get("snippet") or "")
            pairs.append((query, text))

        reranker_available = True
        try:
            logits = self._get_model().predict(pairs)
        except Exception as exc:
            logger.warning(f"Reranker unavailable, using vector score only: {exc}")
            logits = [0.0] * len(rerank_candidates)
            reranker_available = False

        reranked = []
        for row, logit in zip(rerank_candidates, logits):
            text = str(row.get("chunk_text") or row.get("snippet") or "")
            text_tokens = _tokenize(text)
            overlap = query_tokens.intersection(text_tokens)
            lexical_score = len(overlap) / max(len(query_tokens), 1) if query_tokens else 0.0
            vector_score = float(row.get("score") or 0.0)
            rerank_score = _sigmoid(float(logit)) if reranker_available else None
            if reranker_available:
                combined_score = round(
                    min(1.0, (0.55 * float(rerank_score)) + (0.35 * vector_score) + (0.10 * lexical_score)),
                    4,
                )
            else:
                combined_score = round(min(1.0, (0.85 * vector_score) + (0.15 * lexical_score)), 4)

            reranked.append(
                {
                    **row,
                    "vector_score": round(vector_score, 4),
                    "rerank_score": None if rerank_score is None else round(float(rerank_score), 4),
                    "lexical_score": round(lexical_score, 4),
                    "overlap_terms": sorted(overlap),
                    "score": combined_score,
                }
            )

        reranked.sort(key=lambda row: float(row.get("score") or 0.0), reverse=True)
        untouched_candidates = [
            {
                **row,
                "vector_score": round(float(row.get("score") or 0.0), 4),
                "rerank_score": None,
                "lexical_score": 0.0,
                "overlap_terms": [],
            }
            for row in untouched_candidates
        ]
        return reranked + untouched_candidates
