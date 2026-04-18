from __future__ import annotations

from typing import Any, Dict, Iterable, List


def _normalize_company_key(match: Dict[str, Any]) -> str:
    company_id = str(match.get("company_id") or "").strip()
    if company_id:
        return company_id
    company_name = str(match.get("company_name") or "").strip().lower()
    return company_name


def _rank_score(chunks: List[Dict[str, Any]]) -> float:
    if not chunks:
        return 0.0
    ordered = sorted(chunks, key=lambda row: row["score"], reverse=True)
    best_score = float(ordered[0]["score"])
    consistency_window = ordered[:3]
    consistency = sum(float(row["score"]) for row in consistency_window) / len(consistency_window)
    support_window = ordered[:2]
    support = sum(float(row["score"]) for row in support_window) / len(support_window)
    unique_sections = len(
        {
            str(row.get("chunk_title") or "").strip()
            for row in ordered
            if str(row.get("chunk_title") or "").strip()
        }
    )
    evidence_bonus = min(0.12, 0.04 * max(0, unique_sections - 1))
    return round(
        min(1.0, (0.60 * best_score) + (0.25 * consistency) + (0.10 * support) + evidence_bonus),
        4,
    )


def _similarity_score(chunks: List[Dict[str, Any]], source_category_count: int) -> float:
    if not chunks:
        return 0.0
    ordered = sorted(chunks, key=lambda row: row["score"], reverse=True)
    best_score = float(ordered[0]["score"])
    average_score = sum(float(row["score"]) for row in ordered[:5]) / min(len(ordered), 5)
    category_count = len({str(row.get("chunk_title") or "") for row in ordered if str(row.get("chunk_title") or "")})
    coverage_ratio = min(1.0, category_count / max(1, int(source_category_count or 1)))
    return round(min(1.0, (0.55 * best_score) + (0.25 * average_score) + (0.20 * coverage_ratio)), 4)


def rank_company_matches(
    matches: Iterable[Dict[str, Any]],
    *,
    top_k: int = 5,
    exclude_company: str = "",
    max_chunks_per_company: int = 5,
) -> List[Dict[str, Any]]:
    exclude = exclude_company.strip().lower()
    buckets: Dict[str, Dict[str, Any]] = {}

    for match in matches:
        if not isinstance(match, dict):
            continue

        company_name = str(match.get("company_name") or "").strip()
        if exclude and company_name.lower() == exclude:
            continue

        company_key = _normalize_company_key(match)
        if not company_key:
            continue

        score = round(float(match.get("score") or 0.0), 4)
        chunk = {
            "chunk_id": match.get("id"),
            "chunk_title": match.get("chunk_title") or match.get("category"),
            "score": score,
            "vector_score": match.get("vector_score"),
            "rerank_score": match.get("rerank_score"),
            "lexical_score": match.get("lexical_score"),
            "overlap_terms": list(match.get("overlap_terms") or []),
            "snippet": str(match.get("snippet") or match.get("chunk_text") or ""),
        }

        bucket = buckets.setdefault(
            company_key,
            {
                "company_id": str(match.get("company_id") or ""),
                "company_name": company_name,
                "chunks": [],
            },
        )
        bucket["chunks"].append(chunk)

    ranked: List[Dict[str, Any]] = []
    for bucket in buckets.values():
        chunks = sorted(bucket["chunks"], key=lambda row: row["score"], reverse=True)
        if not chunks:
            continue

        ranked.append(
            {
                "company_id": bucket["company_id"],
                "company_name": bucket["company_name"],
                "score": _rank_score(chunks),
                "max_score": chunks[0]["score"],
                "match_count": len(chunks),
                "top_chunks": chunks[: max(1, int(max_chunks_per_company))],
                "category": chunks[0]["chunk_title"],
                "snippet": chunks[0]["snippet"],
            }
        )

    ranked.sort(
        key=lambda row: (
            float(row.get("score") or 0.0),
            float(row.get("max_score") or 0.0),
            int(row.get("match_count") or 0),
            str(row.get("company_name") or "").lower(),
        ),
        reverse=True,
    )
    return ranked[: max(1, int(top_k))]


def rank_similar_companies(
    matches: Iterable[Dict[str, Any]],
    *,
    source_company_id: str,
    source_category_count: int,
    top_k: int = 5,
    exclude_company: str = "",
    max_chunks_per_company: int = 5,
) -> List[Dict[str, Any]]:
    exclude = exclude_company.strip().lower()
    buckets: Dict[str, Dict[str, Any]] = {}

    for match in matches:
        if not isinstance(match, dict):
            continue

        company_id = str(match.get("company_id") or "").strip()
        company_name = str(match.get("company_name") or "").strip()
        if not company_id or company_id == source_company_id:
            continue
        if exclude and company_name.lower() == exclude:
            continue

        score = round(float(match.get("score") or 0.0), 4)
        chunk = {
            "chunk_id": match.get("id"),
            "chunk_title": match.get("category"),
            "source_category": match.get("source_category"),
            "score": score,
            "overlap_terms": [],
            "snippet": str(match.get("snippet") or match.get("chunk_text") or ""),
        }

        bucket = buckets.setdefault(
            company_id,
            {
                "company_id": company_id,
                "company_name": company_name,
                "chunks": [],
            },
        )
        bucket["chunks"].append(chunk)

    ranked: List[Dict[str, Any]] = []
    for bucket in buckets.values():
        chunks = sorted(bucket["chunks"], key=lambda row: row["score"], reverse=True)
        if not chunks:
            continue

        shared_categories = [
            category
            for category in dict.fromkeys(
                str(chunk.get("chunk_title") or "").strip()
                for chunk in chunks
                if str(chunk.get("chunk_title") or "").strip()
            )
        ]

        ranked.append(
            {
                "company_id": bucket["company_id"],
                "company_name": bucket["company_name"],
                "score": _similarity_score(chunks, source_category_count),
                "max_score": chunks[0]["score"],
                "match_count": len(shared_categories) or len(chunks),
                "top_chunks": chunks[: max(1, int(max_chunks_per_company))],
                "category": chunks[0]["chunk_title"],
                "snippet": chunks[0]["snippet"],
                "shared_categories": shared_categories,
            }
        )

    ranked.sort(
        key=lambda row: (
            float(row.get("score") or 0.0),
            int(row.get("match_count") or 0),
            float(row.get("max_score") or 0.0),
            str(row.get("company_name") or "").lower(),
        ),
        reverse=True,
    )
    return ranked[: max(1, int(top_k))]
