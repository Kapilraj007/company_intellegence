from __future__ import annotations

import re
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from core.chunking import EXPECTED_FIELD_COUNT, generate_semantic_chunks
from core.local_store import get_local_store_client
from core.user_scope import require_user_id
from logger import get_logger
from search.company_similarity import rank_company_matches, rank_similar_companies
from search.reranker import OpenSourceReranker
from search.semantic_search import PineconeSemanticSearch

logger = get_logger("search_service")
_EMPTY_VALUES = {"not found", "n/a", "na", "unknown", "none", "null", "", "-"}
MIN_SEMANTIC_CHUNK_SCORE = float(os.getenv("SEARCH_MIN_CHUNK_SCORE", "0.5"))
MIN_SEMANTIC_COMPANY_SCORE = float(os.getenv("SEARCH_MIN_COMPANY_SCORE", "0.6"))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_company_id(value: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in str(value or "").lower())
    return "_".join(part for part in safe.split("_") if part) or "unknown"


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-zA-Z0-9]+", str(text or "").lower())
        if len(token) >= 3
    }


def _iter_text_values(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        for item in value.values():
            yield from _iter_text_values(item)
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_text_values(item)
        return

    text = str(value or "").strip()
    if text and text.lower() not in _EMPTY_VALUES:
        yield text


def _company_profile_text(doc: Dict[str, Any]) -> str:
    lines = [str(doc.get("company_name") or doc.get("company_id") or "").strip()]
    consolidated_json = ((doc.get("consolidated") or {}).get("json") or {})
    for text in _iter_text_values(consolidated_json):
        lines.append(text)
        if len(lines) >= 200:
            break
    return "\n".join(line for line in lines if line)


class CompanyNotFoundError(LookupError):
    pass


class SearchService:
    def __init__(
        self,
        *,
        semantic_backend: Any | None = None,
        local_store: Any | None = None,
        reranker: Any | None = None,
    ) -> None:
        self._semantic_backend = semantic_backend or PineconeSemanticSearch()
        self._local_store = local_store or get_local_store_client()
        self._reranker = reranker if reranker is not None else OpenSourceReranker()

    def _resolve_company_doc(self, company: str, *, user_id: str) -> Dict[str, Any] | None:
        user_id = require_user_id(user_id, context="search company resolution")
        company_name = str(company or "").strip()
        company_id = _make_company_id(company_name)

        if hasattr(self._local_store, "get_company"):
            doc = self._local_store.get_company(
                company_id=company_id,
                company_name=company_name,
                user_id=user_id,
            )
            if isinstance(doc, dict) and doc:
                return doc

        try:
            rows = self._local_store.get_companies_full_data(
                company_ids=[company_id],
                company_names=[company_name],
                user_id=user_id,
            )
        except Exception:
            return None

        if company_id in rows:
            return rows[company_id]
        return next(iter(rows.values()), None)

    def _hydrate(self, matches: list[Dict[str, Any]], include_full_data: bool, *, user_id: str) -> list[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="search result hydration")
        if not include_full_data or not matches:
            return [{**item, "full_company_data": None} for item in matches]

        hydrated: Dict[str, Dict[str, Any]] = {}
        try:
            hydrated = self._local_store.get_companies_full_data(
                company_ids=[str(item.get("company_id") or "") for item in matches],
                company_names=[str(item.get("company_name") or "") for item in matches],
                user_id=user_id,
            )
        except Exception as exc:
            logger.warning(f"Local hydration failed (non-fatal): {exc}")

        enriched = []
        for item in matches:
            company_id = str(item.get("company_id") or "")
            company_name = str(item.get("company_name") or "")

            full_data = hydrated.get(company_id)
            if full_data is None and hydrated:
                full_data = next(
                    (
                        row
                        for row in hydrated.values()
                        if str(row.get("company_name", "")).lower() == company_name.lower()
                    ),
                    None,
                )

            full_data = self._repair_full_company_data(
                full_data,
                company_id=company_id,
                company_name=company_name,
                user_id=user_id,
            )
            enriched.append({**item, "full_company_data": full_data})
        return enriched

    def _repair_full_company_data(
        self,
        doc: Dict[str, Any] | None,
        *,
        company_id: str,
        company_name: str,
        user_id: str,
    ) -> Dict[str, Any] | None:
        user_id = require_user_id(user_id, context="search company repair")
        if not isinstance(doc, dict) or not doc:
            return doc

        normalized = dict(doc)
        consolidated = dict(normalized.get("consolidated") or {})
        raw_data = dict(normalized.get("raw_data") or {})
        chunks = normalized.get("chunks") if isinstance(normalized.get("chunks"), list) else []
        consolidated_json = consolidated.get("json") if isinstance(consolidated.get("json"), dict) else {}
        if not consolidated_json:
            normalized["consolidated"] = consolidated
            normalized["raw_data"] = raw_data
            normalized["chunks"] = chunks
            return normalized

        normalized_name = (
            str(normalized.get("company_name") or "").strip()
            or str(consolidated_json.get("company_name") or "").strip()
            or company_name
            or company_id
            or "unknown"
        )
        run_id = str(raw_data.get("last_run_id") or consolidated.get("run_id") or f"repair-{company_id or 'company'}")

        generated = None
        generated_chunks = chunks
        if not generated_chunks:
            generated = generate_semantic_chunks(normalized_name, consolidated_json)
            generated_chunks = list(generated.get("chunks") or [])

        chunk_count = int(consolidated.get("chunk_count") or 0)
        chunk_coverage_pct = float(consolidated.get("chunk_coverage_pct") or 0.0)

        needs_chunk_count = chunk_count <= 0
        needs_coverage = chunk_coverage_pct <= 0.0
        if needs_coverage and generated is None:
            generated = generate_semantic_chunks(normalized_name, consolidated_json)

        coverage = dict((generated or {}).get("coverage") or {})
        if needs_chunk_count:
            chunk_count = len(generated_chunks)
        if needs_coverage:
            chunk_coverage_pct = float(coverage.get("coverage_pct") or 0.0)

        needs_schema_field_count = not raw_data.get("schema_field_count")
        needs_last_run_id = not raw_data.get("last_run_id") and bool(consolidated.get("run_id"))
        needs_repair = (
            (not chunks and bool(generated_chunks))
            or needs_chunk_count
            or needs_coverage
            or needs_schema_field_count
            or needs_last_run_id
        )

        normalized["chunks"] = generated_chunks
        normalized["consolidated"] = {
            **consolidated,
            "chunk_count": chunk_count,
            "chunk_coverage_pct": chunk_coverage_pct,
        }
        normalized["raw_data"] = {
            **raw_data,
            "schema_field_count": raw_data.get("schema_field_count") or EXPECTED_FIELD_COUNT,
            "last_run_id": raw_data.get("last_run_id") or consolidated.get("run_id"),
        }

        if needs_repair and hasattr(self._local_store, "repair_company_chunks"):
            try:
                persisted = self._local_store.repair_company_chunks(
                    run_id=run_id,
                    company_name=normalized_name,
                    company_id=company_id or str(normalized.get("company_id") or ""),
                    consolidated_json=consolidated_json,
                    chunk_count=chunk_count,
                    chunk_coverage_pct=chunk_coverage_pct,
                    chunks=generated_chunks,
                    schema_field_count=EXPECTED_FIELD_COUNT,
                    user_id=user_id,
                )
                if isinstance(persisted, dict) and persisted:
                    normalized = persisted
            except Exception as exc:
                logger.warning(f"Local chunk repair failed (non-fatal): {exc}")

        return normalized

    def _find_local_similar_companies(
        self,
        *,
        source_doc: Dict[str, Any],
        source_company_id: str,
        top_k: int,
        exclude_company: str = "",
        user_id: str,
    ) -> list[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="local company similarity")
        if not hasattr(self._local_store, "list_companies"):
            raise RuntimeError("Local company listing is unavailable.")

        source_tokens = _tokenize(_company_profile_text(source_doc))
        if not source_tokens:
            return []

        exclude = exclude_company.strip().lower()
        matches = []
        for candidate in self._local_store.list_companies(user_id=user_id):
            if not isinstance(candidate, dict):
                continue

            candidate_id = str(candidate.get("company_id") or "").strip()
            candidate_name = str(candidate.get("company_name") or "").strip()
            if not candidate_id or candidate_id == source_company_id:
                continue
            if exclude and candidate_name.lower() == exclude:
                continue

            candidate_text = _company_profile_text(candidate)
            candidate_tokens = _tokenize(candidate_text)
            if not candidate_tokens:
                continue

            overlap_terms = sorted(source_tokens.intersection(candidate_tokens))
            if not overlap_terms:
                continue

            precision = len(overlap_terms) / max(len(source_tokens), 1)
            recall = len(overlap_terms) / max(len(candidate_tokens), 1)
            score = round((0.65 * precision) + (0.35 * recall), 4)

            matches.append(
                {
                    "company_id": candidate_id,
                    "company_name": candidate_name,
                    "score": score,
                    "max_score": score,
                    "match_count": len(overlap_terms),
                    "top_chunks": [
                        {
                            "chunk_id": None,
                            "chunk_title": "Profile overlap",
                            "score": score,
                            "overlap_terms": overlap_terms[:10],
                            "snippet": candidate_text[:300],
                        }
                    ],
                    "category": "Profile overlap",
                    "snippet": candidate_text[:300],
                    "shared_categories": [],
                }
            )

        matches.sort(
            key=lambda row: (
                float(row.get("score") or 0.0),
                int(row.get("match_count") or 0),
                str(row.get("company_name") or "").lower(),
            ),
            reverse=True,
        )
        return matches[: max(1, int(top_k))]

    def _apply_semantic_thresholds(self, matches: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        filtered = []
        for row in matches:
            score = float(row.get("score") or 0.0)
            max_score = float(row.get("max_score") or 0.0)
            if score < MIN_SEMANTIC_COMPANY_SCORE or max_score < MIN_SEMANTIC_CHUNK_SCORE:
                continue
            filtered.append(row)
        return filtered

    def search_companies(
        self,
        *,
        query: str,
        top_k: int = 5,
        top_k_chunks: int = 200,
        exclude_company: str = "",
        include_full_data: bool = True,
        filters: Optional[Dict[str, Any]] = None,
        user_id: str,
    ) -> Dict[str, Any]:
        user_id = require_user_id(user_id, context="company search")
        normalized_query = str(query or "").strip()
        if not normalized_query:
            raise ValueError("'query' must not be empty.")

        normalized_top_k = max(1, min(int(top_k), 50))
        normalized_top_k_chunks = max(normalized_top_k * 10, min(int(top_k_chunks), 500))

        backend = "pinecone"
        try:
            raw_matches = self._semantic_backend.search(
                query=normalized_query,
                top_k_chunks=normalized_top_k_chunks,
                filters=filters or {},
                user_id=user_id,
            )
            raw_matches = self._reranker.rerank(query=normalized_query, matches=raw_matches)
            matches = rank_company_matches(
                raw_matches,
                top_k=normalized_top_k,
                exclude_company=exclude_company,
            )
            matches = self._apply_semantic_thresholds(matches)
        except Exception as exc:
            backend = "local_store"
            logger.warning(f"Pinecone search unavailable, falling back to local search: {exc}")
            try:
                matches = self._local_store.search_similar_companies_from_chunks(
                    query_text=normalized_query,
                    top_k_companies=normalized_top_k,
                    top_k_chunks=normalized_top_k_chunks,
                    filters=filters or {},
                    exclude_company_name=exclude_company or None,
                    user_id=user_id,
                )
            except Exception as inner_exc:
                logger.error(f"Similarity search failed: {inner_exc}")
                raise RuntimeError(f"Similarity search unavailable: {inner_exc}") from inner_exc

        enriched = self._hydrate(matches, include_full_data, user_id=user_id)
        return {
            "query": normalized_query,
            "results": enriched,
            "result_count": len(enriched),
            "backend": backend,
            "top_k": normalized_top_k,
            "top_k_chunks": normalized_top_k_chunks,
            "date": _utc_now_iso(),
        }

    def find_similar_companies(
        self,
        *,
        company: str,
        top_k: int = 5,
        top_k_chunks: int = 200,
        exclude_company: str = "",
        include_full_data: bool = True,
        filters: Optional[Dict[str, Any]] = None,
        user_id: str,
    ) -> Dict[str, Any]:
        user_id = require_user_id(user_id, context="company similarity search")
        normalized_company = str(company or "").strip()
        if not normalized_company:
            raise ValueError("'query' must not be empty.")

        normalized_top_k = max(1, min(int(top_k), 50))
        normalized_top_k_chunks = max(normalized_top_k * 10, min(int(top_k_chunks), 500))

        source_doc = self._resolve_company_doc(normalized_company, user_id=user_id)
        source_company_id = str((source_doc or {}).get("company_id") or _make_company_id(normalized_company))
        source_company_name = str((source_doc or {}).get("company_name") or normalized_company)

        backend = "pinecone"
        try:
            similarity_payload = self._semantic_backend.find_similar_companies(
                company_id=source_company_id,
                top_k_chunks=normalized_top_k_chunks,
                filters=filters or {},
                user_id=user_id,
            )
            source_vectors = list(similarity_payload.get("source_vectors") or [])
            raw_matches = list(similarity_payload.get("matches") or [])
            if not source_vectors:
                raise CompanyNotFoundError(
                    f"No vectors found for '{source_company_name}'. Run the pipeline for this company first."
                )

            source_company_name = str(source_vectors[0].get("company_name") or source_company_name)
            matches = rank_similar_companies(
                raw_matches,
                source_company_id=source_company_id,
                source_category_count=len(source_vectors),
                top_k=normalized_top_k,
                exclude_company=exclude_company,
            )
        except CompanyNotFoundError:
            if source_doc is None:
                raise
            backend = "local_store"
            matches = self._find_local_similar_companies(
                source_doc=source_doc,
                source_company_id=source_company_id,
                top_k=normalized_top_k,
                exclude_company=exclude_company,
                user_id=user_id,
            )
        except Exception as exc:
            backend = "local_store"
            logger.warning(f"Pinecone company similarity unavailable, falling back to local profiles: {exc}")
            if source_doc is None:
                raise RuntimeError(f"Company similarity unavailable: {exc}") from exc
            try:
                matches = self._find_local_similar_companies(
                    source_doc=source_doc,
                    source_company_id=source_company_id,
                    top_k=normalized_top_k,
                    exclude_company=exclude_company,
                    user_id=user_id,
                )
            except Exception as inner_exc:
                logger.error(f"Company similarity failed: {inner_exc}")
                raise RuntimeError(f"Company similarity unavailable: {inner_exc}") from inner_exc

        enriched = self._hydrate(matches, include_full_data, user_id=user_id)
        return {
            "query": normalized_company,
            "company": {
                "company_id": source_company_id,
                "company_name": source_company_name,
            },
            "results": enriched,
            "result_count": len(enriched),
            "backend": backend,
            "top_k": normalized_top_k,
            "top_k_chunks": normalized_top_k_chunks,
            "date": _utc_now_iso(),
        }


_CLIENT: SearchService | None = None


def get_search_service() -> SearchService:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = SearchService()
    return _CLIENT
