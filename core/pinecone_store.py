"""
Pinecone vector store for Agent 2 consolidated golden records.

Stores ONLY the validated golden_record from agent3_save.py.
Nothing from Agent 1 goes here.

Design:
  - Index:     company-knowledge
  - Namespace: prod
  - ID format: {user_slug}__{company_id}_{category_slug}_{chunk_idx}
  - Dims:      384  (BAAI/bge-small-en-v1.5)
  - Metric:    cosine

Scalability:
  - Multiple vectors per category for finer search intent matching
  - Still filter by metadata; never scan blindly across versions
  - Always filter by company_id or category — never full-index scan
"""

from __future__ import annotations

import os
import re
from math import ceil
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pinecone import Pinecone, ServerlessSpec
from sentence_transformers import SentenceTransformer
from logger import get_logger
from core.user_scope import require_user_id

logger = get_logger("pinecone_store")

# ── Constants ─────────────────────────────────────────────────────────────────
INDEX_NAME  = "company-knowledge"
NAMESPACE   = "prod"
DIMENSION   = 384
VERSION     = "v2"
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME", "BAAI/bge-small-en-v1.5")
QUERY_EMBEDDING_INSTRUCTION = (
    "Represent this sentence for searching relevant company profiles: "
)
MAX_FIELDS_PER_CHUNK = 4
MAX_CHARS_PER_CHUNK = 450
MAX_CHUNKS_PER_CATEGORY = 12
ALLOW_LEGACY_UNSCOPED_FALLBACK = os.getenv("PINECONE_ALLOW_LEGACY_UNSCOPED_FALLBACK", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

_EMPTY = {"not found", "n/a", "na", "unknown", "none", "null", "", "-"}

# Lazy-loaded embedding model
_MODEL: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        logger.info("[Pinecone] Loading sentence-transformers model...")
        _MODEL = SentenceTransformer(EMBEDDING_MODEL_NAME)
        logger.info("[Pinecone] Model loaded.")
    return _MODEL


def _embed_document(text: str) -> List[float]:
    return _get_model().encode(text, normalize_embeddings=True).tolist()


def _embed_query(text: str) -> List[float]:
    query = f"{QUERY_EMBEDDING_INSTRUCTION}{str(text or '').strip()}"
    return _get_model().encode(query, normalize_embeddings=True).tolist()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_empty(value: str) -> bool:
    return str(value).strip().lower() in _EMPTY


def _category_slug(category: str) -> str:
    """
    Converts category name to a safe ID slug.
    'Strategy & Culture' → 'strategy_and_culture'
    """
    return (
        category.lower()
        .replace(" & ", "_and_")
        .replace(" / ", "_")
        .replace("/", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )


def _user_scope_slug(user_id: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9]+", "_", str(user_id or "").strip().lower())).strip("_") or "unknown_user"


def _legacy_vector_id(company_id: str, category: str, chunk_index: int | None = None) -> str:
    if chunk_index is None:
        return f"{company_id}_{_category_slug(category)}"
    return _legacy_chunk_vector_id(company_id, category, chunk_index)


def _legacy_chunk_vector_id(company_id: str, category: str, chunk_index: int) -> str:
    return f"{company_id}_{_category_slug(category)}_{chunk_index:02d}"


def _vector_id(company_id: str, category: str, user_id: str, chunk_index: int | None = None) -> str:
    """
    Deterministic vector ID — same company + category always produces same ID.
    Prevents duplicates on re-indexing and scopes vectors by authenticated user.
    Example: 'user_123__tcs_financials_00'
    """
    prefix = _user_scope_slug(user_id)
    if chunk_index is None:
        return f"{prefix}__{company_id}_{_category_slug(category)}"
    return _chunk_vector_id(company_id, category, user_id, chunk_index)


def _chunk_vector_id(company_id: str, category: str, user_id: str, chunk_index: int) -> str:
    return f"{_user_scope_slug(user_id)}__{company_id}_{_category_slug(category)}_{chunk_index:02d}"


def _all_vector_ids_for_company(company_id: str, user_id: str) -> List[str]:
    ids = []
    for category in _all_categories():
        ids.append(_vector_id(company_id, category, user_id))
        for chunk_index in range(MAX_CHUNKS_PER_CATEGORY):
            ids.append(_chunk_vector_id(company_id, category, user_id, chunk_index))
    return ids


def _all_legacy_vector_ids_for_company(company_id: str) -> List[str]:
    ids = []
    for category in _all_categories():
        ids.append(_legacy_vector_id(company_id, category))
        for chunk_index in range(MAX_CHUNKS_PER_CATEGORY):
            ids.append(_legacy_chunk_vector_id(company_id, category, chunk_index))
    return ids


# ── Category map from prompts._SCHEMA_ROWS ────────────────────────────────────

def _build_category_map() -> Dict[int, tuple]:
    from core.prompts import _SCHEMA_ROWS
    return {row[0]: (row[1], row[2]) for row in _SCHEMA_ROWS}


def _all_categories() -> List[str]:
    from core.prompts import _SCHEMA_ROWS

    return sorted({row[1] for row in _SCHEMA_ROWS})


# ── Main class ────────────────────────────────────────────────────────────────

class PineconeStore:
    def __init__(self) -> None:
        api_key = os.environ.get("PINECONE_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(
                "PINECONE_API_KEY environment variable is not set. "
                "Semantic search is disabled. Set PINECONE_API_KEY to enable Pinecone search."
            )
        
        pc = Pinecone(api_key=api_key)

        if INDEX_NAME not in pc.list_indexes().names():
            pc.create_index(
                name=INDEX_NAME,
                dimension=DIMENSION,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )
            logger.info(f"[Pinecone] Index '{INDEX_NAME}' created.")

        self._index = pc.Index(INDEX_NAME)
        logger.info(f"[Pinecone] Connected to index '{INDEX_NAME}', namespace='{NAMESPACE}'.")

    # ── Public entry point called from agent3_save.py ─────────────────────────

    def _cleanup_legacy_vectors_for_user(self, *, company_id: str, user_id: str) -> None:
        """Delete legacy unscoped IDs only when they belong to this user."""
        legacy_ids = _all_legacy_vector_ids_for_company(company_id)
        payload = self._index.fetch(ids=legacy_ids, namespace=NAMESPACE)
        vectors = payload.vectors if hasattr(payload, "vectors") else {}
        owned_ids = []
        for vector_id, vector_payload in (vectors or {}).items():
            metadata = getattr(vector_payload, "metadata", None) or {}
            owner = str(metadata.get("user_id") or "").strip()
            if owner == user_id:
                owned_ids.append(vector_id)

        if owned_ids:
            self._index.delete(ids=owned_ids, namespace=NAMESPACE)
            logger.info(
                f"[Pinecone] Migrated {len(owned_ids)} legacy vectors for "
                f"company_id='{company_id}', user_id='{user_id}'."
            )

    def upsert_golden_record(
        self,
        *,
        run_id: str,
        company_id: str,
        company_name: str,
        golden_record: Dict[str, str],
        user_id: str,
    ) -> int:
        user_id = require_user_id(user_id, context="Pinecone golden record upsert")
        """
        Embed and upsert the consolidated golden record into Pinecone.
        Uses deterministic IDs — safe to call multiple times (idempotent).
        Returns the number of vectors upserted.
        """
        try:
            self._cleanup_legacy_vectors_for_user(company_id=company_id, user_id=user_id)
        except Exception as exc:
            logger.warning(f"[Pinecone] Legacy vector cleanup skipped: {exc}")

        vectors = self._build_vectors(
            run_id=run_id,
            company_id=company_id,
            company_name=company_name,
            golden_record=golden_record,
            user_id=user_id,
        )

        if not vectors:
            logger.warning(f"[Pinecone] No vectors to upsert for '{company_name}'.")
            return 0

        # Upsert in batches of 100
        batch_size = 100
        for i in range(0, len(vectors), batch_size):
            self._index.upsert(
                vectors=vectors[i: i + batch_size],
                namespace=NAMESPACE,
            )

        logger.info(
            f"[Pinecone] Upserted {len(vectors)} vectors for '{company_name}' "
            f"(namespace={NAMESPACE}, version={VERSION})."
        )
        return len(vectors)

    # ── Vector builder ────────────────────────────────────────────────────────

    def _build_vectors(
        self,
        *,
        run_id: str,
        company_id: str,
        company_name: str,
        golden_record: Dict[str, str],
        user_id: str,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="Pinecone vector build")
        from core.prompts import _FLAT_KEYS

        category_map = _build_category_map()

        # Group filled fields by category
        by_category: Dict[str, List[tuple]] = {}

        for field_id in range(1, 164):
            category_info = category_map.get(field_id)
            flat_key      = _FLAT_KEYS.get(field_id)

            if category_info is None or flat_key is None:
                continue

            category, param_name = category_info
            value = golden_record.get(flat_key, "Not Found")

            if _is_empty(value):
                continue

            by_category.setdefault(category, []).append((param_name, value))

        # Build multiple deterministic vectors per category for finer-grained retrieval.
        vectors = []
        for category, fields in by_category.items():
            category_chunks = self._split_category_fields(fields)
            total_chunks = len(category_chunks)

            for chunk_index, field_chunk in enumerate(category_chunks):
                chunk_text = self._build_chunk_text(
                    company_name,
                    category,
                    field_chunk,
                    chunk_index=chunk_index,
                    total_chunks=total_chunks,
                )
                embedding = _embed_document(chunk_text)

                vectors.append(
                    {
                        "id": _chunk_vector_id(company_id, category, user_id, chunk_index),
                        "values": embedding,
                        "metadata": {
                            "company_id": company_id,
                            "company_name": company_name,
                            "user_id": user_id,
                            "run_id": run_id,
                            "category": category,
                            "version": VERSION,
                            "chunk_text": chunk_text[:1000],
                            "field_count": len(field_chunk),
                            "chunk_index": chunk_index,
                            "total_chunks": total_chunks,
                            "chunk_title": f"{category} #{chunk_index + 1}",
                            "generated_at": _now_iso(),
                        },
                    }
                )

        return vectors

    def _split_category_fields(self, fields: List[tuple]) -> List[List[tuple]]:
        chunks: List[List[tuple]] = []
        current: List[tuple] = []
        current_chars = 0

        for param_name, value in fields:
            entry = f"{param_name}: {value}"
            entry_len = len(entry)
            if current and (
                len(current) >= MAX_FIELDS_PER_CHUNK
                or current_chars + entry_len > MAX_CHARS_PER_CHUNK
            ):
                chunks.append(current)
                current = []
                current_chars = 0

            current.append((param_name, value))
            current_chars += entry_len

        if current:
            chunks.append(current)

        if len(chunks) <= MAX_CHUNKS_PER_CATEGORY:
            return chunks

        merged_tail = [item for chunk in chunks[MAX_CHUNKS_PER_CATEGORY - 1:] for item in chunk]
        return chunks[: MAX_CHUNKS_PER_CATEGORY - 1] + [merged_tail]

    def _build_chunk_text(
        self,
        company_name: str,
        category: str,
        fields: List[tuple],
        *,
        chunk_index: int,
        total_chunks: int,
    ) -> str:
        """
        Structured chunk text with semantic labels for better embedding quality.

        Example output:
          Company: TCS
          Category: Financials

          Annual Revenues: $25 billion
          Annual Profits: $5 billion
        """
        lines = [
            f"Company: {company_name}",
            f"Category: {category}",
            f"Section: {chunk_index + 1} of {total_chunks}",
            f"Semantic focus: {', '.join(param_name for param_name, _ in fields[:3])}",
            "",  # blank line for separation
        ]
        for param_name, value in fields:
            lines.append(f"{param_name}: {value}")
        return "\n".join(lines)

    # ── Delete a company ──────────────────────────────────────────────────────

    def delete_company(self, *, company_id: str, user_id: str) -> None:
        user_id = require_user_id(user_id, context="Pinecone company delete")
        """
        Delete all vectors for a company using deterministic IDs.
        Called when re-indexing or removing a company.
        """
        ids_to_delete = _all_vector_ids_for_company(company_id, user_id)

        self._index.delete(ids=ids_to_delete, namespace=NAMESPACE)
        logger.info(
            f"[Pinecone] Deleted scoped vectors for company_id='{company_id}', user_id='{user_id}'."
        )

    # ── Semantic search ───────────────────────────────────────────────────────

    def _build_query_filter(
        self,
        *,
        user_id_filter: str | None = None,
        category_filter: str | None = None,
        company_id_filter: str | None = None,
        version: str = VERSION,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        pinecone_filter: Dict[str, Any] = {"version": {"$eq": version}}
        if user_id_filter:
            normalized_user_id = require_user_id(user_id_filter, context="Pinecone query filter")
            pinecone_filter["user_id"] = {"$eq": normalized_user_id}

        if category_filter:
            pinecone_filter["category"] = {"$eq": category_filter}

        if company_id_filter:
            pinecone_filter["company_id"] = {"$eq": company_id_filter}

        for key, value in (filters or {}).items():
            if value is None or key in {"category", "company_id", "version", "user_id"}:
                continue
            if isinstance(value, dict):
                pinecone_filter[key] = value
            elif isinstance(value, (list, tuple, set)):
                values = [item for item in value if item is not None]
                if values:
                    pinecone_filter[key] = {"$in": values}
            else:
                pinecone_filter[key] = {"$eq": value}

        return pinecone_filter

    def search_categories(
        self,
        *,
        query_text: str,
        top_k: int = 25,
        category_filter: str | None = None,
        company_id_filter: str | None = None,
        version: str = VERSION,
        filters: Optional[Dict[str, Any]] = None,
        user_id: str,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="Pinecone category search")
        """
        Return the best matching company-category vectors for a query.
        Each row represents a raw Pinecone category hit before company-level ranking.
        """
        query_vector = _embed_query(query_text)
        pinecone_filter = self._build_query_filter(
            user_id_filter=user_id,
            category_filter=category_filter,
            company_id_filter=company_id_filter,
            version=version,
            filters=filters,
        )

        results = self._index.query(
            vector=query_vector,
            top_k=max(1, int(top_k)),
            include_metadata=True,
            filter=pinecone_filter,
            namespace=NAMESPACE,
        )

        raw_result_matches = list(getattr(results, "matches", []) or [])
        query_scope = "user"
        if not raw_result_matches and ALLOW_LEGACY_UNSCOPED_FALLBACK:
            logger.warning(
                "[Pinecone] No user-scoped matches found; falling back to legacy unscoped query."
            )
            legacy_filter = self._build_query_filter(
                category_filter=category_filter,
                company_id_filter=company_id_filter,
                version=version,
                filters=filters,
            )
            results = self._index.query(
                vector=query_vector,
                top_k=max(1, int(top_k)),
                include_metadata=True,
                filter=legacy_filter,
                namespace=NAMESPACE,
            )
            raw_result_matches = list(getattr(results, "matches", []) or [])
            query_scope = "legacy"

        matches = []
        for match in raw_result_matches:
            metadata = match.metadata or {}
            matches.append(
                {
                    "id": match.id,
                    "company_id": metadata.get("company_id"),
                    "company_name": metadata.get("company_name"),
                    "user_id": metadata.get("user_id"),
                    "scope": query_scope,
                    "score": round(float(match.score), 4),
                    "category": metadata.get("category"),
                    "snippet": metadata.get("chunk_text", "")[:300],
                    "chunk_text": metadata.get("chunk_text", ""),
                    "chunk_title": metadata.get("chunk_title") or metadata.get("category"),
                    "field_count": metadata.get("field_count"),
                    "chunk_index": metadata.get("chunk_index"),
                    "total_chunks": metadata.get("total_chunks"),
                    "version": metadata.get("version"),
                }
            )
        return matches

    def fetch_company_vectors(self, *, company_id: str, user_id: str) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="Pinecone vector fetch")
        """
        Fetch all stored category vectors for a company.
        Returns only vectors that currently exist in Pinecone.
        """
        ids_to_fetch = _all_vector_ids_for_company(company_id, user_id) + _all_legacy_vector_ids_for_company(company_id)
        result = self._index.fetch(ids=ids_to_fetch, namespace=NAMESPACE)
        vectors = result.vectors if hasattr(result, "vectors") else {}

        rows = []
        for category in _all_categories():
            for chunk_index in range(MAX_CHUNKS_PER_CATEGORY):
                vector_id = _chunk_vector_id(company_id, category, user_id, chunk_index)
                payload = vectors.get(vector_id)
                if payload is None and chunk_index == 0:
                    payload = vectors.get(_vector_id(company_id, category, user_id))
                if payload is None:
                    vector_id = _legacy_chunk_vector_id(company_id, category, chunk_index)
                    payload = vectors.get(vector_id)
                if payload is None and chunk_index == 0:
                    vector_id = _legacy_vector_id(company_id, category)
                    payload = vectors.get(vector_id)
                if not payload:
                    continue

                metadata = getattr(payload, "metadata", None) or {}
                owner = str(metadata.get("user_id") or "").strip()
                if owner and owner != user_id:
                    continue
                values = getattr(payload, "values", None)
                if values is None and isinstance(payload, dict):
                    values = payload.get("values")

                rows.append(
                    {
                        "id": vector_id,
                        "company_id": metadata.get("company_id", company_id),
                        "company_name": metadata.get("company_name"),
                        "user_id": metadata.get("user_id"),
                        "category": metadata.get("category", category),
                        "values": list(values or []),
                        "field_count": metadata.get("field_count"),
                        "snippet": metadata.get("chunk_text", "")[:300],
                        "chunk_text": metadata.get("chunk_text", ""),
                        "chunk_title": metadata.get("chunk_title") or metadata.get("category", category),
                        "chunk_index": metadata.get("chunk_index", chunk_index),
                        "total_chunks": metadata.get("total_chunks", 1),
                        "version": metadata.get("version", VERSION),
                    }
                )

        return rows

    def find_similar_companies(
        self,
        *,
        company_id: str,
        top_k_chunks: int = 200,
        version: str = VERSION,
        filters: Optional[Dict[str, Any]] = None,
        user_id: str,
    ) -> Dict[str, Any]:
        user_id = require_user_id(user_id, context="Pinecone company similarity")
        """
        Query the nearest neighbors for each category vector belonging to a company.
        Matching is restricted to the same category to compare like-for-like sections.
        """
        source_vectors = self.fetch_company_vectors(company_id=company_id, user_id=user_id)
        if not source_vectors:
            return {"source_vectors": [], "matches": []}

        per_vector_top_k = max(
            5,
            min(25, ceil(max(1, int(top_k_chunks)) / max(1, len(source_vectors)))),
        )

        matches = []
        for source_vector in source_vectors:
            vector_values = list(source_vector.get("values") or [])
            if not vector_values:
                continue

            pinecone_filter = self._build_query_filter(
                user_id_filter=user_id,
                category_filter=str(source_vector.get("category") or ""),
                version=version,
                filters=filters,
            )
            results = self._index.query(
                vector=vector_values,
                top_k=per_vector_top_k + 1,
                include_metadata=True,
                filter=pinecone_filter,
                namespace=NAMESPACE,
            )
            scoped_matches = list(getattr(results, "matches", []) or [])
            query_scope = "user"
            if not scoped_matches and ALLOW_LEGACY_UNSCOPED_FALLBACK:
                logger.warning(
                    "[Pinecone] No user-scoped similarity matches found; "
                    "falling back to legacy unscoped query."
                )
                legacy_filter = self._build_query_filter(
                    category_filter=str(source_vector.get("category") or ""),
                    version=version,
                    filters=filters,
                )
                results = self._index.query(
                    vector=vector_values,
                    top_k=per_vector_top_k + 1,
                    include_metadata=True,
                    filter=legacy_filter,
                    namespace=NAMESPACE,
                )
                scoped_matches = list(getattr(results, "matches", []) or [])
                query_scope = "legacy"

            for match in scoped_matches:
                metadata = match.metadata or {}
                candidate_company_id = str(metadata.get("company_id") or "")
                if not candidate_company_id or candidate_company_id == company_id:
                    continue

                matches.append(
                    {
                        "id": match.id,
                        "company_id": candidate_company_id,
                        "company_name": metadata.get("company_name"),
                        "user_id": metadata.get("user_id"),
                        "scope": query_scope,
                        "score": round(float(match.score), 4),
                        "category": metadata.get("category"),
                        "source_category": source_vector.get("category"),
                        "snippet": metadata.get("chunk_text", "")[:300],
                        "chunk_text": metadata.get("chunk_text", ""),
                        "field_count": metadata.get("field_count"),
                        "version": metadata.get("version", version),
                    }
                )

        matches.sort(key=lambda row: row["score"], reverse=True)
        return {
            "source_vectors": source_vectors,
            "matches": matches,
        }

    def search(
        self,
        *,
        query_text: str,
        top_k: int = 5,
        category_filter: str | None = None,
        company_id_filter: str | None = None,
        version: str = VERSION,
        user_id: str,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="Pinecone search")
        """
        Semantic search over consolidated company profiles.
        Always uses metadata filters — never scans the full index.

        Args:
            query_text:        Natural language query
            top_k:             Number of companies to return
            category_filter:   Restrict to one category e.g. "Financials"
            company_id_filter: Restrict to one company e.g. "tcs"
            version:           Which version of embeddings to query

        Returns:
            List of dicts: [{company_id, company_name, score, category, snippet}]
        """
        raw_matches = self.search_categories(
            query_text=query_text,
            top_k=top_k * 3,
            category_filter=category_filter,
            company_id_filter=company_id_filter,
            version=version,
            user_id=user_id,
        )

        # Deduplicate — keep highest score per company
        seen: Dict[str, Dict[str, Any]] = {}
        for match in raw_matches:
            cid = str(match.get("company_id") or "")
            if not cid:
                continue
            score = float(match.get("score") or 0.0)
            if cid not in seen or score > seen[cid]["score"]:
                seen[cid] = {
                    "company_id": cid,
                    "company_name": match.get("company_name"),
                    "score": round(score, 4),
                    "category": match.get("category"),
                    "snippet": match.get("snippet", ""),
                }

        ranked = sorted(seen.values(), key=lambda x: x["score"], reverse=True)
        return ranked[:top_k]


# ── Singleton ─────────────────────────────────────────────────────────────────

_CLIENT: PineconeStore | None = None


def get_pinecone_client() -> PineconeStore:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = PineconeStore()
    return _CLIENT
