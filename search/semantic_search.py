from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.user_scope import require_user_id


def _normalized_filter_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


class PineconeSemanticSearch:
    def __init__(self, pinecone_client: Any | None = None) -> None:
        self._pinecone_client = pinecone_client

    def _client(self) -> Any:
        if self._pinecone_client is not None:
            return self._pinecone_client

        from core.pinecone_store import get_pinecone_client

        return get_pinecone_client()

    def search(
        self,
        *,
        query: str,
        top_k_chunks: int = 200,
        filters: Optional[Dict[str, Any]] = None,
        user_id: str,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="semantic backend search")
        normalized_filters = dict(filters or {})
        category_filter = _normalized_filter_text(normalized_filters.pop("category", None))
        company_id_filter = _normalized_filter_text(normalized_filters.pop("company_id", None))
        version = _normalized_filter_text(normalized_filters.pop("version", None))
        if version is None:
            from core.pinecone_store import VERSION as default_version

            version = default_version

        return self._client().search_categories(
            query_text=query,
            top_k=max(1, int(top_k_chunks)),
            category_filter=category_filter,
            company_id_filter=company_id_filter,
            version=version,
            filters=normalized_filters,
            user_id=user_id,
        )

    def find_similar_companies(
        self,
        *,
        company_id: str,
        top_k_chunks: int = 200,
        filters: Optional[Dict[str, Any]] = None,
        user_id: str,
    ) -> Dict[str, Any]:
        user_id = require_user_id(user_id, context="semantic backend company similarity")
        normalized_filters = dict(filters or {})
        version = _normalized_filter_text(normalized_filters.pop("version", None))
        if version is None:
            from core.pinecone_store import VERSION as default_version

            version = default_version

        return self._client().find_similar_companies(
            company_id=company_id,
            top_k_chunks=max(1, int(top_k_chunks)),
            version=version,
            filters=normalized_filters,
            user_id=user_id,
        )
