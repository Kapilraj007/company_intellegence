from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from core import pinecone_store as store_module
from core.prompts import _FLAT_KEYS


def test_query_filter_enforces_user_scope():
    store = store_module.PineconeStore.__new__(store_module.PineconeStore)

    pinecone_filter = store._build_query_filter(
        user_id_filter="user-1",
        category_filter="Financials",
        company_id_filter="acme",
        filters={"region": "us", "user_id": "spoofed-user"},
    )

    assert pinecone_filter["user_id"] == {"$eq": "user-1"}
    assert pinecone_filter["category"] == {"$eq": "Financials"}
    assert pinecone_filter["company_id"] == {"$eq": "acme"}
    assert pinecone_filter["region"] == {"$eq": "us"}


def test_build_vectors_use_user_scoped_ids(monkeypatch):
    store = store_module.PineconeStore.__new__(store_module.PineconeStore)
    monkeypatch.setattr(store_module, "_embed_document", lambda _text: [0.1, 0.2, 0.3])

    field_key = _FLAT_KEYS[1]
    vectors = store._build_vectors(
        run_id="run-1",
        company_id="acme",
        company_name="Acme",
        golden_record={field_key: "Some value"},
        user_id="user-123",
    )

    assert vectors
    assert vectors[0]["id"].startswith("user_123__acme_")
    assert vectors[0]["metadata"]["user_id"] == "user-123"


@dataclass
class _FetchResult:
    vectors: Dict[str, Any]


@dataclass
class _VectorPayload:
    metadata: Dict[str, Any]
    values: list[float]


class _FakeIndex:
    def __init__(self, vectors: Dict[str, Any]) -> None:
        self._vectors = vectors

    def fetch(self, *, ids, namespace):  # noqa: ANN001
        del ids
        del namespace
        return _FetchResult(vectors=self._vectors)


@dataclass
class _QueryMatch:
    id: str
    score: float
    metadata: Dict[str, Any]


@dataclass
class _QueryResult:
    matches: list[_QueryMatch]


class _FallbackQueryIndex:
    def __init__(self) -> None:
        self.filters = []

    def query(self, *, vector, top_k, include_metadata, filter, namespace):  # noqa: ANN001
        del vector
        del top_k
        del include_metadata
        del namespace
        self.filters.append(filter)
        if len(self.filters) == 1:
            return _QueryResult(matches=[])
        return _QueryResult(
            matches=[
                _QueryMatch(
                    id="legacy-1",
                    score=0.77,
                    metadata={
                        "company_id": "acme",
                        "company_name": "Acme",
                        "category": "Financials",
                        "version": store_module.VERSION,
                    },
                )
            ]
        )


def test_fetch_company_vectors_skips_other_users_legacy_vectors():
    store = store_module.PineconeStore.__new__(store_module.PineconeStore)
    category = store_module._all_categories()[0]
    legacy_id = store_module._legacy_chunk_vector_id("acme", category, 0)
    store._index = _FakeIndex(
        {
            legacy_id: _VectorPayload(
                metadata={
                    "company_id": "acme",
                    "company_name": "Acme",
                    "user_id": "user-b",
                    "category": category,
                    "version": store_module.VERSION,
                },
                values=[0.2, 0.4],
            )
        }
    )

    vectors = store.fetch_company_vectors(company_id="acme", user_id="user-a")
    assert vectors == []


def test_search_categories_falls_back_to_legacy_scope_when_user_filter_has_no_matches(monkeypatch):
    store = store_module.PineconeStore.__new__(store_module.PineconeStore)
    store._index = _FallbackQueryIndex()
    monkeypatch.setattr(store_module, "_embed_query", lambda _text: [0.1, 0.2])
    monkeypatch.setattr(store_module, "ALLOW_LEGACY_UNSCOPED_FALLBACK", True)

    rows = store.search_categories(query_text="banking ai", user_id="user-a")

    assert len(rows) == 1
    assert rows[0]["scope"] == "legacy"
    assert store._index.filters[0]["user_id"] == {"$eq": "user-a"}
    assert "user_id" not in store._index.filters[1]
