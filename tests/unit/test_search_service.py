from search.company_similarity import rank_company_matches, rank_similar_companies
from search.search_service import CompanyNotFoundError, SearchService


class DummySemanticBackend:
    def __init__(self, matches=None, exc=None, similar_payload=None, similar_exc=None):
        self.matches = matches or []
        self.exc = exc
        self.similar_payload = similar_payload or {"source_vectors": [], "matches": []}
        self.similar_exc = similar_exc
        self.calls = []
        self.similar_calls = []

    def search(self, *, query, top_k_chunks, filters, user_id):
        self.calls.append(
            {
                "query": query,
                "top_k_chunks": top_k_chunks,
                "filters": filters,
                "user_id": user_id,
            }
        )
        if self.exc is not None:
            raise self.exc
        return list(self.matches)

    def find_similar_companies(self, *, company_id, top_k_chunks, filters, user_id):
        self.similar_calls.append(
            {
                "company_id": company_id,
                "top_k_chunks": top_k_chunks,
                "filters": filters,
                "user_id": user_id,
            }
        )
        if self.similar_exc is not None:
            raise self.similar_exc
        return {
            "source_vectors": list(self.similar_payload.get("source_vectors") or []),
            "matches": list(self.similar_payload.get("matches") or []),
        }


class DummyReranker:
    def __init__(self, score_shift=0.0):
        self.score_shift = score_shift
        self.calls = []

    def rerank(self, *, query, matches):
        candidates = [dict(match) for match in matches]
        self.calls.append({"query": query, "count": len(candidates)})
        reranked = []
        for match in candidates:
            score = round(min(1.0, max(0.0, float(match.get("score") or 0.0) + self.score_shift)), 4)
            reranked.append(
                {
                    **match,
                    "vector_score": match.get("score"),
                    "rerank_score": score,
                    "lexical_score": 0.2,
                    "overlap_terms": ["automation"],
                    "score": score,
                }
            )
        return reranked


class DummyLocalStore:
    def __init__(self, *, full_data=None, local_matches=None, company_docs=None):
        self.full_data = full_data or {}
        self.local_matches = local_matches or []
        self.company_docs = list(company_docs or [])
        self.search_calls = []
        self.hydration_calls = []
        self.get_company_calls = []
        self.repair_calls = []

    def search_similar_companies_from_chunks(
        self,
        *,
        query_text,
        top_k_companies,
        top_k_chunks,
        filters,
        exclude_company_name,
        user_id,
    ):
        self.search_calls.append(
            {
                "query_text": query_text,
                "top_k_companies": top_k_companies,
                "top_k_chunks": top_k_chunks,
                "filters": filters,
                "exclude_company_name": exclude_company_name,
                "user_id": user_id,
            }
        )
        return list(self.local_matches)

    def get_companies_full_data(self, *, company_ids, company_names, user_id):
        self.hydration_calls.append(
            {
                "company_ids": company_ids,
                "company_names": company_names,
                "user_id": user_id,
            }
        )
        return dict(self.full_data)

    def get_company(self, *, company_id="", company_name="", user_id):
        self.get_company_calls.append(
            {
                "company_id": company_id,
                "company_name": company_name,
                "user_id": user_id,
            }
        )
        wanted_name = company_name.strip().lower()
        for doc in self.company_docs:
            if doc.get("company_id") == company_id:
                return dict(doc)
            if str(doc.get("company_name") or "").strip().lower() == wanted_name:
                return dict(doc)
        return None

    def list_companies(self, *, user_id):
        self.hydration_calls.append({"list_user_id": user_id})
        return [dict(doc) for doc in self.company_docs]

    def repair_company_chunks(
        self,
        *,
        run_id,
        company_name,
        company_id,
        consolidated_json,
        chunk_count,
        chunk_coverage_pct,
        chunks,
        schema_field_count=None,
        user_id,
    ):
        self.repair_calls.append(
            {
                "run_id": run_id,
                "company_name": company_name,
                "company_id": company_id,
                "chunk_count": chunk_count,
                "chunk_coverage_pct": chunk_coverage_pct,
                "schema_field_count": schema_field_count,
                "user_id": user_id,
            }
        )
        repaired = {
            "company_id": company_id,
            "company_name": company_name,
            "raw_data": {
                "last_run_id": run_id,
                "schema_field_count": schema_field_count,
            },
            "consolidated": {
                "run_id": run_id,
                "chunk_count": chunk_count,
                "chunk_coverage_pct": chunk_coverage_pct,
                "json": consolidated_json,
            },
            "chunks": list(chunks),
        }
        self.full_data[company_id] = repaired
        return repaired


def test_rank_company_matches_prefers_consistent_multi_category_hits():
    matches = [
        {
            "id": "tempus_innovation",
            "company_id": "tempus",
            "company_name": "Tempus",
            "score": 0.91,
            "category": "Innovation",
            "snippet": "AI diagnostics and clinical decision support.",
        },
        {
            "id": "tempus_market",
            "company_id": "tempus",
            "company_name": "Tempus",
            "score": 0.88,
            "category": "Market",
            "snippet": "Healthcare data platform with oncology focus.",
        },
        {
            "id": "ibm_watson_health_innovation",
            "company_id": "ibm_watson_health",
            "company_name": "IBM Watson Health",
            "score": 0.92,
            "category": "Innovation",
            "snippet": "Enterprise AI for providers and health systems.",
        },
    ]

    ranked = rank_company_matches(matches, top_k=2)

    assert [row["company_name"] for row in ranked] == ["Tempus", "IBM Watson Health"]
    assert ranked[0]["match_count"] == 2
    assert ranked[0]["category"] == "Innovation"
    assert ranked[0]["top_chunks"][0]["chunk_title"] == "Innovation"


def test_search_service_returns_ranked_pinecone_results_with_hydration():
    semantic_backend = DummySemanticBackend(
        matches=[
            {
                "id": "tempus_innovation",
                "company_id": "tempus",
                "company_name": "Tempus",
                "score": 0.91,
                "category": "Innovation",
                "snippet": "AI diagnostics and clinical decision support.",
            },
            {
                "id": "tempus_market",
                "company_id": "tempus",
                "company_name": "Tempus",
                "score": 0.89,
                "category": "Market",
                "snippet": "Precision medicine in oncology.",
            },
            {
                "id": "tcs_healthcare_division_market",
                "company_id": "tcs",
                "company_name": "TCS Healthcare Division",
                "score": 0.84,
                "category": "Market",
                "snippet": "Healthcare and life sciences consulting.",
            },
        ]
    )
    local_store = DummyLocalStore(
        full_data={
            "tempus": {"company_id": "tempus", "company_name": "Tempus", "sector": "Healthcare AI"},
            "tcs": {"company_id": "tcs", "company_name": "TCS Healthcare Division", "sector": "IT Services"},
        }
    )
    reranker = DummyReranker(score_shift=0.02)
    service = SearchService(semantic_backend=semantic_backend, local_store=local_store, reranker=reranker)

    response = service.search_companies(
        query="AI healthcare companies",
        top_k=3,
        top_k_chunks=50,
        include_full_data=True,
        user_id="user-123",
    )

    assert response["backend"] == "pinecone"
    assert response["result_count"] == 2
    assert response["results"][0]["company_name"] == "Tempus"
    assert response["results"][0]["full_company_data"]["sector"] == "Healthcare AI"
    assert semantic_backend.calls[0]["top_k_chunks"] == 50
    assert semantic_backend.calls[0]["user_id"] == "user-123"
    assert reranker.calls[0]["query"] == "AI healthcare companies"
    assert local_store.hydration_calls[0]["company_ids"] == ["tempus", "tcs"]
    assert local_store.hydration_calls[0]["user_id"] == "user-123"


def test_search_service_repairs_missing_chunk_metadata_during_hydration():
    semantic_backend = DummySemanticBackend(
        matches=[
            {
                "id": "wipro_people_talent",
                "company_id": "wipro",
                "company_name": "Wipro",
                "score": 0.92,
                "category": "People & Talent",
                "snippet": "Employee Size and Hiring Velocity signals.",
            }
        ]
    )
    local_store = DummyLocalStore(
        full_data={
            "wipro": {
                "company_id": "wipro",
                "company_name": "Wipro",
                "raw_data": {
                    "last_run_id": "run-123",
                },
                "consolidated": {
                    "run_id": "run-123",
                    "chunk_count": 0,
                    "chunk_coverage_pct": 0.0,
                    "json": {
                        "company_name": "Wipro",
                        "employee_size": "195000",
                        "hiring_velocity": "High",
                    },
                },
                "chunks": [],
            }
        }
    )
    service = SearchService(
        semantic_backend=semantic_backend,
        local_store=local_store,
        reranker=DummyReranker(),
    )

    response = service.search_companies(
        query="company with large employee size and high hiring velocity",
        top_k=3,
        top_k_chunks=40,
        include_full_data=True,
        user_id="user-123",
    )

    full_data = response["results"][0]["full_company_data"]
    assert full_data["consolidated"]["chunk_count"] > 0
    assert full_data["consolidated"]["chunk_coverage_pct"] > 0
    assert len(full_data["chunks"]) > 0
    assert full_data["raw_data"]["schema_field_count"] == 163
    assert local_store.repair_calls[0]["company_id"] == "wipro"
    assert local_store.repair_calls[0]["user_id"] == "user-123"


def test_search_service_falls_back_to_local_store_when_pinecone_fails():
    semantic_backend = DummySemanticBackend(exc=RuntimeError("pinecone unavailable"))
    local_store = DummyLocalStore(
        local_matches=[
            {
                "company_id": "tempus",
                "company_name": "Tempus",
                "score": 0.77,
                "match_count": 3,
                "top_chunks": [{"chunk_id": "1", "chunk_title": "Innovation", "score": 0.77, "overlap_terms": ["ai"]}],
                "category": "Innovation",
                "snippet": "Healthcare AI company.",
            }
        ]
    )
    service = SearchService(
        semantic_backend=semantic_backend,
        local_store=local_store,
        reranker=DummyReranker(),
    )

    response = service.search_companies(
        query="AI healthcare companies",
        top_k=5,
        top_k_chunks=40,
        exclude_company="",
        include_full_data=False,
        user_id="user-123",
    )

    assert response["backend"] == "local_store"
    assert response["result_count"] == 1
    assert response["results"][0]["company_name"] == "Tempus"
    assert response["results"][0]["full_company_data"] is None
    assert local_store.search_calls[0]["top_k_companies"] == 5
    assert local_store.search_calls[0]["top_k_chunks"] == 50
    assert local_store.search_calls[0]["user_id"] == "user-123"


def test_search_service_filters_weak_semantic_matches():
    semantic_backend = DummySemanticBackend(
        matches=[
            {
                "id": "generic_sales",
                "company_id": "generic_co",
                "company_name": "Generic Co",
                "score": 0.41,
                "category": "Sales & Growth",
                "snippet": "Broad enterprise sales information.",
            }
        ]
    )
    service = SearchService(
        semantic_backend=semantic_backend,
        local_store=DummyLocalStore(),
        reranker=DummyReranker(score_shift=0.0),
    )

    response = service.search_companies(
        query="customer support automation for large retailers",
        top_k=5,
        top_k_chunks=40,
        include_full_data=False,
        user_id="user-123",
    )

    assert response["backend"] == "pinecone"
    assert response["result_count"] == 0


def test_search_service_requires_user_id():
    service = SearchService(
        semantic_backend=DummySemanticBackend(),
        local_store=DummyLocalStore(),
        reranker=DummyReranker(),
    )

    try:
        service.search_companies(
            query="AI healthcare companies",
            user_id="",
        )
    except ValueError as exc:
        assert "user_id" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing user_id")


def test_rank_similar_companies_prefers_broader_shared_category_overlap():
    matches = [
        {
            "id": "square_financials",
            "company_id": "square",
            "company_name": "Square",
            "score": 0.91,
            "category": "Financials",
            "source_category": "Financials",
            "snippet": "Merchant payments and business banking.",
        },
        {
            "id": "square_operations",
            "company_id": "square",
            "company_name": "Square",
            "score": 0.88,
            "category": "Operations",
            "source_category": "Operations",
            "snippet": "SMB commerce operations and POS.",
        },
        {
            "id": "adyen_financials",
            "company_id": "adyen",
            "company_name": "Adyen",
            "score": 0.93,
            "category": "Financials",
            "source_category": "Financials",
            "snippet": "Global payments platform.",
        },
    ]

    ranked = rank_similar_companies(
        matches,
        source_company_id="stripe",
        source_category_count=3,
        top_k=2,
    )

    assert [row["company_name"] for row in ranked] == ["Square", "Adyen"]
    assert ranked[0]["match_count"] == 2
    assert ranked[0]["shared_categories"] == ["Financials", "Operations"]


def test_search_service_returns_company_neighbors_from_pinecone_similarity():
    semantic_backend = DummySemanticBackend(
        similar_payload={
            "source_vectors": [
                {"id": "stripe_financials", "company_id": "stripe", "company_name": "Stripe", "category": "Financials"},
                {"id": "stripe_operations", "company_id": "stripe", "company_name": "Stripe", "category": "Operations"},
            ],
            "matches": [
                {
                    "id": "square_financials",
                    "company_id": "square",
                    "company_name": "Square",
                    "score": 0.91,
                    "category": "Financials",
                    "source_category": "Financials",
                    "snippet": "Merchant payments and business banking.",
                },
                {
                    "id": "square_operations",
                    "company_id": "square",
                    "company_name": "Square",
                    "score": 0.89,
                    "category": "Operations",
                    "source_category": "Operations",
                    "snippet": "SMB commerce operations and POS.",
                },
                {
                    "id": "adyen_financials",
                    "company_id": "adyen",
                    "company_name": "Adyen",
                    "score": 0.9,
                    "category": "Financials",
                    "source_category": "Financials",
                    "snippet": "Unified commerce payments infrastructure.",
                },
            ],
        }
    )
    local_store = DummyLocalStore(
        full_data={
            "square": {"company_id": "square", "company_name": "Square", "sector": "Payments"},
            "adyen": {"company_id": "adyen", "company_name": "Adyen", "sector": "Payments"},
        },
        company_docs=[
            {
                "company_id": "stripe",
                "company_name": "Stripe",
                "consolidated": {"json": {"summary": "Payments infrastructure for internet businesses."}},
            }
        ],
    )
    service = SearchService(
        semantic_backend=semantic_backend,
        local_store=local_store,
        reranker=DummyReranker(),
    )

    response = service.find_similar_companies(
        company="Stripe",
        top_k=3,
        top_k_chunks=60,
        include_full_data=True,
        user_id="user-123",
    )

    assert response["backend"] == "pinecone"
    assert response["company"]["company_id"] == "stripe"
    assert response["company"]["company_name"] == "Stripe"
    assert response["results"][0]["company_name"] == "Square"
    assert response["results"][0]["shared_categories"] == ["Financials", "Operations"]
    assert response["results"][0]["full_company_data"]["sector"] == "Payments"
    assert semantic_backend.similar_calls[0]["company_id"] == "stripe"
    assert semantic_backend.similar_calls[0]["top_k_chunks"] == 60
    assert semantic_backend.similar_calls[0]["user_id"] == "user-123"


def test_search_service_raises_not_found_when_company_has_no_vectors_or_profile():
    semantic_backend = DummySemanticBackend(
        similar_payload={"source_vectors": [], "matches": []}
    )
    local_store = DummyLocalStore()
    service = SearchService(
        semantic_backend=semantic_backend,
        local_store=local_store,
        reranker=DummyReranker(),
    )

    try:
        service.find_similar_companies(company="Stripe", user_id="user-123")
    except CompanyNotFoundError as exc:
        assert "Stripe" in str(exc)
    else:
        raise AssertionError("Expected CompanyNotFoundError")
