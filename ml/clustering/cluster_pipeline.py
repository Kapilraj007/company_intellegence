from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Any, Callable, Dict, List, Sequence

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import HashingVectorizer

from core.chunking import generate_semantic_chunks
from core.local_store import get_local_store_client
from core.pinecone_store import EMBEDDING_MODEL_NAME, get_pinecone_client
from core.user_scope import require_user_id
from logger import get_logger
from ml.clustering.innovation_cluster import InnovationClusterConfig, InnovationClusterer

logger = get_logger("innovation_cluster_pipeline")

_EMPTY_VALUES = {"not found", "n/a", "na", "unknown", "none", "null", "", "-"}
_INNOVATION_SECTIONS = [
    (
        "Innovation & Technology",
        [
            "category",
            "ai_ml_adoption_level",
            "tech_stack_tools_used",
            "intellectual_property",
            "rd_investment",
            "innovation_roadmap",
            "product_pipeline",
            "industry_benchmark_tech_adoption",
        ],
    ),
    (
        "Market & Offering",
        [
            "focus_sectors_industries",
            "services_offerings_products",
            "core_value_proposition",
            "unique_differentiators",
            "competitive_advantages",
            "strategic_priorities",
            "future_projections",
        ],
    ),
    (
        "Customers & GTM",
        [
            "top_customers_by_client_segments",
            "sales_motion",
            "go_to_market_strategy",
            "partnership_ecosystem",
            "customer_acquisition_cost",
            "customer_lifetime_value",
            "net_promoter_score",
        ],
    ),
    (
        "Talent & Execution",
        [
            "employee_size",
            "hiring_velocity",
            "employee_turnover",
            "work_culture",
            "learning_culture",
            "automation_level",
            "cross_functional_exposure",
        ],
    ),
]
_MODEL: SentenceTransformer | None = None
_MODEL_UNAVAILABLE = False
_HASHING_VECTORIZER = HashingVectorizer(
    n_features=384,
    alternate_sign=False,
    norm="l2",
    stop_words="english",
)
_PINECONE_UNSET = object()


def _default_embed_texts(texts: Sequence[str]) -> List[List[float]]:
    global _MODEL, _MODEL_UNAVAILABLE
    if _MODEL_UNAVAILABLE:
        matrix = _HASHING_VECTORIZER.transform(list(texts))
        return matrix.toarray().astype("float32").tolist()

    try:
        if _MODEL is None:
            logger.info("[InnovationClustering] Loading sentence-transformers model...")
            _MODEL = SentenceTransformer(EMBEDDING_MODEL_NAME)
        embeddings = _MODEL.encode(list(texts), normalize_embeddings=True)
        return [list(row) for row in embeddings]
    except Exception as exc:
        _MODEL_UNAVAILABLE = True
        logger.warning(
            f"[InnovationClustering] Embedding model unavailable, using offline lexical vectors: {exc}"
        )
        matrix = _HASHING_VECTORIZER.transform(list(texts))
        return matrix.toarray().astype("float32").tolist()


class InnovationClusterPipeline:
    """
    Collect company vectors, reduce dimensionality, and cluster companies by
    innovation pattern. Existing search and ingestion flows stay untouched.
    """

    def __init__(
        self,
        *,
        pinecone_client: Any = _PINECONE_UNSET,
        local_store: Any | None = None,
        clusterer: InnovationClusterer | None = None,
        embed_texts: Callable[[Sequence[str]], List[List[float]]] | None = None,
    ) -> None:
        self._pinecone_client = pinecone_client
        self._local_store = local_store or get_local_store_client()
        self._clusterer = clusterer or InnovationClusterer()
        self._embed_texts = embed_texts or _default_embed_texts

    def run(
        self,
        *,
        company_ids: Sequence[str] | None = None,
        company_names: Sequence[str] | None = None,
        limit: int | None = None,
        algorithm: str = "auto",
        reduction: str = "auto",
        n_clusters: int | None = None,
        min_cluster_size: int = 2,
        include_noise: bool = False,
        user_id: str,
    ) -> Dict[str, Any]:
        user_id = require_user_id(user_id, context="innovation clustering")
        company_vectors = self.collect_company_vectors(
            company_ids=company_ids,
            company_names=company_names,
            limit=limit,
            user_id=user_id,
        )
        if not company_vectors:
            raise ValueError("No company vectors are available for clustering.")

        config = InnovationClusterConfig(
            algorithm=algorithm,
            reduction=reduction,
            n_clusters=n_clusters,
            min_cluster_size=min_cluster_size,
            include_noise=include_noise,
        )
        clustered = self._clusterer.cluster(company_vectors, config=config)
        clustered.update(
            {
                "source_breakdown": dict(Counter(row.get("vector_source") or "unknown" for row in company_vectors)),
                "companies": [
                    {
                        "company_id": row["company_id"],
                        "company_name": row["company_name"],
                        "vector_source": row["vector_source"],
                        "vector_count": row["vector_count"],
                        "dominant_categories": row["dominant_categories"],
                    }
                    for row in company_vectors
                ],
            }
        )
        return clustered

    def collect_company_vectors(
        self,
        *,
        company_ids: Sequence[str] | None = None,
        company_names: Sequence[str] | None = None,
        limit: int | None = None,
        user_id: str,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="innovation vector collection")
        docs = self._select_company_docs(
            company_ids=company_ids,
            company_names=company_names,
            limit=limit,
            user_id=user_id,
        )
        company_vectors: List[Dict[str, Any]] = []

        for doc in docs:
            record = self._collect_company_vector(doc, user_id=user_id)
            if record is not None:
                company_vectors.append(record)

        company_vectors.sort(key=lambda row: str(row.get("company_name") or "").lower())
        return company_vectors

    def _select_company_docs(
        self,
        *,
        company_ids: Sequence[str] | None = None,
        company_names: Sequence[str] | None = None,
        limit: int | None = None,
        user_id: str,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="innovation company selection")
        rows = []
        if hasattr(self._local_store, "list_companies"):
            rows = list(self._local_store.list_companies(user_id=user_id))
        elif hasattr(self._local_store, "get_companies_full_data"):
            fetched = self._local_store.get_companies_full_data(
                company_ids=list(company_ids or []),
                company_names=list(company_names or []),
                user_id=user_id,
            )
            rows = list(fetched.values()) if isinstance(fetched, dict) else []

        wanted_ids = {str(company_id).strip() for company_id in company_ids or [] if str(company_id).strip()}
        wanted_names = {str(company_name).strip().lower() for company_name in company_names or [] if str(company_name).strip()}

        filtered = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            company_id = str(row.get("company_id") or "").strip()
            company_name = str(row.get("company_name") or "").strip()
            if wanted_ids and company_id not in wanted_ids:
                continue
            if wanted_names and company_name.lower() not in wanted_names:
                continue
            filtered.append(row)

        filtered.sort(key=lambda row: str(row.get("company_name") or row.get("company_id") or "").lower())
        if limit is not None:
            return filtered[: max(1, int(limit))]
        return filtered

    def _collect_company_vector(self, doc: Dict[str, Any], *, user_id: str) -> Dict[str, Any] | None:
        user_id = require_user_id(user_id, context="innovation company vector collection")
        company_id = str(doc.get("company_id") or "").strip()
        company_name = str(doc.get("company_name") or company_id or "Unknown").strip()
        if not company_id:
            return None

        pinecone_rows = self._fetch_pinecone_vectors(company_id, user_id=user_id)
        if pinecone_rows:
            return self._build_company_record(
                company_id=company_id,
                company_name=company_name,
                vector_rows=pinecone_rows,
                vector_source="pinecone",
            )

        local_rows = self._build_local_vector_rows(doc)
        if not local_rows:
            return None

        return self._build_company_record(
            company_id=company_id,
            company_name=company_name,
            vector_rows=local_rows,
            vector_source="local_embeddings",
        )

    def _fetch_pinecone_vectors(self, company_id: str, *, user_id: str) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="innovation Pinecone vector fetch")
        client = self._get_pinecone_client()
        if client is None:
            return []
        try:
            return list(client.fetch_company_vectors(company_id=company_id, user_id=user_id) or [])
        except Exception as exc:
            logger.warning(f"[InnovationClustering] Pinecone fetch failed for '{company_id}': {exc}")
            return []

    def _build_local_vector_rows(self, doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        consolidated = doc.get("consolidated") if isinstance(doc.get("consolidated"), dict) else {}
        consolidated_json = consolidated.get("json") if isinstance(consolidated.get("json"), dict) else {}
        company_name = str(doc.get("company_name") or doc.get("company_id") or "Unknown").strip()

        section_rows = self._build_innovation_section_rows(company_name, consolidated_json)
        if section_rows:
            texts = [row["chunk_text"] for row in section_rows]
            embeddings = self._embed_texts(texts)
            return [
                {
                    "values": list(embedding or []),
                    "category": row["category"],
                    "chunk_title": row["chunk_title"],
                    "chunk_text": row["chunk_text"],
                }
                for row, embedding in zip(section_rows, embeddings)
            ]

        chunks = doc.get("chunks")
        if not isinstance(chunks, list) or not chunks:
            if not consolidated_json:
                return []
            chunk_payload = generate_semantic_chunks(company_name, consolidated_json)
            chunks = list(chunk_payload.get("chunks") or [])

        texts = [str(chunk.get("chunk_text") or "").strip() for chunk in chunks if str(chunk.get("chunk_text") or "").strip()]
        if not texts:
            return []

        embeddings = self._embed_texts(texts)
        rows = []
        for chunk, embedding in zip(chunks, embeddings):
            rows.append(
                {
                    "values": list(embedding or []),
                    "category": chunk.get("chunk_title") or chunk.get("chunk_type") or "Semantic Profile",
                    "chunk_title": chunk.get("chunk_title") or chunk.get("chunk_type") or "Semantic Profile",
                    "chunk_text": chunk.get("chunk_text") or "",
                }
            )
        return rows

    def _build_innovation_section_rows(
        self,
        company_name: str,
        consolidated_json: Dict[str, Any],
    ) -> List[Dict[str, str]]:
        if not isinstance(consolidated_json, dict) or not consolidated_json:
            return []

        rows: List[Dict[str, str]] = []
        for title, keys in _INNOVATION_SECTIONS:
            lines = [f"Company: {company_name}", f"Section: {title}"]
            field_count = 0
            for key in keys:
                value = str(consolidated_json.get(key) or "").strip()
                if value.lower() in _EMPTY_VALUES:
                    continue
                lines.append(f"{key.replace('_', ' ').title()}: {value}")
                field_count += 1

            if field_count == 0:
                continue

            rows.append(
                {
                    "category": title,
                    "chunk_title": title,
                    "chunk_text": "\n".join(lines),
                }
            )

        return rows

    def _build_company_record(
        self,
        *,
        company_id: str,
        company_name: str,
        vector_rows: Sequence[Dict[str, Any]],
        vector_source: str,
    ) -> Dict[str, Any] | None:
        vectors = [np.asarray(row.get("values") or [], dtype="float32") for row in vector_rows]
        vectors = [vector for vector in vectors if vector.ndim == 1 and vector.size > 0]
        if not vectors:
            return None

        matrix = np.vstack(vectors)
        centroid = np.mean(matrix, axis=0)
        centroid_norm = float(np.linalg.norm(centroid)) or 1.0
        centroid = centroid / centroid_norm

        category_counter: Counter[str] = Counter()
        texts: List[str] = []
        for row in vector_rows:
            category = str(row.get("category") or row.get("chunk_title") or "").strip()
            if category:
                category_counter[category] += 1
            text = str(row.get("chunk_text") or row.get("snippet") or "").strip()
            if text:
                texts.append(text)

        cluster_text = "\n".join(texts[:8])
        return {
            "company_id": company_id,
            "company_name": company_name,
            "vector": centroid.tolist(),
            "vector_count": len(vectors),
            "vector_source": vector_source,
            "cluster_text": cluster_text,
            "dominant_categories": [item for item, _ in category_counter.most_common(4)],
        }

    def _get_pinecone_client(self) -> Any | None:
        if self._pinecone_client is _PINECONE_UNSET:
            try:
                self._pinecone_client = get_pinecone_client()
            except Exception as exc:
                logger.warning(f"[InnovationClustering] Pinecone unavailable, using local embeddings: {exc}")
                self._pinecone_client = None
        return self._pinecone_client


_PIPELINE: InnovationClusterPipeline | None = None


def get_innovation_cluster_pipeline() -> InnovationClusterPipeline:
    global _PIPELINE
    if _PIPELINE is None:
        _PIPELINE = InnovationClusterPipeline()
    return _PIPELINE


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cluster companies by innovation pattern.")
    parser.add_argument("--company-id", action="append", dest="company_ids", default=[], help="Filter by company_id. Repeat to add more than one.")
    parser.add_argument("--company-name", action="append", dest="company_names", default=[], help="Filter by company name. Repeat to add more than one.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of companies to cluster.")
    parser.add_argument("--algorithm", choices=["auto", "kmeans", "dbscan", "hdbscan"], default="auto")
    parser.add_argument("--reduction", choices=["auto", "none", "pca", "svd", "umap"], default="auto")
    parser.add_argument("--n-clusters", type=int, default=None)
    parser.add_argument("--min-cluster-size", type=int, default=2)
    parser.add_argument("--include-noise", action="store_true")
    parser.add_argument("--user-id", default="", help="Authenticated user id for scoped clustering")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = get_innovation_cluster_pipeline().run(
        company_ids=args.company_ids,
        company_names=args.company_names,
        limit=args.limit,
        algorithm=args.algorithm,
        reduction=args.reduction,
        n_clusters=args.n_clusters,
        min_cluster_size=args.min_cluster_size,
        include_noise=args.include_noise,
        user_id=args.user_id,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
