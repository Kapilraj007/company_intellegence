from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import re
from typing import Any, Dict, List, Sequence

import numpy as np
from sklearn.cluster import DBSCAN, KMeans
from sklearn.decomposition import PCA, TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score

try:
    import umap  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    umap = None

try:
    import hdbscan  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    hdbscan = None


_LABEL_ALIASES = {
    "ai": "AI",
    "ml": "ML",
    "llm": "LLM",
    "gpu": "GPU",
    "erp": "ERP",
    "soc": "SOC",
    "fintech": "FinTech",
    "healthtech": "HealthTech",
    "saas": "SaaS",
}
_THEME_RULES = [
    ("AI Infrastructure", {"ai", "gpu", "llm", "inference", "infrastructure", "compute", "datacenter", "model"}),
    ("FinTech", {"fintech", "payment", "payments", "merchant", "banking", "bank", "fraud", "lending"}),
    ("HealthTech", {"healthcare", "clinical", "oncology", "hospital", "patient", "biotech"}),
    ("Cybersecurity", {"cybersecurity", "security", "identity", "soc", "threat", "fraud"}),
    ("Retail Commerce", {"retail", "commerce", "ecommerce", "store", "customer support"}),
    ("Cloud Platform", {"cloud", "platform", "infrastructure", "devops", "kubernetes"}),
    ("Talent Intelligence", {"talent", "hiring", "employee", "recruiting", "workforce"}),
]


@dataclass(slots=True)
class InnovationClusterConfig:
    algorithm: str = "auto"
    reduction: str = "auto"
    n_clusters: int | None = None
    min_cluster_size: int = 2
    max_clusters: int = 8
    random_state: int = 42
    include_noise: bool = False


class InnovationClusterer:
    """
    Cluster company-level innovation vectors into thematic groups.

    Input rows are expected to contain:
      - company_id
      - company_name
      - vector
      - cluster_text
      - dominant_categories
      - vector_source
      - vector_count
    """

    def cluster(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        config: InnovationClusterConfig | None = None,
    ) -> Dict[str, Any]:
        normalized_rows = [self._normalize_row(row) for row in rows if isinstance(row, dict)]
        normalized_rows = [row for row in normalized_rows if row is not None]
        if not normalized_rows:
            raise ValueError("No company vectors were provided for clustering.")

        config = config or InnovationClusterConfig()
        matrix = np.vstack([row["vector"] for row in normalized_rows]).astype("float32")
        matrix = self._l2_normalize(matrix)

        reduced_matrix, reduction_meta = self._reduce(matrix, config=config)
        labels, algorithm_meta = self._cluster_labels(reduced_matrix, config=config)
        points = self._project_points(reduced_matrix)
        clusters = self._assemble_clusters(
            rows=normalized_rows,
            matrix=matrix,
            reduced_matrix=reduced_matrix,
            labels=labels,
            points=points,
            include_noise=config.include_noise,
        )

        non_noise_clusters = [cluster for cluster in clusters if cluster["cluster_id"] != -1]
        return {
            "company_count": len(normalized_rows),
            "cluster_count": len(non_noise_clusters),
            "noise_count": sum(1 for label in labels if int(label) == -1),
            "algorithm": algorithm_meta,
            "reduction": reduction_meta,
            "clusters": clusters,
            "points": [
                {
                    "company_id": row["company_id"],
                    "company_name": row["company_name"],
                    "cluster_id": int(label),
                    "x": point["x"],
                    "y": point["y"],
                    "vector_source": row.get("vector_source"),
                }
                for row, label, point in zip(normalized_rows, labels, points)
            ],
        }

    def _normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any] | None:
        vector = np.asarray(row.get("vector") or [], dtype="float32")
        if vector.ndim != 1 or vector.size == 0:
            return None

        company_id = str(row.get("company_id") or "").strip()
        company_name = str(row.get("company_name") or company_id or "Unknown").strip()
        if not company_id:
            company_id = re.sub(r"[^a-z0-9]+", "_", company_name.lower()).strip("_") or "unknown"

        dominant_categories = row.get("dominant_categories") or row.get("categories") or []
        if not isinstance(dominant_categories, list):
            dominant_categories = [str(dominant_categories)]

        return {
            "company_id": company_id,
            "company_name": company_name,
            "vector": vector,
            "cluster_text": str(row.get("cluster_text") or "").strip(),
            "dominant_categories": [str(item) for item in dominant_categories if str(item).strip()],
            "vector_source": str(row.get("vector_source") or "unknown"),
            "vector_count": int(row.get("vector_count") or 0),
        }

    @staticmethod
    def _l2_normalize(matrix: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return matrix / norms

    def _reduce(
        self,
        matrix: np.ndarray,
        *,
        config: InnovationClusterConfig,
    ) -> tuple[np.ndarray, Dict[str, Any]]:
        sample_count, dimension_count = matrix.shape
        if sample_count <= 2 or dimension_count <= 2 or config.reduction == "none":
            return matrix, {"requested_method": config.reduction, "effective_method": "none", "components": int(dimension_count)}

        target_components = max(2, min(12, sample_count - 1, dimension_count))
        requested = config.reduction
        effective = requested
        fallback_reason = None

        if requested == "auto":
            effective = "umap" if umap is not None and sample_count >= 6 else "pca"

        if effective == "umap" and umap is None:
            effective = "pca"
            fallback_reason = "umap-learn is not installed"

        if effective == "umap":
            reducer = umap.UMAP(  # type: ignore[union-attr]
                n_neighbors=max(2, min(15, sample_count - 1)),
                n_components=min(5, target_components),
                metric="cosine",
                random_state=config.random_state,
            )
            reduced = reducer.fit_transform(matrix)
        elif effective == "svd":
            components = max(1, min(target_components, dimension_count - 1))
            reducer = TruncatedSVD(n_components=components, random_state=config.random_state)
            reduced = reducer.fit_transform(matrix)
        else:
            components = max(2, min(target_components, sample_count, dimension_count))
            reducer = PCA(n_components=components, random_state=config.random_state)
            reduced = reducer.fit_transform(matrix)

        return reduced.astype("float32"), {
            "requested_method": requested,
            "effective_method": effective,
            "components": int(reduced.shape[1]),
            "fallback_reason": fallback_reason,
        }

    def _cluster_labels(
        self,
        matrix: np.ndarray,
        *,
        config: InnovationClusterConfig,
    ) -> tuple[np.ndarray, Dict[str, Any]]:
        sample_count = matrix.shape[0]
        if sample_count == 1:
            return np.asarray([0], dtype=int), {
                "requested_algorithm": config.algorithm,
                "effective_algorithm": "single_cluster",
                "n_clusters": 1,
                "fallback_reason": None,
            }

        requested = config.algorithm
        effective = requested
        fallback_reason = None

        if requested == "auto":
            effective = "hdbscan" if hdbscan is not None and sample_count >= max(6, config.min_cluster_size * 2) else "kmeans"

        if effective == "hdbscan" and hdbscan is None:
            effective = "dbscan"
            fallback_reason = "hdbscan is not installed"

        if effective == "dbscan":
            clusterer = DBSCAN(eps=0.7, min_samples=max(2, config.min_cluster_size))
            labels = clusterer.fit_predict(matrix)
            unique = {int(label) for label in labels if int(label) >= 0}
            if not unique:
                effective = "kmeans"
                fallback_reason = fallback_reason or "dbscan produced only noise"
            else:
                return labels.astype(int), {
                    "requested_algorithm": requested,
                    "effective_algorithm": "dbscan",
                    "n_clusters": len(unique),
                    "fallback_reason": fallback_reason,
                }

        if effective == "hdbscan":
            clusterer = hdbscan.HDBSCAN(  # type: ignore[union-attr]
                min_cluster_size=max(2, config.min_cluster_size),
                metric="euclidean",
            )
            labels = clusterer.fit_predict(matrix)
            unique = {int(label) for label in labels if int(label) >= 0}
            if unique:
                return labels.astype(int), {
                    "requested_algorithm": requested,
                    "effective_algorithm": "hdbscan",
                    "n_clusters": len(unique),
                    "fallback_reason": fallback_reason,
                }
            effective = "kmeans"
            fallback_reason = fallback_reason or "hdbscan produced only noise"

        n_clusters = config.n_clusters
        if n_clusters is None:
            n_clusters = self._choose_cluster_count(matrix, config=config)

        n_clusters = max(1, min(int(n_clusters), sample_count))
        if n_clusters == 1:
            return np.zeros(sample_count, dtype=int), {
                "requested_algorithm": requested,
                "effective_algorithm": "single_cluster",
                "n_clusters": 1,
                "fallback_reason": fallback_reason,
            }

        clusterer = KMeans(
            n_clusters=n_clusters,
            n_init="auto",
            random_state=config.random_state,
        )
        labels = clusterer.fit_predict(matrix)
        return labels.astype(int), {
            "requested_algorithm": requested,
            "effective_algorithm": "kmeans",
            "n_clusters": n_clusters,
            "fallback_reason": fallback_reason,
        }

    def _choose_cluster_count(
        self,
        matrix: np.ndarray,
        *,
        config: InnovationClusterConfig,
    ) -> int:
        sample_count = matrix.shape[0]
        if sample_count < max(4, config.min_cluster_size * 2):
            return 1

        upper = min(config.max_clusters, sample_count - 1)
        if upper < 2:
            return 1

        best_cluster_count = 2
        best_score = -1.0

        for cluster_count in range(2, upper + 1):
            clusterer = KMeans(
                n_clusters=cluster_count,
                n_init="auto",
                random_state=config.random_state,
            )
            labels = clusterer.fit_predict(matrix)
            if len(set(int(label) for label in labels)) < 2:
                continue
            try:
                score = float(silhouette_score(matrix, labels))
            except Exception:
                continue
            if score > best_score:
                best_score = score
                best_cluster_count = cluster_count

        return best_cluster_count

    @staticmethod
    def _project_points(matrix: np.ndarray) -> List[Dict[str, float]]:
        if matrix.shape[1] == 1:
            return [{"x": float(value), "y": 0.0} for value in matrix[:, 0]]
        return [{"x": float(row[0]), "y": float(row[1])} for row in matrix[:, :2]]

    def _assemble_clusters(
        self,
        *,
        rows: Sequence[Dict[str, Any]],
        matrix: np.ndarray,
        reduced_matrix: np.ndarray,
        labels: np.ndarray,
        points: Sequence[Dict[str, float]],
        include_noise: bool,
    ) -> List[Dict[str, Any]]:
        cluster_ids = sorted({int(label) for label in labels})
        clusters: List[Dict[str, Any]] = []

        for cluster_id in cluster_ids:
            if cluster_id == -1 and not include_noise:
                continue

            indices = [idx for idx, label in enumerate(labels) if int(label) == cluster_id]
            if not indices:
                continue

            member_rows = [rows[idx] for idx in indices]
            member_vectors = matrix[indices]
            centroid = np.mean(member_vectors, axis=0)
            centroid_norm = float(np.linalg.norm(centroid)) or 1.0
            centroid = centroid / centroid_norm

            top_categories = self._top_categories(member_rows)
            top_terms = self._top_terms(member_rows)
            label = self._resolve_cluster_label(cluster_id, top_terms=top_terms, top_categories=top_categories)

            members = []
            for idx in indices:
                row = rows[idx]
                similarity = float(np.dot(matrix[idx], centroid))
                members.append(
                    {
                        "company_id": row["company_id"],
                        "company_name": row["company_name"],
                        "similarity_to_centroid": round(similarity, 4),
                        "vector_source": row.get("vector_source"),
                        "vector_count": row.get("vector_count"),
                        "dominant_categories": row.get("dominant_categories") or [],
                        "point": points[idx],
                    }
                )

            members.sort(
                key=lambda item: (
                    float(item.get("similarity_to_centroid") or 0.0),
                    int(item.get("vector_count") or 0),
                    str(item.get("company_name") or "").lower(),
                ),
                reverse=True,
            )

            cohesion = self._cluster_cohesion(member_vectors, centroid)
            clusters.append(
                {
                    "cluster_id": cluster_id,
                    "label": label,
                    "size": len(indices),
                    "cohesion_score": cohesion,
                    "top_terms": top_terms,
                    "top_categories": top_categories,
                    "members": members,
                }
            )

        clusters.sort(
            key=lambda item: (
                item["cluster_id"] == -1,
                -int(item["size"]),
                -float(item.get("cohesion_score") or 0.0),
                str(item.get("label") or ""),
            )
        )
        return clusters

    @staticmethod
    def _cluster_cohesion(member_vectors: np.ndarray, centroid: np.ndarray) -> float:
        if member_vectors.size == 0:
            return 0.0
        similarities = member_vectors @ centroid
        return round(float(np.mean(similarities)), 4)

    @staticmethod
    def _top_categories(rows: Sequence[Dict[str, Any]], limit: int = 3) -> List[str]:
        counter: Counter[str] = Counter()
        for row in rows:
            for category in row.get("dominant_categories") or []:
                category_text = str(category).strip()
                if category_text:
                    counter[category_text] += 1
        return [item for item, _ in counter.most_common(limit)]

    def _top_terms(self, rows: Sequence[Dict[str, Any]], limit: int = 4) -> List[str]:
        texts = [str(row.get("cluster_text") or "").strip() for row in rows if str(row.get("cluster_text") or "").strip()]
        if not texts:
            return []

        company_tokens = {
            token
            for row in rows
            for token in re.split(r"[^a-z0-9]+", str(row.get("company_name") or "").lower())
            if len(token) >= 3
        }

        vectorizer = TfidfVectorizer(
            stop_words="english",
            ngram_range=(1, 2),
            max_features=256,
        )
        try:
            matrix = vectorizer.fit_transform(texts)
        except ValueError:
            return []

        scores = np.asarray(matrix.mean(axis=0)).ravel()
        if scores.size == 0:
            return []

        feature_names = vectorizer.get_feature_names_out()
        ranked_indices = np.argsort(scores)[::-1]
        terms: List[str] = []

        for index in ranked_indices:
            term = str(feature_names[index]).strip()
            if not term:
                continue
            tokens = [token for token in re.split(r"[^a-z0-9]+", term.lower()) if token]
            if not tokens:
                continue
            if any(token in company_tokens for token in tokens):
                continue
            if term not in terms:
                terms.append(term)
            if len(terms) >= limit:
                break

        return terms

    def _resolve_cluster_label(
        self,
        cluster_id: int,
        *,
        top_terms: Sequence[str],
        top_categories: Sequence[str],
    ) -> str:
        normalized_terms = {
            token
            for term in list(top_terms) + list(top_categories)
            for token in re.split(r"[^a-z0-9]+", str(term).lower())
            if token
        }

        for label, keywords in _THEME_RULES:
            if normalized_terms.intersection(keywords):
                return label

        if top_terms:
            return self._compose_label(top_terms[:2])
        if top_categories:
            return self._compose_label(top_categories[:2])
        if cluster_id == -1:
            return "Noise"
        return f"Cluster {cluster_id + 1}"

    def _compose_label(self, terms: Sequence[str]) -> str:
        cleaned = [self._humanize_term(term) for term in terms if str(term).strip()]
        if not cleaned:
            return "Cluster"
        if len(cleaned) == 1:
            return cleaned[0]

        simple_terms = [term for term in cleaned if " " not in term]
        if len(simple_terms) == len(cleaned):
            return " ".join(cleaned[:2])
        return " / ".join(cleaned[:2])

    def _humanize_term(self, term: str) -> str:
        pieces = [piece for piece in re.split(r"[\s/_-]+", str(term).strip()) if piece]
        if not pieces:
            return "Cluster"

        normalized = []
        for piece in pieces:
            alias = _LABEL_ALIASES.get(piece.lower())
            normalized.append(alias or piece.capitalize())
        return " ".join(normalized)
