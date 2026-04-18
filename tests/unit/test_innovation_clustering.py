from ml.clustering.cluster_pipeline import InnovationClusterPipeline
from ml.clustering.innovation_cluster import InnovationClusterConfig, InnovationClusterer


class DummyLocalStore:
    def __init__(self, docs):
        self._docs = list(docs)
        self.calls = []

    def list_companies(self, *, user_id):
        self.calls.append({"user_id": user_id})
        return [dict(doc) for doc in self._docs]


class BrokenPineconeClient:
    def fetch_company_vectors(self, *, company_id, user_id):
        raise RuntimeError(f"pinecone unavailable for {company_id}")


def test_innovation_clusterer_groups_ai_and_fintech_companies():
    rows = [
        {
            "company_id": "openai",
            "company_name": "OpenAI",
            "vector": [1.0, 0.1, 0.0],
            "cluster_text": "AI infrastructure models inference platform and GPU compute.",
            "dominant_categories": ["Innovation", "Infrastructure"],
            "vector_source": "pinecone",
            "vector_count": 8,
        },
        {
            "company_id": "nvidia",
            "company_name": "NVIDIA",
            "vector": [0.95, 0.05, 0.0],
            "cluster_text": "GPU datacenter infrastructure for AI training and inference.",
            "dominant_categories": ["Technology", "Infrastructure"],
            "vector_source": "pinecone",
            "vector_count": 8,
        },
        {
            "company_id": "stripe",
            "company_name": "Stripe",
            "vector": [0.0, 1.0, 0.1],
            "cluster_text": "Payments merchant acquiring fintech APIs and banking rails.",
            "dominant_categories": ["Financials", "Platform"],
            "vector_source": "pinecone",
            "vector_count": 8,
        },
        {
            "company_id": "paypal",
            "company_name": "PayPal",
            "vector": [0.1, 0.9, 0.05],
            "cluster_text": "Digital payments, merchant checkout, consumer banking, and fintech products.",
            "dominant_categories": ["Financials", "Operations"],
            "vector_source": "pinecone",
            "vector_count": 8,
        },
    ]

    result = InnovationClusterer().cluster(
        rows,
        config=InnovationClusterConfig(
            algorithm="kmeans",
            reduction="pca",
            n_clusters=2,
        ),
    )

    assert result["company_count"] == 4
    assert result["cluster_count"] == 2

    labels = {cluster["label"] for cluster in result["clusters"]}
    assert labels == {"AI Infrastructure", "FinTech"}

    members_by_company = {
        member["company_id"]: cluster["label"]
        for cluster in result["clusters"]
        for member in cluster["members"]
    }
    assert members_by_company["openai"] == members_by_company["nvidia"]
    assert members_by_company["stripe"] == members_by_company["paypal"]


def test_cluster_pipeline_falls_back_to_local_embeddings_when_pinecone_is_unavailable():
    docs = [
        {
            "company_id": "openai",
            "company_name": "OpenAI",
            "chunks": [
                {"chunk_title": "Innovation", "chunk_text": "AI models, inference platform, and infrastructure."},
                {"chunk_title": "Products", "chunk_text": "Foundation models and enterprise AI APIs."},
            ],
        },
        {
            "company_id": "nvidia",
            "company_name": "NVIDIA",
            "chunks": [
                {"chunk_title": "Infrastructure", "chunk_text": "GPU infrastructure for datacenter AI workloads."},
            ],
        },
        {
            "company_id": "stripe",
            "company_name": "Stripe",
            "chunks": [
                {"chunk_title": "Payments", "chunk_text": "Payments, merchant acquiring, and fintech APIs."},
            ],
        },
        {
            "company_id": "paypal",
            "company_name": "PayPal",
            "chunks": [
                {"chunk_title": "Fintech", "chunk_text": "Digital payments, checkout, merchant tools, and banking."},
            ],
        },
    ]

    def fake_embed_texts(texts):
        vectors = []
        for text in texts:
            lowered = text.lower()
            if any(term in lowered for term in ["ai", "gpu", "inference", "infrastructure"]):
                vectors.append([1.0, 0.0, 0.0])
            elif any(term in lowered for term in ["payment", "payments", "merchant", "fintech", "banking"]):
                vectors.append([0.0, 1.0, 0.0])
            else:
                vectors.append([0.0, 0.0, 1.0])
        return vectors

    pipeline = InnovationClusterPipeline(
        pinecone_client=BrokenPineconeClient(),
        local_store=DummyLocalStore(docs),
        embed_texts=fake_embed_texts,
    )

    result = pipeline.run(
        algorithm="kmeans",
        reduction="pca",
        n_clusters=2,
        user_id="user-123",
    )

    assert result["company_count"] == 4
    assert result["source_breakdown"] == {"local_embeddings": 4}
    assert pipeline._local_store.calls[0]["user_id"] == "user-123"

    members_by_company = {
        member["company_id"]: cluster["label"]
        for cluster in result["clusters"]
        for member in cluster["members"]
    }
    assert members_by_company["openai"] == members_by_company["nvidia"]
    assert members_by_company["stripe"] == members_by_company["paypal"]
