"""Innovation clustering pipeline built on top of company vectors."""

from ml.clustering.innovation_cluster import InnovationClusterConfig, InnovationClusterer

__all__ = [
    "InnovationClusterConfig",
    "InnovationClusterer",
    "InnovationClusterPipeline",
    "get_innovation_cluster_pipeline",
]


def __getattr__(name: str):
    if name in {"InnovationClusterPipeline", "get_innovation_cluster_pipeline"}:
        from ml.clustering.cluster_pipeline import InnovationClusterPipeline, get_innovation_cluster_pipeline

        return {
            "InnovationClusterPipeline": InnovationClusterPipeline,
            "get_innovation_cluster_pipeline": get_innovation_cluster_pipeline,
        }[name]
    raise AttributeError(name)
