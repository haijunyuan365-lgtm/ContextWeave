"""Central configuration for the knowledge-base RAG pipeline."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
        return value if minimum <= value <= maximum else default
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
        return value if minimum <= value <= maximum else default
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class RAGConfig:
    """Validated knobs shared by retrieval, fusion, diversity and reranking."""

    final_top_k: int = 3
    recall_k: int = 10
    rrf_k: int = 60
    mmr_candidates: int = 24
    rerank_candidates: int = 12
    mmr_lambda: float = 0.8
    rewrite_count: int = 3
    rewritten_query_weight: float = 0.7

    @classmethod
    def from_env(cls) -> "RAGConfig":
        return cls(
            final_top_k=_int("ECHOMIND_RAG_TOP_K", 3, 1, 20),
            recall_k=_int("ECHOMIND_RAG_RECALL_K", 10, 1, 100),
            rrf_k=_int("ECHOMIND_RAG_RRF_K", 60, 1, 1000),
            mmr_candidates=_int("ECHOMIND_RAG_MMR_CANDIDATES", 24, 1, 200),
            rerank_candidates=_int("ECHOMIND_RAG_RERANK_CANDIDATES", 12, 1, 100),
            mmr_lambda=_float("ECHOMIND_RAG_MMR_LAMBDA", 0.8, 0.0, 1.0),
            rewrite_count=_int("ECHOMIND_RAG_REWRITE_COUNT", 3, 0, 10),
            rewritten_query_weight=_float("ECHOMIND_RAG_REWRITE_WEIGHT", 0.7, 0.0, 1.0),
        )

