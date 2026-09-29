"""
rag_engine.py — Local/Docker Qdrant vector indexing & protocol retrieval.

Indexes knowledge_base markdown files into Qdrant for semantic retrieval.
Supports both embedded Qdrant (local mode) and remote Qdrant (docker mode).
Uses a simple TF-IDF/keyword-based vector encoding for zero-dependency operation
(no external embedding API required).
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

import numpy as np

from backend.config import get_config

logger = logging.getLogger("copilot.rag_engine")

# ---------------------------------------------------------------------------
# Simple keyword-based vector encoder (no external API needed)
# ---------------------------------------------------------------------------

# Clinical vocabulary for consistent encoding
CLINICAL_VOCAB = [
    "sepsis", "hemodynamic", "hypotension", "tachycardia", "heart", "rate",
    "blood", "pressure", "systolic", "diastolic", "map", "mean", "arterial",
    "hypoxia", "respiratory", "oxygen", "saturation", "spo2", "tachypnea",
    "breathing", "ventilation", "mews", "score", "escalation", "deterioration",
    "clinical", "patient", "vital", "monitoring", "warning", "early",
    "lactate", "immunocompromised", "infection", "antibiotic", "fluid",
    "resuscitation", "icu", "transfer", "notify", "physician", "assessment",
    "situation", "background", "recommendation", "sbar", "protocol",
    "threshold", "trigger", "sustained", "window", "trend", "delta",
    "artifact", "suppression", "probe", "motion", "transient", "stable",
    "compromise", "failure", "acute", "progressive", "desaturation",
    "hr", "sbp", "dbp", "rr", "bpm", "mmhg", "breaths", "percent",
    "immediate", "urgent", "critical", "moderate", "low", "high",
]

VOCAB_INDEX = {word: i for i, word in enumerate(CLINICAL_VOCAB)}
VECTOR_DIM = len(CLINICAL_VOCAB)


def _text_to_vector(text: str) -> List[float]:
    """Convert text to a simple TF-based vector using the clinical vocabulary."""
    text_lower = text.lower()
    words = re.findall(r'\b[a-z]+\b', text_lower)
    vec = np.zeros(VECTOR_DIM, dtype=np.float32)
    for word in words:
        if word in VOCAB_INDEX:
            vec[VOCAB_INDEX[word]] += 1.0
    # L2 normalize
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec.tolist()


def _chunk_text(text: str, chunk_size: int = 500, overlap: int = 100) -> List[str]:
    """Split text into overlapping chunks."""
    words = text.split()
    chunks = []
    start = 0
    while start < len(words):
        end = min(start + chunk_size, len(words))
        chunk = " ".join(words[start:end])
        if chunk.strip():
            chunks.append(chunk)
        start += chunk_size - overlap
    return chunks if chunks else [text]


# ---------------------------------------------------------------------------
# RAG Engine
# ---------------------------------------------------------------------------

class RAGEngine:
    """
    Protocol retrieval engine backed by Qdrant vector database.
    Indexes knowledge_base/ markdown files and supports semantic search.
    """

    def __init__(self):
        self._cfg = get_config()
        self._client = None
        self._collection_name = "clinical_protocols"
        self._indexed = False
        self._raw_docs: Dict[str, str] = {}  # filename -> full text
        self._chunks: List[Tuple[str, str]] = []  # (filename, chunk_text)
        self._initialize()

    def _initialize(self):
        """Initialize Qdrant client and index knowledge base."""
        try:
            from qdrant_client import QdrantClient
            from qdrant_client.models import Distance, VectorParams, PointStruct

            if self._cfg.mode == "docker":
                self._client = QdrantClient(url=self._cfg.qdrant_url, timeout=5)
            else:
                os.makedirs(self._cfg.qdrant_local_path, exist_ok=True)
                self._client = QdrantClient(path=self._cfg.qdrant_local_path)

            # Create or recreate collection
            collections = [c.name for c in self._client.get_collections().collections]
            if self._collection_name in collections:
                self._client.delete_collection(self._collection_name)

            self._client.create_collection(
                collection_name=self._collection_name,
                vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
            )

            # Index knowledge base
            self._index_knowledge_base()
            self._indexed = True
            logger.info(f"RAG engine initialized with {len(self._chunks)} chunks")

        except Exception as e:
            logger.warning(f"RAG engine initialization failed: {e}. Falling back to keyword search.")
            self._client = None
            self._load_raw_docs()

    def _load_raw_docs(self):
        """Load raw markdown documents for fallback keyword search."""
        kb_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "knowledge_base")
        if not os.path.isdir(kb_dir):
            logger.warning(f"Knowledge base directory not found: {kb_dir}")
            return
        for fname in os.listdir(kb_dir):
            if fname.endswith(".md"):
                fpath = os.path.join(kb_dir, fname)
                with open(fpath, "r", encoding="utf-8") as f:
                    self._raw_docs[fname] = f.read()
                    chunks = _chunk_text(self._raw_docs[fname])
                    for chunk in chunks:
                        self._chunks.append((fname, chunk))

    def _index_knowledge_base(self):
        """Index all markdown files from knowledge_base/ into Qdrant."""
        from qdrant_client.models import PointStruct

        self._load_raw_docs()
        points = []
        for idx, (fname, chunk) in enumerate(self._chunks):
            vector = _text_to_vector(chunk)
            point_id = idx + 1
            points.append(PointStruct(
                id=point_id,
                vector=vector,
                payload={"filename": fname, "chunk": chunk},
            ))

        if points:
            self._client.upsert(
                collection_name=self._collection_name,
                points=points,
            )
            logger.info(f"Indexed {len(points)} chunks into Qdrant")

    def retrieve(self, query: str, top_k: int = 3) -> List[Dict]:
        """
        Retrieve relevant protocol chunks for a query.
        Returns list of {filename, chunk, score}.
        """
        if self._client and self._indexed:
            return self._retrieve_qdrant(query, top_k)
        else:
            return self._retrieve_keyword(query, top_k)

    def _retrieve_qdrant(self, query: str, top_k: int) -> List[Dict]:
        """Retrieve using Qdrant semantic search."""
        query_vector = _text_to_vector(query)
        try:
            response = self._client.query_points(
                collection_name=self._collection_name,
                query=query_vector,
                limit=top_k,
            )
            results = response.points
            return [
                {
                    "filename": r.payload["filename"],
                    "chunk": r.payload["chunk"],
                    "score": r.score,
                }
                for r in results
            ]
        except Exception as e:
            logger.error(f"Qdrant search failed: {e}")
            return self._retrieve_keyword(query, top_k)

    def _retrieve_keyword(self, query: str, top_k: int) -> List[Dict]:
        """Fallback keyword-based retrieval."""
        query_lower = query.lower()
        query_words = set(re.findall(r'\b[a-z]+\b', query_lower))
        scored = []
        for fname, chunk in self._chunks:
            chunk_lower = chunk.lower()
            chunk_words = set(re.findall(r'\b[a-z]+\b', chunk_lower))
            overlap = len(query_words & chunk_words)
            if overlap > 0:
                scored.append({"filename": fname, "chunk": chunk, "score": overlap / max(len(query_words), 1)})
        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored[:top_k]

    def get_protocol_filenames(self, query: str) -> List[str]:
        """Return just the filenames of matching protocols."""
        results = self.retrieve(query, top_k=5)
        seen = set()
        filenames = []
        for r in results:
            if r["filename"] not in seen:
                seen.add(r["filename"])
                filenames.append(r["filename"])
        return filenames

    def close(self):
        """Close the Qdrant client."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass


# Module-level singleton
_rag: Optional[RAGEngine] = None


def get_rag() -> RAGEngine:
    global _rag
    if _rag is None:
        _rag = RAGEngine()
    return _rag


def reset_rag():
    global _rag
    if _rag:
        _rag.close()
    _rag = None
