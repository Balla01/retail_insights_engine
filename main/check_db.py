from __future__ import annotations

import argparse
import json
import os
from typing import Any

DEFAULT_CHROMA_PATH = "/home/ntlpt19/personal_projects/sales_rag/main/chroma_db"
DEFAULT_COLLECTION = "schema_metadata"
DEFAULT_EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL_NAME", "Alibaba-NLP/gte-large-en-v1.5")
DEFAULT_RERANK_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"

try:
    import chromadb
except Exception:
    chromadb = None

try:
    from sentence_transformers import CrossEncoder
except Exception:
    CrossEncoder = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple ChromaDB viewer/query tool")
    parser.add_argument("--path", default=DEFAULT_CHROMA_PATH, help="Chroma persistent path")
    parser.add_argument("--collection", default=DEFAULT_COLLECTION, help="Collection name")
    parser.add_argument("--limit", type=int, default=20, help="How many rows to list")
    parser.add_argument("--query", default=None, help="Search query for relevant chunks")
    parser.add_argument("--top-k", type=int, default=3, help="How many relevant chunks to return")
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_EMBEDDING_MODEL,
        help="Embedding model for semantic query",
    )
    parser.add_argument(
        "--rerank",
        action="store_true",
        help="Apply CrossEncoder reranking on retrieved chunks",
    )
    parser.add_argument(
        "--rerank-model",
        default=DEFAULT_RERANK_MODEL,
        help="CrossEncoder model name for reranking",
    )
    parser.add_argument(
        "--rerank-pool",
        type=int,
        default=12,
        help="How many chunks to fetch before reranking",
    )
    return parser.parse_args()


def list_chunks(collection, limit: int) -> None:
    data = collection.get(limit=limit, include=["documents", "metadatas"])
    ids = data.get("ids", [])
    docs = data.get("documents", [])
    metas = data.get("metadatas", [])

    print(f"Collection: {collection.name}")
    print(f"Total rows: {collection.count()}")
    print(f"Showing: {len(ids)}\n")

    for i, row_id in enumerate(ids, start=1):
        doc = docs[i - 1] if i - 1 < len(docs) else ""
        meta = metas[i - 1] if i - 1 < len(metas) else {}
        preview = doc if len(doc) <= 400 else doc[:400] + "..."

        print(f"[{i}] id={row_id}")
        print(f"metadata={json.dumps(meta, ensure_ascii=True)}")
        print(f"chunk={preview}\n")


def _rerank_hits(query: str, hits: list[dict[str, Any]], model_name: str) -> list[dict[str, Any]]:
    if not hits:
        return hits
    if CrossEncoder is None:
        print("sentence-transformers not installed, skipping rerank.")
        return hits

    model = CrossEncoder(model_name)
    pairs = [[query, item.get("document", "")] for item in hits]
    scores = model.predict(pairs)
    for i, score in enumerate(scores):
        hits[i]["rerank_score"] = float(score)
    return sorted(hits, key=lambda item: item.get("rerank_score", float("-inf")), reverse=True)


def query_chunks(
    collection,
    query: str,
    top_k: int,
    rerank: bool = False,
    rerank_model: str = DEFAULT_RERANK_MODEL,
    rerank_pool: int = 12,
) -> None:
    n_results = max(top_k, rerank_pool) if rerank else top_k
    result = collection.query(
        query_texts=[query],
        n_results=n_results,
        include=["documents", "metadatas", "distances"],
    )

    docs = (result.get("documents") or [[]])[0]
    metas = (result.get("metadatas") or [[]])[0]
    dists = (result.get("distances") or [[]])[0]
    ids = (result.get("ids") or [[]])[0]

    hits: list[dict[str, Any]] = []
    for i, doc in enumerate(docs):
        hits.append(
            {
                "id": ids[i] if i < len(ids) else "",
                "metadata": metas[i] if i < len(metas) else {},
                "distance": dists[i] if i < len(dists) else None,
                "document": doc,
                "retrieval_rank": i + 1,
            }
        )

    if rerank:
        hits = _rerank_hits(query, hits, rerank_model)

    hits = hits[:top_k]

    print(f"Query: {query}")
    print(f"Top {len(hits)} relevant chunks:\n")

    for i, item in enumerate(hits, start=1):
        row_id = item.get("id", "")
        meta = item.get("metadata", {})
        dist = item.get("distance")
        doc = item.get("document", "")
        preview = doc if len(doc) <= 400 else doc[:400] + "..."
        rerank_score = item.get("rerank_score")
        retrieval_rank = item.get("retrieval_rank")

        score_text = f" rerank_score={rerank_score}" if rerank_score is not None else ""
        print(f"[{i}] id={row_id} distance={dist} retrieval_rank={retrieval_rank}{score_text}")
        print(f"metadata={json.dumps(meta, ensure_ascii=True)}")
        print(f"chunk={preview}\n")


def main() -> None:
    args = parse_args()

    if chromadb is None:
        raise SystemExit("chromadb not installed. Run: pip install chromadb")

    client = chromadb.PersistentClient(path=args.path)

    try:
        if args.query:
            # Use the exact same embedding function family as ingestion/retrieval pipeline.
            from retail_insights.retrieval import GteEmbeddingFunction

            embedding_fn = GteEmbeddingFunction(args.embedding_model)
            collection = client.get_collection(name=args.collection, embedding_function=embedding_fn)
        else:
            collection = client.get_collection(name=args.collection)
    except Exception:
        raise SystemExit(f"Collection '{args.collection}' not found at: {args.path}")

    if args.query:
        query_chunks(
            collection=collection,
            query=args.query,
            top_k=args.top_k,
            rerank=args.rerank,
            rerank_model=args.rerank_model,
            rerank_pool=args.rerank_pool,
        )
    else:
        list_chunks(collection, args.limit)


if __name__ == "__main__":
    main()
