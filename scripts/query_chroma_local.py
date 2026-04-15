#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions

BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_DIR = BACKEND_DIR / 'chroma_db'
DEFAULT_COLLECTION = 'cricket_semantic_index'


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Query local ChromaDB cricket index')
    p.add_argument('--db-dir', type=Path, default=DEFAULT_DB_DIR)
    p.add_argument('--collection', default=DEFAULT_COLLECTION)
    p.add_argument('--query', required=True)
    p.add_argument('--k', type=int, default=5)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    try:
        client = chromadb.PersistentClient(path=str(args.db_dir.resolve()))
        collection = client.get_collection(args.collection, embedding_function=embedding_functions.DefaultEmbeddingFunction())
        result = collection.query(query_texts=[args.query], n_results=args.k)
    except Exception as exc:  # noqa: BLE001
        # Return an empty result set for missing collections / startup races so callers can degrade gracefully.
        message = str(exc)
        if 'does not exist' in message.lower() or 'not found' in message.lower():
            print(
                json.dumps(
                    {
                        'query': args.query,
                        'results': [],
                        'warning': f'collection_unavailable: {message}',
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )
            return 0
        print(f"[query_chroma_local] {message}", file=sys.stderr)
        return 1

    rows = []
    ids = (result.get('ids') or [[]])[0]
    docs = (result.get('documents') or [[]])[0]
    metas = (result.get('metadatas') or [[]])[0]
    distances = (result.get('distances') or [[]])[0]

    for idx, doc_id in enumerate(ids):
        full_document = docs[idx] if idx < len(docs) else ''
        rows.append(
            {
                'id': doc_id,
                'distance': distances[idx] if idx < len(distances) else None,
                'metadata': metas[idx] if idx < len(metas) else {},
                'document': full_document,
                'document_preview': full_document[:500],
            }
        )

    print(json.dumps({'query': args.query, 'results': rows}, indent=2, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
