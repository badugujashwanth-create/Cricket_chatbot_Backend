#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

import chromadb
from chromadb.utils import embedding_functions

BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_DIR = BACKEND_DIR / 'chroma_db'
DEFAULT_COLLECTION = 'semantic_cache'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Query and maintain the semantic cache Chroma collection.')
    parser.add_argument('command', choices=['query', 'upsert'])
    parser.add_argument('--db-dir', type=Path, default=DEFAULT_DB_DIR)
    parser.add_argument('--collection', default=DEFAULT_COLLECTION)
    parser.add_argument('--question', default='')
    parser.add_argument('--k', type=int, default=1)
    parser.add_argument('--input', type=Path, default=None)
    return parser.parse_args()


def embedding_function():
    return embedding_functions.DefaultEmbeddingFunction()


def get_collection(client, name: str):
    return client.get_or_create_collection(
        name=name,
        embedding_function=embedding_function(),
        metadata={'source': 'semantic_cache', 'mode': 'semantic_cache'},
    )


def sanitize_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            clean[key] = value
        else:
            clean[key] = json.dumps(value, ensure_ascii=False)
    return clean


def stable_cache_id(question: str) -> str:
    normalized = ' '.join(str(question or '').strip().lower().split())
    digest = hashlib.sha1(normalized.encode('utf-8')).hexdigest()[:24]
    return f'semantic-cache-{digest}'


def do_query(args: argparse.Namespace) -> int:
    question = str(args.question or '').strip()
    if not question:
        print(
            json.dumps(
                {'query': question, 'results': [], 'warning': 'empty_question'},
                ensure_ascii=False,
            )
        )
        return 0

    client = chromadb.PersistentClient(path=str(args.db_dir.resolve()))
    collection = get_collection(client, args.collection)
    result = collection.query(query_texts=[question], n_results=max(1, int(args.k or 1)))

    ids = (result.get('ids') or [[]])[0]
    docs = (result.get('documents') or [[]])[0]
    metas = (result.get('metadatas') or [[]])[0]
    distances = (result.get('distances') or [[]])[0]

    rows = []
    for index, row_id in enumerate(ids):
        rows.append(
            {
                'id': row_id,
                'distance': distances[index] if index < len(distances) else None,
                'document': docs[index] if index < len(docs) else '',
                'metadata': metas[index] if index < len(metas) else {},
            }
        )

    print(json.dumps({'query': question, 'results': rows}, ensure_ascii=False))
    return 0


def do_upsert(args: argparse.Namespace) -> int:
    if not args.input or not args.input.exists():
        print(json.dumps({'ok': False, 'error': 'missing_input'}, ensure_ascii=False))
        return 1

    payload = json.loads(args.input.read_text(encoding='utf-8'))
    question = str(payload.get('question') or '').strip()
    document_text = str(payload.get('document_text') or question).strip()
    if not question:
        print(json.dumps({'ok': False, 'error': 'missing_question'}, ensure_ascii=False))
        return 1

    client = chromadb.PersistentClient(path=str(args.db_dir.resolve()))
    collection = get_collection(client, args.collection)
    metadata = sanitize_metadata(
        {
            'doc_type': 'semantic_cache_entry',
            'question_text': question,
            'document_text': document_text,
            'answer_text': str(payload.get('answer_text') or '').strip(),
            'ui_payload_json': json.dumps(payload.get('ui_payload') or {}, ensure_ascii=False),
            'response_json': json.dumps(payload.get('response') or {}, ensure_ascii=False),
            'cached_at': int(time.time()),
        }
    )

    cache_id = str(payload.get('id') or stable_cache_id(question))
    collection.upsert(ids=[cache_id], documents=[document_text or question], metadatas=[metadata])
    print(json.dumps({'ok': True, 'id': cache_id}, ensure_ascii=False))
    return 0


def main() -> int:
    args = parse_args()
    try:
        args.db_dir.mkdir(parents=True, exist_ok=True)
        if args.command == 'query':
            return do_query(args)
        return do_upsert(args)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({'ok': False, 'error': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
