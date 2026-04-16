#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import chromadb
from chromadb.utils import embedding_functions

BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_DIR = BACKEND_DIR / 'chroma_db'
DEFAULT_COLLECTION = 'cricket_semantic_index'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Lightweight Chroma collection admin helper.')
    parser.add_argument('command', choices=['get', 'upsert'])
    parser.add_argument('--db-dir', type=Path, default=DEFAULT_DB_DIR)
    parser.add_argument('--collection', default=DEFAULT_COLLECTION)
    parser.add_argument('--where-json', default='')
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--input', type=Path, default=None)
    return parser.parse_args()


def get_collection(client, name: str):
    return client.get_or_create_collection(
        name=name,
        embedding_function=embedding_functions.DefaultEmbeddingFunction(),
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


def parse_where_json(raw: str) -> dict[str, Any] | None:
    text = str(raw or '').strip()
    if not text:
        return None
    payload = json.loads(text)
    if not isinstance(payload, dict):
        return None
    if any(str(key).startswith('$') for key in payload):
        return payload
    if len(payload) <= 1:
        return payload
    return {
        '$and': [{key: value} for key, value in payload.items()]
    }


def do_get(args: argparse.Namespace) -> int:
    client = chromadb.PersistentClient(path=str(args.db_dir.resolve()))
    collection = get_collection(client, args.collection)
    where = parse_where_json(args.where_json)
    rows = collection.get(
        where=where,
        limit=max(1, int(args.limit or 10)),
        offset=max(0, int(args.offset or 0)),
    )
    print(
        json.dumps(
            {
                'ids': rows.get('ids') or [],
                'documents': rows.get('documents') or [],
                'metadatas': rows.get('metadatas') or [],
            },
            ensure_ascii=False,
        )
    )
    return 0


def do_upsert(args: argparse.Namespace) -> int:
    if not args.input or not args.input.exists():
        print(json.dumps({'ok': False, 'error': 'missing_input'}, ensure_ascii=False))
        return 1

    payload = json.loads(args.input.read_text(encoding='utf-8'))
    docs = payload.get('documents') or []
    if not isinstance(docs, list) or not docs:
        print(json.dumps({'ok': False, 'error': 'missing_documents'}, ensure_ascii=False))
        return 1

    ids: list[str] = []
    documents: list[str] = []
    metadatas: list[dict[str, Any]] = []
    for doc in docs:
        doc_id = str(doc.get('id') or '').strip()
        document = str(doc.get('document') or '').strip()
        metadata = doc.get('metadata') or {}
        if not doc_id or not document or not isinstance(metadata, dict):
            continue
        ids.append(doc_id)
        documents.append(document)
        metadatas.append(sanitize_metadata(metadata))

    if not ids:
        print(json.dumps({'ok': False, 'error': 'no_valid_documents'}, ensure_ascii=False))
        return 1

    client = chromadb.PersistentClient(path=str(args.db_dir.resolve()))
    collection = get_collection(client, args.collection)
    collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
    print(json.dumps({'ok': True, 'count': len(ids)}, ensure_ascii=False))
    return 0


def main() -> int:
    args = parse_args()
    try:
        args.db_dir.mkdir(parents=True, exist_ok=True)
        if args.command == 'get':
            return do_get(args)
        return do_upsert(args)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({'ok': False, 'error': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
