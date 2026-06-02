#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import chromadb
from chromadb.utils import embedding_functions


BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = BACKEND_DIR / 'cleaned_balls_all_matches.csv'
DEFAULT_DB_DIR = BACKEND_DIR / 'chroma_db'
DEFAULT_MANIFEST = BACKEND_DIR / 'chroma_manifest.json'
DEFAULT_COLLECTION = 'cricket_semantic_index'
CLEAN_DATASET_SCRIPT = Path(__file__).with_name('build_clean_dataset.py')

CREDITED_WICKET_KINDS = {
    'bowled',
    'caught',
    'caught and bowled',
    'lbw',
    'stumped',
    'hit wicket',
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Build the local ChromaDB cricket index directly from the cleaned CSV dataset.'
    )
    parser.add_argument('--input', type=Path, default=DEFAULT_INPUT)
    parser.add_argument('--db-dir', type=Path, default=DEFAULT_DB_DIR)
    parser.add_argument('--manifest', type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument('--collection', default=DEFAULT_COLLECTION)
    parser.add_argument('--batch-size', type=int, default=500)
    parser.add_argument('--reset', action='store_true')
    parser.add_argument(
        '--skip-clean-build',
        action='store_true',
        help='Fail if the cleaned CSV is missing instead of running build_clean_dataset.py.',
    )
    return parser.parse_args()


def s_text(value: Any) -> str:
    return ' '.join(str(value or '').strip().split())


def s_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def normalize_id(value: str) -> str:
    text = ''.join(ch.lower() if ch.isalnum() else '-' for ch in s_text(value))
    return '-'.join(part for part in text.split('-') if part)


def stable_id(prefix: str, *parts: str) -> str:
    raw = '|'.join(s_text(part) for part in parts if s_text(part)) or prefix
    slug = normalize_id(raw)[:48] or prefix
    digest = hashlib.sha1(raw.encode('utf-8')).hexdigest()[:10]
    return f'{prefix}:{slug}-{digest}'


def is_legal_delivery(row: dict[str, str]) -> bool:
    return s_int(row.get('extras_wides')) == 0 and s_int(row.get('extras_noballs')) == 0


def overs_from_balls(balls: int) -> float:
    return round(max(0, balls) / 6, 2)


def batting_average(runs: int, dismissals: int) -> float:
    if dismissals <= 0:
        return float(runs) if runs else 0.0
    return round(runs / dismissals, 2)


def strike_rate(runs: int, balls: int) -> float:
    if balls <= 0:
        return 0.0
    return round((runs / balls) * 100, 2)


def economy_rate(runs: int, balls: int) -> float:
    if balls <= 0:
        return 0.0
    return round(runs / (balls / 6), 2)


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


def ensure_clean_input(input_path: Path, skip_clean_build: bool) -> None:
    if input_path.exists():
        return
    if skip_clean_build:
        raise FileNotFoundError(f'Cleaned dataset not found: {input_path}')
    if not CLEAN_DATASET_SCRIPT.exists():
        raise FileNotFoundError(f'Missing cleaner script: {CLEAN_DATASET_SCRIPT}')
    subprocess.check_call([sys.executable, str(CLEAN_DATASET_SCRIPT), '--output', str(input_path)])


def make_player_state() -> dict[str, Any]:
    return {
        'matches': set(),
        'teams': Counter(),
        'runs': 0,
        'balls': 0,
        'dismissals': 0,
        'wickets': 0,
        'bowling_balls': 0,
        'runs_conceded': 0,
        'fours': 0,
        'sixes': 0,
    }


def make_team_state() -> dict[str, Any]:
    return {
        'matches': set(),
        'wins': 0,
        'losses': 0,
        'no_result': 0,
        'runs': 0,
        'balls': 0,
    }


def make_match_state(match_id: str) -> dict[str, Any]:
    return {
        'id': match_id,
        'date': '',
        'season': '',
        'match_type': '',
        'venue': '',
        'city': '',
        'winner': '',
        'teams': set(),
        'innings': defaultdict(lambda: {'team': '', 'runs': 0, 'wickets': 0, 'balls': 0}),
    }


def read_dataset(input_path: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    players: dict[str, Any] = defaultdict(make_player_state)
    teams: dict[str, Any] = defaultdict(make_team_state)
    matches: dict[str, Any] = {}
    dates: list[str] = []

    with input_path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            match_id = s_text(row.get('match_id'))
            if not match_id:
                continue

            match = matches.setdefault(match_id, make_match_state(match_id))
            match['date'] = match['date'] or s_text(row.get('match_date'))
            match['season'] = match['season'] or s_text(row.get('season'))
            match['match_type'] = match['match_type'] or s_text(row.get('match_type'))
            match['venue'] = match['venue'] or s_text(row.get('venue'))
            match['city'] = match['city'] or s_text(row.get('city'))
            match['winner'] = match['winner'] or s_text(row.get('match_winner'))
            if match['date']:
                dates.append(match['date'])

            batting_team = s_text(row.get('batting_team'))
            if batting_team:
                match['teams'].add(batting_team)
                teams[batting_team]['runs'] += s_int(row.get('runs_total'))
                if is_legal_delivery(row):
                    teams[batting_team]['balls'] += 1

            inning_key = s_text(row.get('inning')) or batting_team or 'innings'
            inning = match['innings'][inning_key]
            inning['team'] = inning['team'] or batting_team
            inning['runs'] += s_int(row.get('runs_total'))
            inning['wickets'] += s_int(row.get('wicket_count'))
            if is_legal_delivery(row):
                inning['balls'] += 1

            batter = s_text(row.get('batter'))
            bowler = s_text(row.get('bowler'))
            runs_batter = s_int(row.get('runs_batter'))

            if batter:
                state = players[batter]
                state['matches'].add(match_id)
                if batting_team:
                    state['teams'][batting_team] += 1
                state['runs'] += runs_batter
                if is_legal_delivery(row):
                    state['balls'] += 1
                if runs_batter == 4 and s_int(row.get('non_boundary')) == 0:
                    state['fours'] += 1
                if runs_batter == 6 and s_int(row.get('non_boundary')) == 0:
                    state['sixes'] += 1

            player_out = s_text(row.get('wicket_player_out'))
            if player_out:
                players[player_out]['dismissals'] += 1

            if bowler:
                state = players[bowler]
                state['matches'].add(match_id)
                if is_legal_delivery(row):
                    state['bowling_balls'] += 1
                state['runs_conceded'] += (
                    s_int(row.get('runs_batter'))
                    + s_int(row.get('extras_wides'))
                    + s_int(row.get('extras_noballs'))
                )
                if s_text(row.get('wicket_kind')).lower() in CREDITED_WICKET_KINDS:
                    state['wickets'] += 1

    for match in matches.values():
        winner = s_text(match.get('winner'))
        for team_name in match['teams']:
            teams[team_name]['matches'].add(match['id'])
            if winner == team_name:
                teams[team_name]['wins'] += 1
            elif winner:
                teams[team_name]['losses'] += 1
            else:
                teams[team_name]['no_result'] += 1

    summary = {
        'matches': len(matches),
        'players': len(players),
        'teams': len(teams),
        'min_date': min(dates) if dates else '',
        'max_date': max(dates) if dates else '',
    }
    return players, teams, matches, summary


def player_role(state: dict[str, Any]) -> str:
    runs = int(state['runs'])
    wickets = int(state['wickets'])
    if runs >= 200 and wickets >= 20:
        return 'All-rounder'
    if wickets > runs / 25 and wickets > 0:
        return 'Bowler'
    return 'Batter'


def build_player_docs(players: dict[str, Any]) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for name, state in players.items():
        team = state['teams'].most_common(1)[0][0] if state['teams'] else ''
        matches = len(state['matches'])
        runs = int(state['runs'])
        wickets = int(state['wickets'])
        average = batting_average(runs, int(state['dismissals']))
        sr = strike_rate(runs, int(state['balls']))
        economy = economy_rate(int(state['runs_conceded']), int(state['bowling_balls']))
        role = player_role(state)
        doc_id = stable_id('player', name)
        document = (
            f'Cricket player profile {name}. Team {team or "Unknown"}. Role {role}. '
            f'Indexed matches {matches}. Runs {runs}, wickets {wickets}, batting average {average}, '
            f'strike rate {sr}, bowling economy {economy}. Fours {int(state["fours"])}, '
            f'sixes {int(state["sixes"])}.'
        )
        docs.append(
            {
                'id': doc_id,
                'document': document,
                'metadata': sanitize_metadata(
                    {
                        'doc_type': 'player_profile',
                        'player': name,
                        'team': team,
                        'role': role,
                        'matches': matches,
                        'runs': runs,
                        'wickets': wickets,
                        'average': average,
                        'strike_rate': sr,
                        'economy': economy,
                        'fours': int(state['fours']),
                        'sixes': int(state['sixes']),
                        'is_active': False,
                        'source': 'chroma_direct_csv',
                    }
                ),
            }
        )
    return docs


def build_team_docs(teams: dict[str, Any]) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for name, state in teams.items():
        matches = len(state['matches'])
        wins = int(state['wins'])
        losses = int(state['losses'])
        no_result = int(state['no_result'])
        runs = int(state['runs'])
        win_rate = round((wins / matches) * 100, 2) if matches else 0.0
        average_score = round(runs / matches, 2) if matches else 0.0
        sr = strike_rate(runs, int(state['balls']))
        document = (
            f'Cricket team summary for {name}. Indexed matches {matches}, wins {wins}, '
            f'losses {losses}, no result {no_result}, win rate {win_rate} percent. '
            f'Total batting runs {runs}, average score {average_score}, team strike rate {sr}.'
        )
        docs.append(
            {
                'id': stable_id('team', name),
                'document': document,
                'metadata': sanitize_metadata(
                    {
                        'doc_type': 'team_summary',
                        'team': name,
                        'matches': matches,
                        'wins': wins,
                        'losses': losses,
                        'no_result': no_result,
                        'win_rate': win_rate,
                        'runs': runs,
                        'average_score': average_score,
                        'strike_rate': sr,
                        'source': 'chroma_direct_csv',
                    }
                ),
            }
        )
    return docs


def build_match_docs(matches: dict[str, Any]) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for match_id, match in matches.items():
        teams = sorted(match['teams'])
        innings_summary = '; '.join(
            f'{value["team"] or key}: {value["runs"]}/{value["wickets"]} in {overs_from_balls(value["balls"])} overs'
            for key, value in match['innings'].items()
        )
        document = (
            f'Cricket match {match_id} on {match["date"] or "date unavailable"} {match["match_type"]}. '
            f'Teams: {" vs ".join(teams) or "Unknown"}. Venue: {match["venue"] or "venue unavailable"}. '
            f'Winner: {match["winner"] or "unknown"}. Innings summary: {innings_summary}.'
        )
        docs.append(
            {
                'id': f'match:{match_id}',
                'document': document,
                'metadata': sanitize_metadata(
                    {
                        'doc_type': 'match_summary',
                        'match_id': match_id,
                        'date': match['date'],
                        'season': match['season'],
                        'match_type': match['match_type'],
                        'venue': match['venue'],
                        'city': match['city'],
                        'winner': match['winner'],
                        'source': 'chroma_direct_csv',
                    }
                ),
            }
        )
    return docs


def upsert_docs(db_dir: Path, collection_name: str, docs: list[dict[str, Any]], batch_size: int, reset: bool) -> int:
    db_dir.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(db_dir.resolve()))
    if reset:
        try:
            client.delete_collection(collection_name)
        except Exception:
            pass
    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=embedding_functions.DefaultEmbeddingFunction(),
    )
    for start in range(0, len(docs), max(1, batch_size)):
        batch = docs[start:start + max(1, batch_size)]
        collection.upsert(
            ids=[doc['id'] for doc in batch],
            documents=[doc['document'] for doc in batch],
            metadatas=[doc['metadata'] for doc in batch],
        )
    return collection.count()


def main() -> int:
    args = parse_args()
    ensure_clean_input(args.input, args.skip_clean_build)
    players, teams, matches, dataset_summary = read_dataset(args.input)

    player_docs = build_player_docs(players)
    team_docs = build_team_docs(teams)
    match_docs = build_match_docs(matches)
    docs = player_docs + team_docs + match_docs
    if not docs:
        raise RuntimeError('No Chroma documents were built from the cleaned dataset.')

    collection_count = upsert_docs(args.db_dir, args.collection, docs, args.batch_size, args.reset)
    manifest = {
        'built_at': __import__('datetime').datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        'source': 'chroma_direct_csv',
        'input_csv': str(args.input.resolve()),
        'db_dir': str(args.db_dir.resolve()),
        'collection': args.collection,
        'collection_count': collection_count,
        'player_docs': len(player_docs),
        'team_docs': len(team_docs),
        'match_docs': len(match_docs),
        'dataset_summary': dataset_summary,
    }
    args.manifest.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
