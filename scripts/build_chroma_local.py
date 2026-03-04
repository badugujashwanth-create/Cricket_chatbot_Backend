#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import chromadb
from chromadb.utils import embedding_functions


CREDITED_WICKET_KINDS = {
    'bowled',
    'caught',
    'caught and bowled',
    'lbw',
    'stumped',
    'hit wicket',
}

BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CSV = BACKEND_DIR / 'cleaned_balls_two_folders.csv'
DEFAULT_DB_DIR = BACKEND_DIR / 'chroma_db'
DEFAULT_COLLECTION = 'cricket_semantic_index'
DEFAULT_MANIFEST = BACKEND_DIR / 'chroma_manifest.json'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Build a local ChromaDB index from the cricket ball-by-ball CSV.')
    parser.add_argument('--csv', type=Path, default=DEFAULT_CSV, help='CSV file path')
    parser.add_argument('--db-dir', type=Path, default=DEFAULT_DB_DIR, help='Chroma persistent directory')
    parser.add_argument('--collection', default=DEFAULT_COLLECTION, help='Chroma collection name')
    parser.add_argument('--manifest', type=Path, default=DEFAULT_MANIFEST, help='Build manifest output JSON')
    parser.add_argument('--reset', action='store_true', help='Delete collection before rebuilding')
    parser.add_argument('--chunk-size', type=int, default=400, help='Rows per delivery chunk document')
    parser.add_argument('--batch-size', type=int, default=64, help='Docs per Chroma upsert batch')
    parser.add_argument('--max-rows', type=int, default=None, help='Optional row limit for testing')
    parser.add_argument('--progress-every', type=int, default=100000, help='Print progress every N CSV rows')
    return parser.parse_args()


def s_int(value: Any) -> int:
    try:
        return int(str(value or '').strip() or 0)
    except Exception:
        return 0


def s_float(value: Any) -> float:
    try:
        return float(str(value or '').strip() or 0)
    except Exception:
        return 0.0


def round_num(value: float, digits: int = 2) -> float:
    return round(float(value), digits) if value else 0.0


def overs_from_balls(balls: int) -> str:
    return f"{balls // 6}.{balls % 6}"


def legal_ball(row: dict[str, str]) -> bool:
    return s_int(row.get('extras_wides')) == 0 and s_int(row.get('extras_noballs')) == 0


def normalize_name(name: str) -> str:
    return ' '.join(str(name or '').strip().split())


def infer_role(stats: dict[str, Any]) -> str:
    runs = stats.get('runs', 0) or 0
    wickets = stats.get('wickets', 0) or 0
    if runs >= 800 and wickets >= 20:
        return 'All-rounder'
    if wickets >= 20:
        return 'Bowler'
    if runs >= 300:
        return 'Batter'
    return 'Cricketer'


def infer_tags(stats: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    if (stats.get('strike_rate') or 0) >= 130:
        tags.append('Fast scoring')
    if (stats.get('batting_average') or 0) >= 35 and (stats.get('runs') or 0) >= 300:
        tags.append('Consistent batting')
    if (stats.get('wickets') or 0) >= 20:
        tags.append('Wicket-taking')
    if (stats.get('economy') or 99) <= 7 and (stats.get('wickets') or 0) >= 10:
        tags.append('Tight bowling')
    if (stats.get('fours') or 0) >= 50:
        tags.append('Boundary hitting')
    if not tags:
        tags.append('Regular contributor')
    return tags[:3]


def safe_upsert(client, collection_name: str, embedding_fn, docs_buffer: list[dict[str, Any]], batch_size: int, counters: dict[str, int]) -> None:
    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=embedding_fn,
        metadata={'source': 'cleaned_balls_two_folders.csv', 'embedding_mode': 'local_default'},
    )
    while docs_buffer:
        batch = docs_buffer[:batch_size]
        del docs_buffer[:batch_size]
        ids = [d['id'] for d in batch]
        documents = [d['document'] for d in batch]
        metadatas = [d['metadata'] for d in batch]
        try:
            collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
        except Exception as exc:  # noqa: BLE001
            if 'does not exist' not in str(exc).lower():
                raise
            # Reacquire and retry once if Chroma invalidates a long-lived collection handle.
            collection = client.get_or_create_collection(
                name=collection_name,
                embedding_function=embedding_fn,
                metadata={'source': 'cleaned_balls_two_folders.csv', 'embedding_mode': 'local_default'},
            )
            collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
        counters['docs_upserted'] += len(batch)


def top_pairs(counter_like: Counter, limit: int = 5) -> list[tuple[str, int]]:
    return sorted(counter_like.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]


def ensure_scalar_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for k, v in meta.items():
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)):
            clean[k] = v
        else:
            clean[k] = str(v)
    return clean

def new_match_agg(row: dict[str, str]) -> dict[str, Any]:
    return {
        'id': str(row.get('match_id') or '').strip(),
        'date': str(row.get('match_date') or ''),
        'season': str(row.get('season') or ''),
        'match_type': str(row.get('match_type') or ''),
        'venue': str(row.get('venue') or ''),
        'city': str(row.get('city') or ''),
        'winner': str(row.get('match_winner') or ''),
        'teams': set(),
        'innings': {},
        'batters': {},
        'bowlers': {},
    }


def new_chunk(row: dict[str, str], seq: int) -> dict[str, Any]:
    inning = s_int(row.get('inning'))
    batting_team = str(row.get('batting_team') or '').strip() or 'Unknown'
    return {
        'match_id': str(row.get('match_id') or '').strip(),
        'date': str(row.get('match_date') or ''),
        'season': str(row.get('season') or ''),
        'match_type': str(row.get('match_type') or ''),
        'venue': str(row.get('venue') or ''),
        'city': str(row.get('city') or ''),
        'inning': inning,
        'batting_team': batting_team,
        'seq': seq,
        'rows': 0,
        'runs': 0,
        'wickets': 0,
        'legal_balls': 0,
        'first_ball': '',
        'last_ball': '',
        'over_start': None,
        'over_end': None,
        'batter_runs': Counter(),
        'bowler_wkts': Counter(),
        'bowler_runs': Counter(),
        'events': [],
    }


def update_chunk(chunk: dict[str, Any], row: dict[str, str]) -> None:
    runs_batter = s_int(row.get('runs_batter'))
    runs_total = s_int(row.get('runs_total'))
    wicket_count = s_int(row.get('wicket_count'))
    ball_id = str(row.get('ball_id') or '')
    over_num = s_float(row.get('over'))
    legal = legal_ball(row)
    batter = normalize_name(row.get('batter') or '')
    bowler = normalize_name(row.get('bowler') or '')

    chunk['rows'] += 1
    chunk['runs'] += runs_total
    chunk['wickets'] += wicket_count
    if legal:
        chunk['legal_balls'] += 1

    if not chunk['first_ball']:
        chunk['first_ball'] = ball_id
    chunk['last_ball'] = ball_id
    if chunk['over_start'] is None:
        chunk['over_start'] = over_num
    chunk['over_end'] = over_num

    if batter:
        chunk['batter_runs'][batter] += runs_batter
    if bowler:
        wides = s_int(row.get('extras_wides'))
        no_balls = s_int(row.get('extras_noballs'))
        chunk['bowler_runs'][bowler] += runs_batter + wides + no_balls
        wicket_kind = str(row.get('wicket_kind') or '').strip().lower()
        if wicket_count > 0 and CREDITED_WICKET_KINDS.__contains__(wicket_kind):
            chunk['bowler_wkts'][bowler] += 1

    if len(chunk['events']) < 20:
        wicket_player = normalize_name(row.get('wicket_player_out') or '')
        wicket_kind = str(row.get('wicket_kind') or '').strip()
        if wicket_count > 0 and wicket_player:
            chunk['events'].append(f"{ball_id}: WICKET {wicket_player} ({wicket_kind or 'out'}) by {bowler or 'unknown bowler'}")
        elif runs_batter >= 4:
            chunk['events'].append(f"{ball_id}: {batter or 'batter'} scored {runs_batter} off {bowler or 'bowler'}")


def chunk_doc(chunk: dict[str, Any]) -> dict[str, Any]:
    top_batters = ', '.join(f"{name} {runs}" for name, runs in top_pairs(chunk['batter_runs'], 4)) or 'No batting data'
    top_bowlers = ', '.join(
        f"{name} {chunk['bowler_wkts'].get(name, 0)}/{chunk['bowler_runs'].get(name, 0)}"
        for name, _ in top_pairs(chunk['bowler_runs'], 4)
    ) or 'No bowling data'
    events = '; '.join(chunk['events']) or 'No major events in this chunk'
    over_range = f"{chunk['over_start']} to {chunk['over_end']}" if chunk['over_start'] is not None else 'unknown'
    text = (
        f"Cricket delivery chunk from match {chunk['match_id']} on {chunk['date']} ({chunk['match_type']}) at {chunk['venue']}, {chunk['city']}. "
        f"Inning {chunk['inning']} batting team {chunk['batting_team']}. "
        f"Chunk {chunk['seq']} covers rows {chunk['rows']} and balls {chunk['first_ball']} to {chunk['last_ball']} over range {over_range}. "
        f"Runs in chunk {chunk['runs']}, wickets {chunk['wickets']}, legal balls {chunk['legal_balls']}. "
        f"Top batters: {top_batters}. Top bowlers: {top_bowlers}. Key events: {events}."
    )
    return {
        'id': f"delivery:{chunk['match_id']}:{chunk['inning']}:{chunk['seq']}",
        'document': text,
        'metadata': ensure_scalar_metadata(
            {
                'doc_type': 'delivery_chunk',
                'match_id': chunk['match_id'],
                'date': chunk['date'],
                'match_type': chunk['match_type'],
                'inning': int(chunk['inning']),
                'batting_team': chunk['batting_team'],
                'venue': chunk['venue'],
                'rows': int(chunk['rows']),
                'runs': int(chunk['runs']),
                'wickets': int(chunk['wickets']),
                'seq': int(chunk['seq']),
            }
        ),
    }


def finalize_and_add_aggregate_docs(players: dict[str, Any], teams: dict[str, Any], matches: dict[str, Any], docs: list[dict[str, Any]], counters: dict[str, int]) -> None:
    finalized_matches: list[dict[str, Any]] = []
    for match in matches.values():
        innings_list = sorted(match['innings'].values(), key=lambda x: x['inning'])
        top_batters = sorted(match['batters'].values(), key=lambda x: (-x['runs'], x['balls'], x['name']))[:4]
        top_bowlers = sorted(match['bowlers'].values(), key=lambda x: (-x['wickets'], x['runs_conceded'], x['name']))[:4]
        teams_list = sorted([t for t in match['teams'] if t])
        innings_summary = ' | '.join(
            [f"{inn['batting_team']} {inn['runs']}/{inn['wickets']} in {overs_from_balls(inn['balls'])} overs" for inn in innings_list]
        )
        top_batters_text = ', '.join([f"{b['name']} {b['runs']}({b['balls']})" for b in top_batters]) or 'n/a'
        top_bowlers_text = ', '.join([f"{b['name']} {b['wickets']}/{b['runs_conceded']}" for b in top_bowlers]) or 'n/a'
        match_doc_text = (
            f"Cricket match {match['id']} on {match['date']} season {match['season']} {match['match_type']} at {match['venue']} {match['city']}. "
            f"Teams: {' vs '.join(teams_list)}. Winner: {match['winner'] or 'unknown'}. "
            f"Innings summary: {innings_summary}. "
            f"Top batters: {top_batters_text}. "
            f"Top bowlers: {top_bowlers_text}."
        )
        docs.append(
            {
                'id': f"match:{match['id']}",
                'document': match_doc_text,
                'metadata': ensure_scalar_metadata(
                    {
                        'doc_type': 'match_summary',
                        'match_id': match['id'],
                        'date': match['date'],
                        'season': match['season'],
                        'match_type': match['match_type'],
                        'venue': match['venue'],
                        'city': match['city'],
                        'winner': match['winner'] or '',
                    }
                ),
            }
        )
        finalized_matches.append({'id': match['id'], 'date': match['date'], 'teams': teams_list, 'winner': match['winner'] or ''})
        counters['match_docs'] += 1

    team_win_map: dict[str, dict[str, int]] = {team_name: {'wins': 0, 'losses': 0, 'no_result': 0} for team_name in teams.keys()}
    for match in finalized_matches:
        for team_name in match['teams']:
            if team_name not in team_win_map:
                team_win_map[team_name] = {'wins': 0, 'losses': 0, 'no_result': 0}
            if match['winner']:
                if match['winner'] == team_name:
                    team_win_map[team_name]['wins'] += 1
                else:
                    team_win_map[team_name]['losses'] += 1
            else:
                team_win_map[team_name]['no_result'] += 1

    for team in teams.values():
        mcount = len(team['matches'])
        wins = team_win_map.get(team['name'], {}).get('wins', 0)
        losses = team_win_map.get(team['name'], {}).get('losses', 0)
        no_result = team_win_map.get(team['name'], {}).get('no_result', 0)
        win_rate = round_num((wins * 100 / mcount) if mcount else 0, 1)
        top_venues_text = ', '.join([f"{v} ({c})" for v, c in top_pairs(team['venues'], 5)]) or 'n/a'
        team_text = (
            f"Cricket team summary for {team['name']}. Indexed matches {mcount}, wins {wins}, losses {losses}, no result {no_result}, win rate {win_rate} percent. "
            f"Total batting runs {team['runs']}, wickets lost {team['wickets_lost']}, team strike rate {round_num((team['runs'] * 100 / team['legal_balls']) if team['legal_balls'] else 0, 1)}. "
            f"Top venues: {top_venues_text}."
        )
        docs.append(
            {
                'id': f"team:{team['slug']}",
                'document': team_text,
                'metadata': ensure_scalar_metadata({'doc_type': 'team_summary', 'team': team['name'], 'matches': mcount, 'wins': wins, 'win_rate': win_rate}),
            }
        )
        counters['team_docs'] += 1

    for player in players.values():
        match_count = len(player['matches'])
        recent_scores = sorted(player['recent_by_match'].values(), key=lambda x: (x['date'], x['match_id']), reverse=True)[:5]
        runs = player['batting_runs']
        wickets = player['bowling_wickets']
        batting_avg = round_num((runs / player['dismissals']) if player['dismissals'] else (runs or 0), 2)
        strike_rate = round_num((runs * 100 / player['batting_balls']) if player['batting_balls'] else 0, 2)
        economy = round_num((player['bowling_runs'] * 6 / player['bowling_balls']) if player['bowling_balls'] else 0, 2)
        stats = {
            'matches': match_count,
            'runs': runs,
            'wickets': wickets,
            'batting_average': batting_avg,
            'strike_rate': strike_rate,
            'economy': economy,
            'fours': player['fours'],
            'sixes': player['sixes'],
            'dot_balls': player['dot_balls'],
        }
        role = infer_role({'runs': runs, 'wickets': wickets, 'economy': economy})
        tags = infer_tags(stats)
        team_name = max(player['teams'].items(), key=lambda kv: kv[1])[0] if player['teams'] else 'Unknown'
        player_text = (
            f"Cricket player profile {player['name']}. Team {team_name}. Role {role}. Indexed matches {match_count}. "
            f"Runs {runs}, wickets {wickets}, batting average {batting_avg}, strike rate {strike_rate}, bowling economy {economy}. "
            f"Fours {player['fours']}, sixes {player['sixes']}. Recent batting scores: {', '.join(str(x['runs']) for x in recent_scores) or 'n/a'}. "
            f"Tags: {', '.join(tags)}."
        )
        docs.append(
            {
                'id': f"player:{player['slug']}",
                'document': player_text,
                'metadata': ensure_scalar_metadata(
                    {
                        'doc_type': 'player_profile',
                        'player': player['name'],
                        'team': team_name,
                        'role': role,
                        'matches': match_count,
                        'runs': runs,
                        'wickets': wickets,
                        'strike_rate': strike_rate,
                        'economy': economy,
                    }
                ),
            }
        )
        counters['player_docs'] += 1

def main() -> int:
    args = parse_args()
    csv_path = args.csv.resolve()
    if not csv_path.exists():
        print(f"[error] CSV not found: {csv_path}", file=sys.stderr)
        return 1

    db_dir = args.db_dir.resolve()
    db_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    counters = {
        'rows_processed': 0,
        'delivery_chunk_docs': 0,
        'player_docs': 0,
        'team_docs': 0,
        'match_docs': 0,
        'docs_upserted': 0,
    }
    docs_buffer: list[dict[str, Any]] = []

    players: dict[str, dict[str, Any]] = {}
    teams: dict[str, dict[str, Any]] = {}
    matches: dict[str, dict[str, Any]] = {}
    seasons: set[str] = set()
    venues: set[str] = set()
    min_date = ''
    max_date = ''

    chunk = None
    chunk_key = None
    chunk_seq_by_innings: dict[tuple[str, int], int] = {}

    with csv_path.open('r', encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            counters['rows_processed'] += 1
            if args.max_rows and counters['rows_processed'] > args.max_rows:
                break

            match_id = str(row.get('match_id') or '').strip()
            if not match_id:
                continue
            inning = s_int(row.get('inning'))
            batting_team = normalize_name(row.get('batting_team') or '') or 'Unknown'
            key = (match_id, inning, batting_team)

            if chunk is None or chunk_key != key or chunk['rows'] >= args.chunk_size:
                if chunk is not None and chunk['rows']:
                    docs_buffer.append(chunk_doc(chunk))
                    counters['delivery_chunk_docs'] += 1
                    # Keep docs in memory during CSV parse; upsert in a shorter final phase for reliability.
                if chunk_key != key or chunk is None or chunk['rows'] >= args.chunk_size:
                    seq_key = (match_id, inning)
                    chunk_seq_by_innings[seq_key] = chunk_seq_by_innings.get(seq_key, 0) + 1
                    chunk = new_chunk(row, chunk_seq_by_innings[seq_key])
                    chunk_key = key

            update_chunk(chunk, row)

            date = str(row.get('match_date') or '')
            if date:
                if not min_date or date < min_date:
                    min_date = date
                if not max_date or date > max_date:
                    max_date = date
            season = str(row.get('season') or '')
            if season:
                seasons.add(season)
            venue = str(row.get('venue') or '')
            if venue:
                venues.add(venue)

            match = matches.get(match_id)
            if match is None:
                match = new_match_agg(row)
                matches[match_id] = match
            match['winner'] = str(row.get('match_winner') or '') or match['winner']
            if batting_team:
                match['teams'].add(batting_team)

            runs_batter = s_int(row.get('runs_batter'))
            runs_total = s_int(row.get('runs_total'))
            wicket_count = s_int(row.get('wicket_count'))
            legal = legal_ball(row)
            wides = s_int(row.get('extras_wides'))
            no_balls = s_int(row.get('extras_noballs'))
            byes = s_int(row.get('extras_byes'))
            leg_byes = s_int(row.get('extras_legbyes'))
            wicket_kind = str(row.get('wicket_kind') or '').strip().lower()
            wicket_player = normalize_name(row.get('wicket_player_out') or '')

            inning_agg = match['innings'].get(inning)
            if inning_agg is None:
                inning_agg = {'inning': inning, 'batting_team': batting_team, 'runs': 0, 'wickets': 0, 'balls': 0}
                match['innings'][inning] = inning_agg
            inning_agg['batting_team'] = batting_team
            inning_agg['runs'] += runs_total
            inning_agg['wickets'] += wicket_count
            if legal:
                inning_agg['balls'] += 1

            team = teams.get(batting_team)
            if team is None:
                team = {
                    'name': batting_team,
                    'slug': '-'.join(batting_team.lower().split()) or batting_team.lower(),
                    'matches': set(),
                    'seasons': set(),
                    'venues': Counter(),
                    'runs': 0,
                    'legal_balls': 0,
                    'wickets_lost': 0,
                }
                teams[batting_team] = team
            team['matches'].add(match_id)
            if season:
                team['seasons'].add(season)
            if venue:
                team['venues'][venue] += 1
            team['runs'] += runs_total
            team['wickets_lost'] += wicket_count
            if legal:
                team['legal_balls'] += 1

            batter = normalize_name(row.get('batter') or '')
            if batter:
                p = players.get(batter)
                if p is None:
                    p = {
                        'name': batter,
                        'slug': '-'.join(batter.lower().split()),
                        'matches': set(),
                        'teams': Counter(),
                        'batting_runs': 0,
                        'batting_balls': 0,
                        'dismissals': 0,
                        'fours': 0,
                        'sixes': 0,
                        'bowling_runs': 0,
                        'bowling_balls': 0,
                        'bowling_wickets': 0,
                        'dot_balls': 0,
                        'recent_by_match': {},
                    }
                    players[batter] = p
                p['matches'].add(match_id)
                p['teams'][batting_team] += 1
                p['batting_runs'] += runs_batter
                if legal:
                    p['batting_balls'] += 1
                if wicket_player and wicket_player == batter:
                    p['dismissals'] += 1
                non_boundary = s_int(row.get('non_boundary'))
                if runs_batter == 4 and non_boundary != 1:
                    p['fours'] += 1
                if runs_batter == 6:
                    p['sixes'] += 1
                prev = p['recent_by_match'].get(match_id) or {'match_id': match_id, 'date': date, 'team': batting_team, 'runs': 0, 'balls': 0}
                prev['date'] = date or prev['date']
                prev['team'] = batting_team or prev['team']
                prev['runs'] += runs_batter
                if legal:
                    prev['balls'] += 1
                p['recent_by_match'][match_id] = prev

                mb = match['batters'].get(batter)
                if mb is None:
                    mb = {'name': batter, 'team': batting_team, 'runs': 0, 'balls': 0}
                    match['batters'][batter] = mb
                mb['team'] = batting_team
                mb['runs'] += runs_batter
                if legal:
                    mb['balls'] += 1

            bowler = normalize_name(row.get('bowler') or '')
            if bowler:
                p = players.get(bowler)
                if p is None:
                    p = {
                        'name': bowler,
                        'slug': '-'.join(bowler.lower().split()),
                        'matches': set(),
                        'teams': Counter(),
                        'batting_runs': 0,
                        'batting_balls': 0,
                        'dismissals': 0,
                        'fours': 0,
                        'sixes': 0,
                        'bowling_runs': 0,
                        'bowling_balls': 0,
                        'bowling_wickets': 0,
                        'dot_balls': 0,
                        'recent_by_match': {},
                    }
                    players[bowler] = p
                p['matches'].add(match_id)
                bowler_runs_conceded = runs_batter + wides + no_balls
                p['bowling_runs'] += bowler_runs_conceded
                if legal:
                    p['bowling_balls'] += 1
                if legal and (bowler_runs_conceded + byes + leg_byes) == 0:
                    p['dot_balls'] += 1
                if wicket_count > 0 and wicket_player and wicket_kind in CREDITED_WICKET_KINDS:
                    p['bowling_wickets'] += 1

                mbo = match['bowlers'].get(bowler)
                if mbo is None:
                    mbo = {'name': bowler, 'wickets': 0, 'runs_conceded': 0, 'balls': 0}
                    match['bowlers'][bowler] = mbo
                mbo['runs_conceded'] += bowler_runs_conceded
                if legal:
                    mbo['balls'] += 1
                if wicket_count > 0 and wicket_player and wicket_kind in CREDITED_WICKET_KINDS:
                    mbo['wickets'] += 1

            if counters['rows_processed'] % max(1, args.progress_every) == 0:
                elapsed = max(time.time() - started, 0.001)
                print(
                    f"[progress] rows={counters['rows_processed']:,} chunks={counters['delivery_chunk_docs']:,} pending_docs={len(docs_buffer):,} rate={counters['rows_processed']/elapsed:,.0f} rows/s"
                )

    if chunk is not None and chunk['rows']:
        docs_buffer.append(chunk_doc(chunk))
        counters['delivery_chunk_docs'] += 1

    finalize_and_add_aggregate_docs(players, teams, matches, docs_buffer, counters)

    print(f"[info] generated {len(docs_buffer):,} docs, starting Chroma vector insertion...")
    vector_started = time.time()
    client = chromadb.PersistentClient(path=str(db_dir))
    embedding_fn = embedding_functions.DefaultEmbeddingFunction()
    if args.reset:
        try:
            client.delete_collection(args.collection)
            print(f"[info] deleted collection {args.collection}")
        except Exception:
            pass
    safe_upsert(client, args.collection, embedding_fn, docs_buffer, args.batch_size, counters)
    vector_elapsed = time.time() - vector_started
    print(f"[info] vector insertion finished in {vector_elapsed:.1f}s")

    elapsed = max(time.time() - started, 0.001)
    manifest = {
        'built_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'csv_path': str(csv_path),
        'db_dir': str(db_dir),
        'collection': args.collection,
        'rows_processed': counters['rows_processed'],
        'delivery_chunk_docs': counters['delivery_chunk_docs'],
        'player_docs': counters['player_docs'],
        'team_docs': counters['team_docs'],
        'match_docs': counters['match_docs'],
        'docs_upserted': counters['docs_upserted'],
        'collection_count': client.get_collection(args.collection, embedding_function=embedding_fn).count(),
        'elapsed_seconds': round(elapsed, 2),
        'dataset_summary': {
            'matches': len(matches),
            'players': len(players),
            'teams': len(teams),
            'seasons': len(seasons),
            'venues': len(venues),
            'min_date': min_date,
            'max_date': max_date,
        },
    }
    args.manifest.write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    print('[done]')
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
