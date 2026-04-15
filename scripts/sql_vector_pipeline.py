#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import chromadb
from chromadb.utils import embedding_functions


csv.field_size_limit(sys.maxsize)


BACKEND_DIR = Path(__file__).resolve().parents[1]
ROOT_DIR = Path(__file__).resolve().parents[2]
BUILD_CLEAN_SCRIPT = BACKEND_DIR / 'scripts' / 'build_clean_dataset.py'
DEFAULT_SOURCE_DIR = ROOT_DIR / 'Datasets'
DEFAULT_CSV = BACKEND_DIR / 'cleaned_balls_all_matches.csv'
DEFAULT_DATASET_MANIFEST = BACKEND_DIR / 'cleaned_dataset_manifest.json'
DEFAULT_SQLITE_DB = BACKEND_DIR / 'cricket_archive.sqlite3'
DEFAULT_SQLITE_MANIFEST = BACKEND_DIR / 'sqlite_manifest.json'
DEFAULT_CHROMA_DB_DIR = BACKEND_DIR / 'chroma_db'
DEFAULT_COLLECTION = 'cricket_semantic_index'
DEFAULT_CHROMA_MANIFEST = BACKEND_DIR / 'chroma_manifest.json'

CREDITED_WICKET_KINDS = {
    'bowled',
    'caught',
    'caught and bowled',
    'lbw',
    'stumped',
    'hit wicket',
}


class ProgressMonitor:
    def __init__(self, stall_seconds: int, heartbeat_seconds: int) -> None:
        now = time.monotonic()
        self.stall_seconds = max(1, stall_seconds)
        self.heartbeat_seconds = max(5, heartbeat_seconds)
        self.last_progress = now
        self.last_heartbeat = now
        self.last_label = 'starting'

    def touch(self, label: str) -> None:
        self.last_progress = time.monotonic()
        self.last_label = label

    def check(self, phase: str) -> None:
        now = time.monotonic()
        if now - self.last_progress > self.stall_seconds:
            raise RuntimeError(
                f'{phase} stalled for more than {self.stall_seconds} seconds while processing {self.last_label}'
            )
        if now - self.last_heartbeat >= self.heartbeat_seconds:
            print(f'[heartbeat] phase={phase} last_item={self.last_label}')
            self.last_heartbeat = now


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='SQLite-first ETL pipeline for cricket archive and Chroma semantic profiles.'
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    rebuild = subparsers.add_parser(
        'rebuild',
        help='Build cleaned CSV if needed, rebuild SQLite archive, and rebuild Chroma documents.',
    )
    rebuild.add_argument('--source-dir', type=Path, default=DEFAULT_SOURCE_DIR)
    rebuild.add_argument('--csv', type=Path, default=DEFAULT_CSV)
    rebuild.add_argument('--dataset-manifest', type=Path, default=DEFAULT_DATASET_MANIFEST)
    rebuild.add_argument('--sqlite-db', type=Path, default=DEFAULT_SQLITE_DB)
    rebuild.add_argument('--sqlite-manifest', type=Path, default=DEFAULT_SQLITE_MANIFEST)
    rebuild.add_argument('--db-dir', type=Path, default=DEFAULT_CHROMA_DB_DIR)
    rebuild.add_argument('--collection', default=DEFAULT_COLLECTION)
    rebuild.add_argument('--manifest', type=Path, default=DEFAULT_CHROMA_MANIFEST)
    rebuild.add_argument('--skip-clean', action='store_true')
    rebuild.add_argument('--reset', action='store_true', help='Accepted for compatibility.')
    rebuild.add_argument('--batch-size', type=int, default=64)
    rebuild.add_argument('--max-files', type=int, default=None)
    rebuild.add_argument('--max-rows', type=int, default=None)
    rebuild.add_argument('--progress-every', type=int, default=100000)
    rebuild.add_argument('--stall-seconds', type=int, default=300)
    rebuild.add_argument('--heartbeat-seconds', type=int, default=30)

    ingest = subparsers.add_parser(
        'ingest-live-match',
        help='Upsert one completed live match into SQLite and refresh affected Chroma docs.',
    )
    ingest.add_argument('--input', type=Path, required=True)
    ingest.add_argument('--sqlite-db', type=Path, default=DEFAULT_SQLITE_DB)
    ingest.add_argument('--sqlite-manifest', type=Path, default=DEFAULT_SQLITE_MANIFEST)
    ingest.add_argument('--db-dir', type=Path, default=DEFAULT_CHROMA_DB_DIR)
    ingest.add_argument('--collection', default=DEFAULT_COLLECTION)
    ingest.add_argument('--manifest', type=Path, default=DEFAULT_CHROMA_MANIFEST)
    ingest.add_argument('--batch-size', type=int, default=32)
    return parser.parse_args()


def s_int(value: Any) -> int:
    try:
        return int(str(value or '').strip() or 0)
    except Exception:
        try:
            return int(float(str(value or '').strip() or 0))
        except Exception:
            return 0


def s_float(value: Any) -> float:
    try:
        return float(str(value or '').strip() or 0)
    except Exception:
        return 0.0


def s_text(value: Any) -> str:
    return ' '.join(str(value or '').strip().split())


def round_num(value: float, digits: int = 2) -> float:
    return round(float(value), digits) if value else 0.0


def normalize_name(name: str) -> str:
    return ' '.join(str(name or '').strip().split())


def normalize_key(value: str) -> str:
    return normalize_name(value).lower()


def legal_ball(row: dict[str, Any]) -> bool:
    return s_int(row.get('extras_wides')) == 0 and s_int(row.get('extras_noballs')) == 0


def overs_from_balls(balls: int) -> str:
    return f'{balls // 6}.{balls % 6}'


def balls_from_overs(value: Any) -> int:
    text = str(value or '').strip()
    if not text:
        return 0
    if '.' in text:
        whole, fraction = text.split('.', 1)
        return s_int(whole) * 6 + s_int(fraction[:1])
    return s_int(text) * 6


def stable_suffix(value: str) -> str:
    return hashlib.sha1(str(value or '').encode('utf-8')).hexdigest()[:10]


def slugify(value: str) -> str:
    clean = ''.join(ch.lower() if ch.isalnum() else '-' for ch in str(value or '').strip())
    return '-'.join(part for part in clean.split('-') if part) or 'unknown'


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


def top_pairs(counter_like: Counter, limit: int = 5) -> list[tuple[str, int]]:
    return sorted(counter_like.items(), key=lambda item: (-item[1], item[0]))[:limit]


def cleanup_path(target: Path) -> None:
    if not target.exists():
        return
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()


def build_temp_path(target: Path) -> Path:
    stamp = time.strftime('%Y%m%d%H%M%S', time.gmtime())
    return target.parent / f'{target.name}.tmp-{stamp}'


def ensure_scalar_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in meta.items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            clean[key] = value
        else:
            clean[key] = json.dumps(value, ensure_ascii=False)
    return clean


def ensure_unique_doc_ids(docs: list[dict[str, Any]]) -> None:
    seen: set[str] = set()
    duplicates: list[str] = []
    for doc in docs:
        doc_id = str(doc.get('id') or '').strip()
        if not doc_id:
            raise RuntimeError('encountered a document without an id')
        if doc_id in seen and doc_id not in duplicates:
            duplicates.append(doc_id)
        seen.add(doc_id)
    if duplicates:
        raise RuntimeError(f'duplicate document ids found before upsert: {", ".join(duplicates[:10])}')


def read_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding='utf-8-sig'))


def run_clean_dataset(args: argparse.Namespace) -> dict[str, Any] | None:
    csv_path = args.csv.resolve()
    manifest_path = args.dataset_manifest.resolve()
    if args.skip_clean:
        if not csv_path.exists():
            raise RuntimeError(f'cleaned CSV not found: {csv_path}')
        if manifest_path.exists():
            try:
                return read_json_file(manifest_path)
            except Exception:
                return None
        return None

    command = [
        sys.executable,
        str(BUILD_CLEAN_SCRIPT),
        '--source-dir',
        str(args.source_dir.resolve()),
        '--output',
        str(csv_path),
        '--manifest',
        str(manifest_path),
        '--progress-every',
        str(max(1, int(args.progress_every))),
        '--stall-seconds',
        str(max(1, int(args.stall_seconds))),
        '--heartbeat-seconds',
        str(max(5, int(args.heartbeat_seconds))),
    ]
    if args.max_files:
        command.extend(['--max-files', str(max(1, int(args.max_files)))])

    subprocess.run(command, cwd=str(BACKEND_DIR), check=True)
    if manifest_path.exists():
        try:
            return read_json_file(manifest_path)
        except Exception:
            return None
    return None


def new_match_agg(row: dict[str, str]) -> dict[str, Any]:
    return {
        'id': str(row.get('match_id') or '').strip(),
        'date': str(row.get('match_date') or ''),
        'season': str(row.get('season') or ''),
        'match_type': str(row.get('match_type') or ''),
        'gender': str(row.get('gender') or ''),
        'team_type': str(row.get('team_type') or ''),
        'venue': str(row.get('venue') or ''),
        'city': str(row.get('city') or ''),
        'winner': str(row.get('match_winner') or ''),
        'toss_winner': str(row.get('toss_winner') or ''),
        'toss_decision': str(row.get('toss_decision') or ''),
        'source_folder': str(row.get('source_folder') or ''),
        'data_version': str(row.get('data_version') or ''),
        'source_kind': 'dataset',
        'status': str(row.get('match_winner') or ''),
        'narrative': '',
        'teams': set(),
        'innings': {},
        'batters': {},
        'bowlers': {},
    }


def new_player_match_row(match: dict[str, Any], player_name: str, team: str = '') -> dict[str, Any]:
    return {
        'match_id': match['id'],
        'player_name': player_name,
        'team': team,
        'runs': 0,
        'balls': 0,
        'wickets': 0,
        'bowling_runs': 0,
        'bowling_balls': 0,
        'dismissals': 0,
        'fours': 0,
        'sixes': 0,
        'dot_balls': 0,
        'match_date': match['date'],
        'season': match['season'],
        'match_type': match['match_type'],
    }


def new_team_match_row(match: dict[str, Any], team_name: str) -> dict[str, Any]:
    return {
        'match_id': match['id'],
        'team_name': team_name,
        'season': match['season'],
        'venue': match['venue'],
        'runs': 0,
        'wickets_lost': 0,
        'legal_balls': 0,
    }


def aggregate_csv_archive(
    csv_path: Path,
    *,
    max_rows: int | None = None,
    progress_every: int = 100000,
    stall_seconds: int = 300,
    heartbeat_seconds: int = 30,
) -> dict[str, Any]:
    if not csv_path.exists():
        raise RuntimeError(f'CSV not found: {csv_path}')

    matches: dict[str, dict[str, Any]] = {}
    player_match_rows: dict[tuple[str, str], dict[str, Any]] = {}
    team_match_rows: dict[tuple[str, str], dict[str, Any]] = {}
    seasons: set[str] = set()
    venues: set[str] = set()
    min_date = ''
    max_date = ''
    rows_processed = 0
    monitor = ProgressMonitor(stall_seconds, heartbeat_seconds)
    started = time.time()

    with csv_path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows_processed += 1
            if max_rows and rows_processed > max_rows:
                break

            monitor.check('csv-parse')
            if rows_processed % 1000 == 0:
                monitor.touch(f'row {rows_processed}')

            match_id = str(row.get('match_id') or '').strip()
            if not match_id:
                continue

            match = matches.get(match_id)
            if match is None:
                match = new_match_agg(row)
                matches[match_id] = match

            date = str(row.get('match_date') or '')
            if date:
                min_date = date if not min_date or date < min_date else min_date
                max_date = date if not max_date or date > max_date else max_date
            season = str(row.get('season') or '')
            if season:
                seasons.add(season)
            venue = str(row.get('venue') or '')
            if venue:
                venues.add(venue)

            inning = s_int(row.get('inning'))
            batting_team = normalize_name(row.get('batting_team') or '') or 'Unknown'
            batter = normalize_name(row.get('batter') or '')
            bowler = normalize_name(row.get('bowler') or '')
            wicket_player = normalize_name(row.get('wicket_player_out') or '')
            wicket_count = s_int(row.get('wicket_count'))
            wicket_kind = str(row.get('wicket_kind') or '').strip().lower()
            runs_batter = s_int(row.get('runs_batter'))
            runs_total = s_int(row.get('runs_total'))
            wides = s_int(row.get('extras_wides'))
            no_balls = s_int(row.get('extras_noballs'))
            byes = s_int(row.get('extras_byes'))
            leg_byes = s_int(row.get('extras_legbyes'))
            non_boundary = s_int(row.get('non_boundary'))
            is_legal = legal_ball(row)

            match['winner'] = str(row.get('match_winner') or '') or match['winner']
            match['toss_winner'] = str(row.get('toss_winner') or '') or match['toss_winner']
            match['toss_decision'] = str(row.get('toss_decision') or '') or match['toss_decision']
            match['status'] = f"{match['winner']} won" if match['winner'] else match['status']
            if batting_team:
                match['teams'].add(batting_team)

            inning_agg = match['innings'].get(inning)
            if inning_agg is None:
                inning_agg = {'inning': inning, 'batting_team': batting_team, 'runs': 0, 'wickets': 0, 'balls': 0}
                match['innings'][inning] = inning_agg
            inning_agg['batting_team'] = batting_team
            inning_agg['runs'] += runs_total
            inning_agg['wickets'] += wicket_count
            if is_legal:
                inning_agg['balls'] += 1

            team_key = (match_id, batting_team)
            team_row = team_match_rows.get(team_key)
            if team_row is None:
                team_row = new_team_match_row(match, batting_team)
                team_match_rows[team_key] = team_row
            team_row['season'] = season or team_row['season']
            team_row['venue'] = venue or team_row['venue']
            team_row['runs'] += runs_total
            team_row['wickets_lost'] += wicket_count
            if is_legal:
                team_row['legal_balls'] += 1

            if batter:
                player_key = (match_id, batter)
                player_row = player_match_rows.get(player_key)
                if player_row is None:
                    player_row = new_player_match_row(match, batter, batting_team)
                    player_match_rows[player_key] = player_row
                player_row['team'] = batting_team or player_row['team']
                player_row['runs'] += runs_batter
                if is_legal:
                    player_row['balls'] += 1
                if wicket_player and wicket_player == batter:
                    player_row['dismissals'] += 1
                if runs_batter == 4 and non_boundary != 1:
                    player_row['fours'] += 1
                if runs_batter == 6:
                    player_row['sixes'] += 1

                match_batter = match['batters'].get(batter)
                if match_batter is None:
                    match_batter = {'name': batter, 'team': batting_team, 'runs': 0, 'balls': 0}
                    match['batters'][batter] = match_batter
                match_batter['team'] = batting_team
                match_batter['runs'] += runs_batter
                if is_legal:
                    match_batter['balls'] += 1

            if bowler:
                player_key = (match_id, bowler)
                player_row = player_match_rows.get(player_key)
                if player_row is None:
                    player_row = new_player_match_row(match, bowler)
                    player_match_rows[player_key] = player_row
                bowler_runs_conceded = runs_batter + wides + no_balls
                player_row['bowling_runs'] += bowler_runs_conceded
                if is_legal:
                    player_row['bowling_balls'] += 1
                if is_legal and (bowler_runs_conceded + byes + leg_byes) == 0:
                    player_row['dot_balls'] += 1
                if wicket_count > 0 and wicket_player and wicket_kind in CREDITED_WICKET_KINDS:
                    player_row['wickets'] += 1

                match_bowler = match['bowlers'].get(bowler)
                if match_bowler is None:
                    match_bowler = {'name': bowler, 'wickets': 0, 'runs_conceded': 0, 'balls': 0}
                    match['bowlers'][bowler] = match_bowler
                match_bowler['runs_conceded'] += bowler_runs_conceded
                if is_legal:
                    match_bowler['balls'] += 1
                if wicket_count > 0 and wicket_player and wicket_kind in CREDITED_WICKET_KINDS:
                    match_bowler['wickets'] += 1

            if rows_processed % max(1, progress_every) == 0:
                elapsed = max(time.time() - started, 0.001)
                print(f'[sql-load-progress] rows={rows_processed:,} matches={len(matches):,} rate={rows_processed/elapsed:,.0f} rows/s')

    for match in matches.values():
        innings_list = sorted(match['innings'].values(), key=lambda item: item['inning'])
        teams_list = sorted([team for team in match['teams'] if team])
        match['team1'] = teams_list[0] if teams_list else ''
        match['team2'] = teams_list[1] if len(teams_list) > 1 else ''
        match['teams_json'] = json.dumps(teams_list, ensure_ascii=False)
        match['innings_summary'] = ' | '.join(
            f"{inning['batting_team']} {inning['runs']}/{inning['wickets']} in {overs_from_balls(inning['balls'])} overs"
            for inning in innings_list
        )
        top_batters = sorted(
            match['batters'].values(),
            key=lambda item: (-item['runs'], item['balls'], item['name']),
        )[:4]
        top_bowlers = sorted(
            match['bowlers'].values(),
            key=lambda item: (-item['wickets'], item['runs_conceded'], item['name']),
        )[:4]
        match['top_batters_json'] = json.dumps(top_batters, ensure_ascii=False)
        match['top_bowlers_json'] = json.dumps(top_bowlers, ensure_ascii=False)

    return {
        'matches': matches,
        'player_match_rows': player_match_rows,
        'team_match_rows': team_match_rows,
        'rows_processed': rows_processed,
        'seasons': seasons,
        'venues': venues,
        'min_date': min_date,
        'max_date': max_date,
    }


def infer_batting_team(inning_label: str, teams: list[str]) -> str:
    clean_label = normalize_key(inning_label)
    for team in teams:
        if normalize_key(team) and normalize_key(team) in clean_label:
            return team
    return teams[0] if teams else ''


def other_team(teams: list[str], current: str) -> str:
    current_key = normalize_key(current)
    for team in teams:
        if normalize_key(team) != current_key:
            return team
    return ''


def is_batting_dismissal(row: dict[str, Any]) -> bool:
    dismissal = normalize_key(row.get('dismissal') or row.get('dismissal_text') or '')
    return bool(dismissal and dismissal != 'not out')


def extract_live_match_rows(match: dict[str, Any], narrative: str = '') -> dict[str, Any]:
    match_id = s_text(match.get('id'))
    if not match_id:
        raise RuntimeError('live match payload is missing match.id')

    teams = [normalize_name(item) for item in (match.get('teams') or []) if normalize_name(item)]
    match_row = {
        'id': match_id,
        'date': s_text(match.get('date_time_gmt') or match.get('date')),
        'season': s_text(str(match.get('date_time_gmt') or match.get('date'))[:4]),
        'match_type': s_text(match.get('match_type')),
        'gender': '',
        'team_type': '',
        'venue': s_text(match.get('venue')),
        'city': '',
        'winner': s_text(match.get('match_winner')),
        'toss_winner': s_text(match.get('toss_winner')),
        'toss_decision': s_text(match.get('toss_choice')),
        'source_folder': 'live_cricapi',
        'data_version': 'live',
        'source_kind': 'live',
        'status': s_text(match.get('status')) or (f"{s_text(match.get('match_winner'))} won" if s_text(match.get('match_winner')) else ''),
        'narrative': s_text(narrative),
        'team1': teams[0] if teams else '',
        'team2': teams[1] if len(teams) > 1 else '',
        'teams_json': json.dumps(teams, ensure_ascii=False),
        'innings_summary': '',
        'top_batters_json': '[]',
        'top_bowlers_json': '[]',
    }
    player_rows: dict[tuple[str, str], dict[str, Any]] = {}
    team_rows: dict[tuple[str, str], dict[str, Any]] = {}
    top_batters: list[dict[str, Any]] = []
    top_bowlers: list[dict[str, Any]] = []
    innings_summary: list[str] = []

    for inning in match.get('scorecard') or []:
        batting_team = infer_batting_team(str(inning.get('inning') or ''), teams) or 'Unknown'
        bowling_team = other_team(teams, batting_team)
        team_key = (match_id, batting_team)
        team_row = team_rows.get(team_key)
        if team_row is None:
            team_row = {
                'match_id': match_id,
                'team_name': batting_team,
                'season': match_row['season'],
                'venue': match_row['venue'],
                'runs': 0,
                'wickets_lost': 0,
                'legal_balls': 0,
            }
            team_rows[team_key] = team_row

        totals = inning.get('totals') or {}
        inning_runs = s_int(totals.get('r'))
        inning_wickets = s_int(totals.get('w'))
        inning_balls = balls_from_overs(totals.get('o'))
        if inning_runs == 0:
            inning_runs = sum(s_int(item.get('runs')) for item in inning.get('batting') or [])
        if inning_wickets == 0:
            inning_wickets = sum(1 for item in inning.get('batting') or [] if is_batting_dismissal(item))
        team_row['runs'] += inning_runs
        team_row['wickets_lost'] += inning_wickets
        team_row['legal_balls'] += inning_balls
        innings_summary.append(f'{batting_team} {inning_runs}/{inning_wickets} in {overs_from_balls(inning_balls)} overs')

        for batting_row in inning.get('batting') or []:
            player_name = normalize_name(batting_row.get('batsman', {}).get('name') or '')
            if not player_name:
                continue
            player_key = (match_id, player_name)
            player_row = player_rows.get(player_key)
            if player_row is None:
                player_row = new_player_match_row(match_row, player_name, batting_team)
                player_rows[player_key] = player_row
            player_row['team'] = batting_team or player_row['team']
            player_row['runs'] += s_int(batting_row.get('runs'))
            player_row['balls'] += s_int(batting_row.get('balls'))
            player_row['fours'] += s_int(batting_row.get('fours'))
            player_row['sixes'] += s_int(batting_row.get('sixes'))
            if is_batting_dismissal(batting_row):
                player_row['dismissals'] += 1
            top_batters.append(
                {
                    'name': player_name,
                    'team': batting_team,
                    'runs': s_int(batting_row.get('runs')),
                    'balls': s_int(batting_row.get('balls')),
                }
            )

        for bowling_row in inning.get('bowling') or []:
            player_name = normalize_name(bowling_row.get('bowler', {}).get('name') or '')
            if not player_name:
                continue
            player_key = (match_id, player_name)
            player_row = player_rows.get(player_key)
            if player_row is None:
                player_row = new_player_match_row(match_row, player_name, bowling_team)
                player_rows[player_key] = player_row
            if not player_row['team']:
                player_row['team'] = bowling_team
            player_row['bowling_runs'] += s_int(bowling_row.get('runs_conceded'))
            player_row['bowling_balls'] += balls_from_overs(bowling_row.get('overs'))
            player_row['wickets'] += s_int(bowling_row.get('wickets'))
            top_bowlers.append(
                {
                    'name': player_name,
                    'wickets': s_int(bowling_row.get('wickets')),
                    'runs_conceded': s_int(bowling_row.get('runs_conceded')),
                    'balls': balls_from_overs(bowling_row.get('overs')),
                }
            )

    match_row['innings_summary'] = ' | '.join(innings_summary)
    top_batters = sorted(top_batters, key=lambda item: (-item['runs'], item['balls'], item['name']))[:4]
    top_bowlers = sorted(top_bowlers, key=lambda item: (-item['wickets'], item['runs_conceded'], item['name']))[:4]
    match_row['top_batters_json'] = json.dumps(top_batters, ensure_ascii=False)
    match_row['top_bowlers_json'] = json.dumps(top_bowlers, ensure_ascii=False)
    return {
        'match_row': match_row,
        'player_rows': list(player_rows.values()),
        'team_rows': list(team_rows.values()),
        'player_names': sorted({row['player_name'] for row in player_rows.values()}),
        'team_names': sorted({row['team_name'] for row in team_rows.values()}),
    }


def connect_sqlite(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON')
    return connection


def initialize_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        '''
        CREATE TABLE IF NOT EXISTS matches (
          match_id TEXT PRIMARY KEY,
          source_kind TEXT NOT NULL DEFAULT 'dataset',
          source_folder TEXT NOT NULL DEFAULT '',
          data_version TEXT NOT NULL DEFAULT '',
          match_date TEXT NOT NULL DEFAULT '',
          season TEXT NOT NULL DEFAULT '',
          match_type TEXT NOT NULL DEFAULT '',
          gender TEXT NOT NULL DEFAULT '',
          team_type TEXT NOT NULL DEFAULT '',
          venue TEXT NOT NULL DEFAULT '',
          city TEXT NOT NULL DEFAULT '',
          team1 TEXT NOT NULL DEFAULT '',
          team2 TEXT NOT NULL DEFAULT '',
          winner TEXT NOT NULL DEFAULT '',
          toss_winner TEXT NOT NULL DEFAULT '',
          toss_decision TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT '',
          innings_summary TEXT NOT NULL DEFAULT '',
          top_batters_json TEXT NOT NULL DEFAULT '[]',
          top_bowlers_json TEXT NOT NULL DEFAULT '[]',
          teams_json TEXT NOT NULL DEFAULT '[]',
          narrative TEXT NOT NULL DEFAULT '',
          last_updated_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS player_match_stats (
          match_id TEXT NOT NULL,
          player_name TEXT NOT NULL,
          team TEXT NOT NULL DEFAULT '',
          runs INTEGER NOT NULL DEFAULT 0,
          balls INTEGER NOT NULL DEFAULT 0,
          wickets INTEGER NOT NULL DEFAULT 0,
          bowling_runs INTEGER NOT NULL DEFAULT 0,
          bowling_balls INTEGER NOT NULL DEFAULT 0,
          dismissals INTEGER NOT NULL DEFAULT 0,
          fours INTEGER NOT NULL DEFAULT 0,
          sixes INTEGER NOT NULL DEFAULT 0,
          dot_balls INTEGER NOT NULL DEFAULT 0,
          match_date TEXT NOT NULL DEFAULT '',
          season TEXT NOT NULL DEFAULT '',
          match_type TEXT NOT NULL DEFAULT '',
          PRIMARY KEY (match_id, player_name),
          FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_player_match_stats_player ON player_match_stats(player_name);
        CREATE INDEX IF NOT EXISTS idx_player_match_stats_date ON player_match_stats(match_date);

        CREATE TABLE IF NOT EXISTS team_match_stats (
          match_id TEXT NOT NULL,
          team_name TEXT NOT NULL,
          season TEXT NOT NULL DEFAULT '',
          venue TEXT NOT NULL DEFAULT '',
          runs INTEGER NOT NULL DEFAULT 0,
          wickets_lost INTEGER NOT NULL DEFAULT 0,
          legal_balls INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (match_id, team_name),
          FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_team_match_stats_team ON team_match_stats(team_name);

        CREATE TABLE IF NOT EXISTS players (
          player_id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          canonical_name TEXT NOT NULL DEFAULT '',
          team TEXT NOT NULL DEFAULT '',
          role TEXT NOT NULL DEFAULT '',
          matches INTEGER NOT NULL DEFAULT 0,
          total_runs INTEGER NOT NULL DEFAULT 0,
          total_wickets INTEGER NOT NULL DEFAULT 0,
          batting_avg REAL NOT NULL DEFAULT 0,
          strike_rate REAL NOT NULL DEFAULT 0,
          economy REAL NOT NULL DEFAULT 0,
          fours INTEGER NOT NULL DEFAULT 0,
          sixes INTEGER NOT NULL DEFAULT 0,
          dot_balls INTEGER NOT NULL DEFAULT 0,
          recent_scores_json TEXT NOT NULL DEFAULT '[]',
          ingested_match_ids_json TEXT NOT NULL DEFAULT '[]',
          tags_json TEXT NOT NULL DEFAULT '[]',
          updated_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS teams (
          team_id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          matches INTEGER NOT NULL DEFAULT 0,
          wins INTEGER NOT NULL DEFAULT 0,
          losses INTEGER NOT NULL DEFAULT 0,
          no_result INTEGER NOT NULL DEFAULT 0,
          win_rate REAL NOT NULL DEFAULT 0,
          total_runs INTEGER NOT NULL DEFAULT 0,
          wickets_lost INTEGER NOT NULL DEFAULT 0,
          strike_rate REAL NOT NULL DEFAULT 0,
          top_venues_json TEXT NOT NULL DEFAULT '[]',
          updated_at TEXT NOT NULL DEFAULT ''
        );
        '''
    )


def insert_archive_rows(connection: sqlite3.Connection, state: dict[str, Any], updated_at: str) -> None:
    match_rows = []
    for match in state['matches'].values():
        match_rows.append(
            (
                match['id'],
                match['source_kind'],
                match['source_folder'],
                match['data_version'],
                match['date'],
                match['season'],
                match['match_type'],
                match['gender'],
                match['team_type'],
                match['venue'],
                match['city'],
                match.get('team1', ''),
                match.get('team2', ''),
                match['winner'],
                match['toss_winner'],
                match['toss_decision'],
                match['status'],
                match.get('innings_summary', ''),
                match.get('top_batters_json', '[]'),
                match.get('top_bowlers_json', '[]'),
                match.get('teams_json', '[]'),
                match.get('narrative', ''),
                updated_at,
            )
        )

    player_rows = [
        (
            row['match_id'],
            row['player_name'],
            row['team'],
            row['runs'],
            row['balls'],
            row['wickets'],
            row['bowling_runs'],
            row['bowling_balls'],
            row['dismissals'],
            row['fours'],
            row['sixes'],
            row['dot_balls'],
            row['match_date'],
            row['season'],
            row['match_type'],
        )
        for row in state['player_match_rows'].values()
    ]
    team_rows = [
        (
            row['match_id'],
            row['team_name'],
            row['season'],
            row['venue'],
            row['runs'],
            row['wickets_lost'],
            row['legal_balls'],
        )
        for row in state['team_match_rows'].values()
    ]

    connection.executemany(
        '''
        INSERT INTO matches (
          match_id, source_kind, source_folder, data_version, match_date, season, match_type,
          gender, team_type, venue, city, team1, team2, winner, toss_winner, toss_decision,
          status, innings_summary, top_batters_json, top_bowlers_json, teams_json, narrative, last_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        match_rows,
    )
    connection.executemany(
        '''
        INSERT INTO player_match_stats (
          match_id, player_name, team, runs, balls, wickets, bowling_runs, bowling_balls,
          dismissals, fours, sixes, dot_balls, match_date, season, match_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        player_rows,
    )
    connection.executemany(
        '''
        INSERT INTO team_match_stats (
          match_id, team_name, season, venue, runs, wickets_lost, legal_balls
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        team_rows,
    )


def upsert_live_match_rows(
    connection: sqlite3.Connection,
    match_row: dict[str, Any],
    player_rows: list[dict[str, Any]],
    team_rows: list[dict[str, Any]],
    updated_at: str,
) -> bool:
    existing = connection.execute(
        'SELECT 1 FROM matches WHERE match_id = ? LIMIT 1',
        (match_row['id'],),
    ).fetchone()
    connection.execute('DELETE FROM player_match_stats WHERE match_id = ?', (match_row['id'],))
    connection.execute('DELETE FROM team_match_stats WHERE match_id = ?', (match_row['id'],))
    connection.execute('DELETE FROM matches WHERE match_id = ?', (match_row['id'],))

    state = {
        'matches': {match_row['id']: match_row},
        'player_match_rows': {(row['match_id'], row['player_name']): row for row in player_rows},
        'team_match_rows': {(row['match_id'], row['team_name']): row for row in team_rows},
    }
    insert_archive_rows(connection, state, updated_at)
    return bool(existing)


def refresh_aggregate_tables(connection: sqlite3.Connection, updated_at: str) -> None:
    connection.execute('DELETE FROM players')
    connection.execute('DELETE FROM teams')

    team_counts: dict[str, Counter] = defaultdict(Counter)
    for row in connection.execute(
        'SELECT player_name, team, COUNT(*) AS appearances FROM player_match_stats WHERE team != "" GROUP BY player_name, team'
    ):
        team_counts[str(row['player_name'])][str(row['team'])] = s_int(row['appearances'])

    recent_scores: dict[str, list[dict[str, Any]]] = defaultdict(list)
    ingested_ids: dict[str, list[str]] = defaultdict(list)
    for row in connection.execute(
        'SELECT player_name, match_id, match_date, team, runs FROM player_match_stats ORDER BY match_date DESC, match_id DESC'
    ):
        player_name = str(row['player_name'])
        if len(recent_scores[player_name]) < 5:
            recent_scores[player_name].append(
                {
                    'match_id': str(row['match_id']),
                    'date': str(row['match_date']),
                    'team': str(row['team']),
                    'runs': s_int(row['runs']),
                }
            )
        if len(ingested_ids[player_name]) < 40:
            ingested_ids[player_name].append(str(row['match_id']))

    aggregated_players = connection.execute(
        '''
        SELECT
          player_name,
          COUNT(*) AS matches,
          SUM(runs) AS total_runs,
          SUM(wickets) AS total_wickets,
          SUM(balls) AS batting_balls,
          SUM(bowling_runs) AS bowling_runs,
          SUM(bowling_balls) AS bowling_balls,
          SUM(dismissals) AS dismissals,
          SUM(fours) AS fours,
          SUM(sixes) AS sixes,
          SUM(dot_balls) AS dot_balls
        FROM player_match_stats
        GROUP BY player_name
        ORDER BY player_name
        '''
    ).fetchall()

    for row in aggregated_players:
        player_name = str(row['player_name'])
        matches = s_int(row['matches'])
        total_runs = s_int(row['total_runs'])
        total_wickets = s_int(row['total_wickets'])
        batting_balls = s_int(row['batting_balls'])
        bowling_runs = s_int(row['bowling_runs'])
        bowling_balls = s_int(row['bowling_balls'])
        dismissals = s_int(row['dismissals'])
        fours = s_int(row['fours'])
        sixes = s_int(row['sixes'])
        dot_balls = s_int(row['dot_balls'])
        batting_avg = round_num((total_runs / dismissals) if dismissals else total_runs, 2)
        strike_rate = round_num((total_runs * 100 / batting_balls) if batting_balls else 0, 2)
        economy = round_num((bowling_runs * 6 / bowling_balls) if bowling_balls else 0, 2)
        team_name = max(team_counts[player_name].items(), key=lambda item: (item[1], item[0]))[0] if team_counts[player_name] else ''
        tags = infer_tags(
            {
                'runs': total_runs,
                'wickets': total_wickets,
                'batting_average': batting_avg,
                'strike_rate': strike_rate,
                'economy': economy,
                'fours': fours,
            }
        )
        role = infer_role({'runs': total_runs, 'wickets': total_wickets})
        connection.execute(
            '''
            INSERT INTO players (
              player_id, name, canonical_name, team, role, matches, total_runs, total_wickets,
              batting_avg, strike_rate, economy, fours, sixes, dot_balls,
              recent_scores_json, ingested_match_ids_json, tags_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                f'player:{slugify(player_name)}-{stable_suffix(player_name)}',
                player_name,
                player_name,
                team_name,
                role,
                matches,
                total_runs,
                total_wickets,
                batting_avg,
                strike_rate,
                economy,
                fours,
                sixes,
                dot_balls,
                json.dumps(recent_scores[player_name], ensure_ascii=False),
                json.dumps(ingested_ids[player_name], ensure_ascii=False),
                json.dumps(tags, ensure_ascii=False),
                updated_at,
            ),
        )

    venue_counts: dict[str, Counter] = defaultdict(Counter)
    for row in connection.execute(
        'SELECT team_name, venue, COUNT(*) AS appearances FROM team_match_stats WHERE venue != "" GROUP BY team_name, venue'
    ):
        venue_counts[str(row['team_name'])][str(row['venue'])] = s_int(row['appearances'])

    win_counts: Counter = Counter()
    no_result_counts: Counter = Counter()
    for row in connection.execute('SELECT winner, team1, team2 FROM matches'):
        winner = str(row['winner'] or '').strip()
        teams = [str(row['team1'] or '').strip(), str(row['team2'] or '').strip()]
        for team in [team for team in teams if team]:
            if not winner:
                no_result_counts[team] += 1
            elif team == winner:
                win_counts[team] += 1

    aggregated_teams = connection.execute(
        '''
        SELECT
          team_name,
          COUNT(*) AS matches,
          SUM(runs) AS total_runs,
          SUM(wickets_lost) AS wickets_lost,
          SUM(legal_balls) AS legal_balls
        FROM team_match_stats
        GROUP BY team_name
        ORDER BY team_name
        '''
    ).fetchall()

    for row in aggregated_teams:
        team_name = str(row['team_name'])
        matches = s_int(row['matches'])
        wins = s_int(win_counts[team_name])
        no_result = s_int(no_result_counts[team_name])
        losses = max(0, matches - wins - no_result)
        total_runs = s_int(row['total_runs'])
        wickets_lost = s_int(row['wickets_lost'])
        legal_balls = s_int(row['legal_balls'])
        win_rate = round_num((wins * 100 / matches) if matches else 0, 1)
        strike_rate = round_num((total_runs * 100 / legal_balls) if legal_balls else 0, 1)
        top_venues = [{'venue': venue, 'matches': count} for venue, count in top_pairs(venue_counts[team_name], 5)]
        connection.execute(
            '''
            INSERT INTO teams (
              team_id, name, matches, wins, losses, no_result, win_rate,
              total_runs, wickets_lost, strike_rate, top_venues_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                f'team:{slugify(team_name)}-{stable_suffix(team_name)}',
                team_name,
                matches,
                wins,
                losses,
                no_result,
                win_rate,
                total_runs,
                wickets_lost,
                strike_rate,
                json.dumps(top_venues, ensure_ascii=False),
                updated_at,
            ),
        )


def sqlite_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    match_count = s_int(connection.execute('SELECT COUNT(*) AS count FROM matches').fetchone()['count'])
    player_count = s_int(connection.execute('SELECT COUNT(*) AS count FROM players').fetchone()['count'])
    team_count = s_int(connection.execute('SELECT COUNT(*) AS count FROM teams').fetchone()['count'])
    date_row = connection.execute(
        'SELECT MIN(match_date) AS min_date, MAX(match_date) AS max_date FROM matches'
    ).fetchone()
    season_count = s_int(
        connection.execute('SELECT COUNT(DISTINCT season) AS count FROM matches WHERE season != ""').fetchone()['count']
    )
    venue_count = s_int(
        connection.execute('SELECT COUNT(DISTINCT venue) AS count FROM matches WHERE venue != ""').fetchone()['count']
    )
    return {
        'matches': match_count,
        'players': player_count,
        'teams': team_count,
        'seasons': season_count,
        'venues': venue_count,
        'min_date': str(date_row['min_date'] or ''),
        'max_date': str(date_row['max_date'] or ''),
    }


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f'{path.suffix}.tmp')
    cleanup_path(temp_path)
    temp_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    os.replace(temp_path, path)


def promote_sqlite_output(temp_db_path: Path, final_db_path: Path, temp_manifest_path: Path, final_manifest_path: Path) -> None:
    stamp = time.strftime('%Y%m%d%H%M%S', time.gmtime())
    backup_db = final_db_path.parent / f'{final_db_path.name}.backup-{stamp}'
    backup_manifest = final_manifest_path.parent / f'{final_manifest_path.name}.backup-{stamp}'
    cleanup_path(backup_db)
    cleanup_path(backup_manifest)
    try:
        if final_db_path.exists():
            final_db_path.rename(backup_db)
        temp_db_path.rename(final_db_path)
        if final_manifest_path.exists():
            final_manifest_path.rename(backup_manifest)
        temp_manifest_path.replace(final_manifest_path)
    except Exception:
        if not final_db_path.exists() and backup_db.exists():
            backup_db.rename(final_db_path)
        if not final_manifest_path.exists() and backup_manifest.exists():
            backup_manifest.rename(final_manifest_path)
        raise
    else:
        cleanup_path(backup_db)
        cleanup_path(backup_manifest)


def read_json_column(value: str, fallback: Any) -> Any:
    text = str(value or '').strip()
    if not text:
        return fallback
    try:
        return json.loads(text)
    except Exception:
        return fallback


def build_player_docs(connection: sqlite3.Connection, names: set[str] | None = None) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    query = 'SELECT * FROM players'
    params: list[Any] = []
    if names:
        placeholders = ','.join('?' for _ in names)
        query += f' WHERE name IN ({placeholders})'
        params.extend(sorted(names))
    query += ' ORDER BY name'
    for row in connection.execute(query, params):
        recent_scores = read_json_column(str(row['recent_scores_json'] or ''), [])
        tags = read_json_column(str(row['tags_json'] or ''), [])
        recent_text = ', '.join(str(item.get('runs', 0)) for item in recent_scores[:5]) or 'n/a'
        text = (
            f"Cricket player profile {row['name']}. Team {row['team'] or 'Unknown'}. Role {row['role']}. Indexed matches {s_int(row['matches'])}. "
            f"Runs {s_int(row['total_runs'])}, wickets {s_int(row['total_wickets'])}, batting average {round_num(row['batting_avg'], 2)}, "
            f"strike rate {round_num(row['strike_rate'], 2)}, bowling economy {round_num(row['economy'], 2)}. "
            f"Fours {s_int(row['fours'])}, sixes {s_int(row['sixes'])}. Recent batting scores: {recent_text}. "
            f"Semantic profile tags: {', '.join(tags) if tags else 'Regular contributor'}."
        )
        docs.append(
            {
                'id': str(row['player_id']),
                'document': text,
                'metadata': ensure_scalar_metadata(
                    {
                        'doc_type': 'player_profile',
                        'player': row['name'],
                        'team': row['team'],
                        'role': row['role'],
                        'matches': s_int(row['matches']),
                        'runs': s_int(row['total_runs']),
                        'wickets': s_int(row['total_wickets']),
                        'strike_rate': round_num(row['strike_rate'], 2),
                        'economy': round_num(row['economy'], 2),
                    }
                ),
            }
        )
    return docs


def build_team_docs(connection: sqlite3.Connection, names: set[str] | None = None) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    query = 'SELECT * FROM teams'
    params: list[Any] = []
    if names:
        placeholders = ','.join('?' for _ in names)
        query += f' WHERE name IN ({placeholders})'
        params.extend(sorted(names))
    query += ' ORDER BY name'
    for row in connection.execute(query, params):
        venues = read_json_column(str(row['top_venues_json'] or ''), [])
        venue_text = ', '.join(f"{item.get('venue', 'Unknown')} ({s_int(item.get('matches'))})" for item in venues) or 'n/a'
        text = (
            f"Cricket team summary for {row['name']}. Indexed matches {s_int(row['matches'])}, wins {s_int(row['wins'])}, "
            f"losses {s_int(row['losses'])}, no result {s_int(row['no_result'])}, win rate {round_num(row['win_rate'], 1)} percent. "
            f"Total batting runs {s_int(row['total_runs'])}, wickets lost {s_int(row['wickets_lost'])}, team strike rate {round_num(row['strike_rate'], 1)}. "
            f"Top venues: {venue_text}."
        )
        docs.append(
            {
                'id': str(row['team_id']),
                'document': text,
                'metadata': ensure_scalar_metadata(
                    {
                        'doc_type': 'team_summary',
                        'team': row['name'],
                        'matches': s_int(row['matches']),
                        'wins': s_int(row['wins']),
                        'win_rate': round_num(row['win_rate'], 1),
                    }
                ),
            }
        )
    return docs


def build_match_docs(connection: sqlite3.Connection, match_ids: set[str] | None = None) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    query = 'SELECT * FROM matches'
    params: list[Any] = []
    if match_ids:
        placeholders = ','.join('?' for _ in match_ids)
        query += f' WHERE match_id IN ({placeholders})'
        params.extend(sorted(match_ids))
    query += ' ORDER BY match_date DESC, match_id DESC'
    for row in connection.execute(query, params):
        top_batters = read_json_column(str(row['top_batters_json'] or ''), [])
        top_bowlers = read_json_column(str(row['top_bowlers_json'] or ''), [])
        top_batters_text = ', '.join(
            f"{item.get('name', 'Unknown')} {s_int(item.get('runs'))}({s_int(item.get('balls'))})"
            for item in top_batters
        ) or 'n/a'
        top_bowlers_text = ', '.join(
            f"{item.get('name', 'Unknown')} {s_int(item.get('wickets'))}/{s_int(item.get('runs_conceded'))}"
            for item in top_bowlers
        ) or 'n/a'
        narrative = s_text(row['narrative'])
        text = (
            f"Cricket match {row['match_id']} on {row['match_date']} season {row['season']} {row['match_type']} "
            f"at {row['venue']} {row['city']}. Teams: {row['team1']} vs {row['team2']}. Winner: {row['winner'] or 'unknown'}. "
            f"Innings summary: {row['innings_summary'] or 'n/a'}. Top batters: {top_batters_text}. Top bowlers: {top_bowlers_text}."
        )
        if narrative:
            text = f'{text} Narrative: {narrative}.'
        docs.append(
            {
                'id': f"match:{row['match_id']}",
                'document': text,
                'metadata': ensure_scalar_metadata(
                    {
                        'doc_type': 'match_summary',
                        'match_id': row['match_id'],
                        'date': row['match_date'],
                        'season': row['season'],
                        'match_type': row['match_type'],
                        'venue': row['venue'],
                        'city': row['city'],
                        'winner': row['winner'],
                    }
                ),
            }
        )
    return docs


def build_all_docs(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], dict[str, int]]:
    player_docs = build_player_docs(connection)
    team_docs = build_team_docs(connection)
    match_docs = build_match_docs(connection)
    docs = player_docs + team_docs + match_docs
    return docs, {
        'player_docs': len(player_docs),
        'team_docs': len(team_docs),
        'match_docs': len(match_docs),
    }


def build_docs_for_entities(
    connection: sqlite3.Connection,
    *,
    player_names: set[str] | None = None,
    team_names: set[str] | None = None,
    match_ids: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    player_docs = build_player_docs(connection, player_names)
    team_docs = build_team_docs(connection, team_names)
    match_docs = build_match_docs(connection, match_ids)
    docs = player_docs + team_docs + match_docs
    return docs, {
        'player_docs': len(player_docs),
        'team_docs': len(team_docs),
        'match_docs': len(match_docs),
    }


def stop_client(client: Any) -> None:
    try:
        system = getattr(client, '_system', None)
        if system and hasattr(system, 'stop'):
            system.stop()
    except Exception:
        pass


def upsert_docs_to_chroma(
    db_dir: Path,
    collection_name: str,
    docs: list[dict[str, Any]],
    batch_size: int,
) -> int:
    if not docs:
        return 0
    db_dir.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(db_dir.resolve()))
    embedding_fn = embedding_functions.DefaultEmbeddingFunction()
    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=embedding_fn,
        metadata={'source': 'sqlite_archive', 'embedding_mode': 'local_default'},
    )
    try:
        for offset in range(0, len(docs), max(1, batch_size)):
            batch = docs[offset : offset + max(1, batch_size)]
            collection.upsert(
                ids=[doc['id'] for doc in batch],
                documents=[doc['document'] for doc in batch],
                metadatas=[doc['metadata'] for doc in batch],
            )
        return collection.count()
    finally:
        stop_client(client)


def promote_chroma_output(temp_db_dir: Path, final_db_dir: Path, temp_manifest_path: Path, final_manifest_path: Path) -> None:
    stamp = time.strftime('%Y%m%d%H%M%S', time.gmtime())
    backup_db = final_db_dir.parent / f'{final_db_dir.name}.backup-{stamp}'
    backup_manifest = final_manifest_path.parent / f'{final_manifest_path.name}.backup-{stamp}'
    cleanup_path(backup_db)
    cleanup_path(backup_manifest)
    try:
        if final_db_dir.exists():
            final_db_dir.rename(backup_db)
        temp_db_dir.rename(final_db_dir)
        if final_manifest_path.exists():
            final_manifest_path.rename(backup_manifest)
        temp_manifest_path.replace(final_manifest_path)
    except Exception:
        if not final_db_dir.exists() and backup_db.exists():
            backup_db.rename(final_db_dir)
        if not final_manifest_path.exists() and backup_manifest.exists():
            backup_manifest.rename(final_manifest_path)
        raise
    else:
        cleanup_path(backup_db)
        cleanup_path(backup_manifest)


def rebuild_pipeline(args: argparse.Namespace) -> int:
    dataset_manifest = run_clean_dataset(args)
    csv_state = aggregate_csv_archive(
        args.csv.resolve(),
        max_rows=args.max_rows,
        progress_every=args.progress_every,
        stall_seconds=args.stall_seconds,
        heartbeat_seconds=args.heartbeat_seconds,
    )

    final_sqlite_db = args.sqlite_db.resolve()
    final_sqlite_manifest = args.sqlite_manifest.resolve()
    temp_sqlite_db = build_temp_path(final_sqlite_db)
    temp_sqlite_manifest = build_temp_path(final_sqlite_manifest)
    cleanup_path(temp_sqlite_db)
    cleanup_path(temp_sqlite_manifest)

    updated_at = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    connection = connect_sqlite(temp_sqlite_db)
    try:
        initialize_schema(connection)
        insert_archive_rows(connection, csv_state, updated_at)
        refresh_aggregate_tables(connection, updated_at)
        connection.commit()
        sql_summary = sqlite_summary(connection)
        sqlite_manifest = {
            'built_at': updated_at,
            'sqlite_db': str(final_sqlite_db),
            'source_csv': str(args.csv.resolve()),
            'rows_processed': csv_state['rows_processed'],
            'dataset_summary': sql_summary,
        }
        temp_sqlite_manifest.write_text(json.dumps(sqlite_manifest, indent=2), encoding='utf-8')

        docs, doc_counts = build_all_docs(connection)
        ensure_unique_doc_ids(docs)
    finally:
        connection.close()

    promote_sqlite_output(temp_sqlite_db, final_sqlite_db, temp_sqlite_manifest, final_sqlite_manifest)

    final_chroma_db = args.db_dir.resolve()
    final_chroma_manifest = args.manifest.resolve()
    temp_chroma_db = build_temp_path(final_chroma_db)
    temp_chroma_manifest = build_temp_path(final_chroma_manifest)
    cleanup_path(temp_chroma_db)
    cleanup_path(temp_chroma_manifest)
    temp_chroma_db.mkdir(parents=True, exist_ok=True)

    collection_count = upsert_docs_to_chroma(temp_chroma_db, args.collection, docs, args.batch_size)
    chroma_manifest = {
        'built_at': updated_at,
        'collection': args.collection,
        'db_dir': str(final_chroma_db),
        'collection_count': collection_count,
        'player_docs': doc_counts['player_docs'],
        'team_docs': doc_counts['team_docs'],
        'match_docs': doc_counts['match_docs'],
        'delivery_chunk_docs': 0,
        'sqlite_db': str(final_sqlite_db),
        'dataset_summary': sqlite_manifest['dataset_summary'],
        'dataset_manifest': dataset_manifest or {},
    }
    temp_chroma_manifest.write_text(json.dumps(chroma_manifest, indent=2), encoding='utf-8')
    promote_chroma_output(temp_chroma_db, final_chroma_db, temp_chroma_manifest, final_chroma_manifest)

    print('[done]')
    print(json.dumps(chroma_manifest, indent=2))
    return 0


def ingest_live_match(args: argparse.Namespace) -> int:
    payload = read_json_file(args.input.resolve())
    match = payload.get('match') if isinstance(payload, dict) else None
    narrative = s_text(payload.get('narrative')) if isinstance(payload, dict) else ''
    if not isinstance(match, dict):
        print('[error] live ingest input is missing a match object', file=sys.stderr)
        return 1

    live_rows = extract_live_match_rows(match, narrative)
    sqlite_db = args.sqlite_db.resolve()
    updated_at = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    connection = connect_sqlite(sqlite_db)
    try:
        initialize_schema(connection)
        updated_existing = upsert_live_match_rows(
            connection,
            live_rows['match_row'],
            live_rows['player_rows'],
            live_rows['team_rows'],
            updated_at,
        )
        refresh_aggregate_tables(connection, updated_at)
        connection.commit()

        docs, doc_counts = build_docs_for_entities(
            connection,
            player_names=set(live_rows['player_names']),
            team_names=set(live_rows['team_names']),
            match_ids={live_rows['match_row']['id']},
        )
        ensure_unique_doc_ids(docs)
        sql_summary = sqlite_summary(connection)
    finally:
        connection.close()

    collection_count = upsert_docs_to_chroma(args.db_dir.resolve(), args.collection, docs, args.batch_size)

    sqlite_manifest = {
        'built_at': updated_at,
        'sqlite_db': str(sqlite_db),
        'source_csv': str(DEFAULT_CSV.resolve()),
        'rows_processed': None,
        'dataset_summary': sql_summary,
    }
    write_json_atomic(args.sqlite_manifest.resolve(), sqlite_manifest)

    chroma_manifest = {
        'built_at': updated_at,
        'collection': args.collection,
        'db_dir': str(args.db_dir.resolve()),
        'collection_count': collection_count,
        'player_docs': sql_summary['players'],
        'team_docs': sql_summary['teams'],
        'match_docs': sql_summary['matches'],
        'delivery_chunk_docs': 0,
        'sqlite_db': str(sqlite_db),
        'dataset_summary': sql_summary,
    }
    write_json_atomic(args.manifest.resolve(), chroma_manifest)

    print(
        json.dumps(
            {
                'ok': True,
                'match_id': live_rows['match_row']['id'],
                'updated_existing': updated_existing,
                'player_documents': doc_counts['player_docs'],
                'team_documents': doc_counts['team_docs'],
                'match_documents': doc_counts['match_docs'],
                'collection_count': collection_count,
            },
            ensure_ascii=False,
        )
    )
    return 0


def main() -> int:
    args = parse_args()
    try:
        if args.command == 'rebuild':
            return rebuild_pipeline(args)
        if args.command == 'ingest-live-match':
            return ingest_live_match(args)
        print(f'[error] unsupported command: {args.command}', file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(f'[error] dataset stage failed with exit code {exc.returncode}', file=sys.stderr)
        return exc.returncode or 1
    except Exception as exc:  # noqa: BLE001
        print(f'[error] {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
