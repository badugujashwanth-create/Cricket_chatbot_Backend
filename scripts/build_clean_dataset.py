#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIR = ROOT_DIR / 'Datasets'
DEFAULT_OUTPUT = BACKEND_DIR / 'cleaned_balls_all_matches.csv'
DEFAULT_MANIFEST = BACKEND_DIR / 'cleaned_dataset_manifest.json'

EXCLUDED_SOURCE_FOLDERS = {'all_json', '__pycache__'}
GENERATED_SOURCE_SUFFIXES = ('_csv',)

CSV_COLUMNS = [
    'match_id',
    'source_folder',
    'data_version',
    'match_date',
    'season',
    'match_type',
    'gender',
    'team_type',
    'venue',
    'city',
    'inning',
    'batting_team',
    'over',
    'ball_in_over',
    'ball_id',
    'batter',
    'bowler',
    'non_striker',
    'runs_batter',
    'runs_extras',
    'runs_total',
    'non_boundary',
    'extras_wides',
    'extras_noballs',
    'extras_byes',
    'extras_legbyes',
    'extras_penalty',
    'wicket_count',
    'wicket_player_out',
    'wicket_kind',
    'wicket_fielders',
    'has_review',
    'has_replacements',
    'match_winner',
    'toss_winner',
    'toss_decision',
]

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
    parser = argparse.ArgumentParser(description='Build a cleaned ball-by-ball CSV from cricket JSON match files.')
    parser.add_argument('--source-dir', type=Path, default=DEFAULT_SOURCE_DIR, help='Datasets root or a single source folder')
    parser.add_argument('--output', type=Path, default=DEFAULT_OUTPUT, help='Output CSV path')
    parser.add_argument('--manifest', type=Path, default=DEFAULT_MANIFEST, help='Output manifest JSON path')
    parser.add_argument('--progress-every', type=int, default=250, help='Print progress every N selected or scanned files')
    parser.add_argument('--max-files', type=int, default=None, help='Optional file limit for smoke tests')
    parser.add_argument('--stall-seconds', type=int, default=300, help='Fail if progress is not updated within this window')
    parser.add_argument('--heartbeat-seconds', type=int, default=30, help='Emit heartbeat progress messages')
    return parser.parse_args()


def s_text(value: Any) -> str:
    return ' '.join(str(value or '').strip().split())


def s_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return 0


def get_value(mapping: dict[str, Any] | None, key: str, default: Any = None) -> Any:
    if not isinstance(mapping, dict):
        return default
    return mapping.get(key, default)


def pick_match_date(info: dict[str, Any]) -> str:
    dates = get_value(info, 'dates', [])
    if isinstance(dates, list) and dates:
        values = [str(item).strip() for item in dates if str(item).strip()]
        if values:
            return s_text(min(values))
    return ''


def extract_fielders(wicket: dict[str, Any]) -> str:
    names: list[str] = []
    for fielder in get_value(wicket, 'fielders', []) or []:
        if isinstance(fielder, dict):
            name = s_text(fielder.get('name'))
        else:
            name = s_text(fielder)
        if name:
            names.append(name)
    return '; '.join(names)


def choose_primary_wicket(wickets: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not wickets:
        return None
    for wicket in wickets:
        kind = s_text(get_value(wicket, 'kind')).lower()
        if kind in CREDITED_WICKET_KINDS and s_text(get_value(wicket, 'player_out')):
            return wicket
    for wicket in wickets:
        if s_text(get_value(wicket, 'player_out')):
            return wicket
    return wickets[0]


def normalize_key_part(value: Any) -> str:
    text = s_text(value).lower()
    return ''.join(ch for ch in text if ch.isalnum() or ch in {' ', '-', '_'})


def extract_teams(info: dict[str, Any]) -> list[str]:
    teams = get_value(info, 'teams', [])
    if isinstance(teams, list):
        out = [s_text(item) for item in teams if s_text(item)]
        if out:
            return sorted(out)
    players = get_value(info, 'players', {})
    if isinstance(players, dict):
        out = [s_text(item) for item in players.keys() if s_text(item)]
        if out:
            return sorted(out)
    return []


def should_skip_folder(path: Path) -> bool:
    name = path.name.lower()
    if name.startswith('.'):
        return True
    if name in EXCLUDED_SOURCE_FOLDERS:
        return True
    return any(name.endswith(suffix) for suffix in GENERATED_SOURCE_SUFFIXES)


def discover_source_folders(source_dir: Path) -> list[Path]:
    direct_json = sorted(path for path in source_dir.glob('*.json') if path.is_file())
    if direct_json:
        return [source_dir]

    folders = [
        child
        for child in sorted(source_dir.iterdir(), key=lambda item: item.name.lower())
        if child.is_dir() and not should_skip_folder(child)
    ]
    return folders


def collect_json_files(source_dir: Path) -> list[tuple[str, Path]]:
    source_folders = discover_source_folders(source_dir)
    files: list[tuple[str, Path]] = []
    for folder in source_folders:
        source_name = folder.name
        folder_files = sorted(path for path in folder.rglob('*.json') if path.is_file())
        files.extend((source_name, path) for path in folder_files)
    return files


def build_match_identity(file_path: Path, payload: dict[str, Any], source_folder: str) -> dict[str, Any]:
    meta = get_value(payload, 'meta', {}) or {}
    info = get_value(payload, 'info', {}) or {}
    match_date = pick_match_date(info)
    teams = extract_teams(info)
    match_type = s_text(get_value(info, 'match_type'))
    venue = s_text(get_value(info, 'venue'))
    stable_match_id = s_text(get_value(meta, 'match_id') or get_value(info, 'match_id'))
    file_stem = s_text(file_path.stem)
    composite = '|'.join(
        [
            normalize_key_part(match_date),
            normalize_key_part(','.join(teams)),
            normalize_key_part(match_type),
            normalize_key_part(venue),
        ]
    ).strip('|')
    identity_key = stable_match_id or file_stem or composite
    if not identity_key:
        identity_key = str(file_path.resolve())
    return {
        'identity_key': identity_key,
        'match_id': stable_match_id or file_stem or composite or s_text(file_path.stem),
        'source_folder': source_folder,
        'path': file_path,
        'revision': s_int(get_value(meta, 'revision')),
        'data_version': s_text(get_value(meta, 'data_version')),
        'match_date': match_date,
        'match_type': match_type,
        'venue': venue,
        'teams': teams,
        'modified_ns': file_path.stat().st_mtime_ns,
    }


def is_better_candidate(candidate: dict[str, Any], incumbent: dict[str, Any]) -> bool:
    if candidate['revision'] != incumbent['revision']:
        return candidate['revision'] > incumbent['revision']
    if candidate['modified_ns'] != incumbent['modified_ns']:
        return candidate['modified_ns'] > incumbent['modified_ns']
    return str(candidate['path']) < str(incumbent['path'])


def new_source_summary() -> dict[str, Any]:
    return {
        'json_files_found': 0,
        'selected_match_files': 0,
        'rows_written': 0,
        'skipped_files': 0,
        'duplicate_files': 0,
    }


def new_counters(files_total: int) -> dict[str, Any]:
    return {
        'files_total': files_total,
        'files_selected': 0,
        'files_processed': 0,
        'files_skipped': 0,
        'duplicate_files': 0,
        'rows_written': 0,
        'matches_with_rows': 0,
        'matches_without_rows': 0,
        'deliveries_with_wickets': 0,
        'deliveries_with_reviews': 0,
        'deliveries_with_replacements': 0,
        'seasons': set(),
        'venues': set(),
        'match_types': set(),
        'genders': set(),
        'team_types': set(),
        'data_versions': Counter(),
        'min_date': '',
        'max_date': '',
    }


def write_match_rows(
    writer: csv.DictWriter,
    candidate: dict[str, Any],
    counters: dict[str, Any],
    source_summary: dict[str, Any],
    monitor: ProgressMonitor | None = None,
) -> None:
    payload = json.loads(candidate['path'].read_text(encoding='utf-8'))
    meta = get_value(payload, 'meta', {}) or {}
    info = get_value(payload, 'info', {}) or {}
    innings_list = get_value(payload, 'innings', []) or []

    match_id = candidate['match_id']
    source_folder = candidate['source_folder']
    match_date = pick_match_date(info)
    season = s_text(get_value(info, 'season'))
    match_type = s_text(get_value(info, 'match_type'))
    gender = s_text(get_value(info, 'gender'))
    team_type = s_text(get_value(info, 'team_type'))
    venue = s_text(get_value(info, 'venue'))
    city = s_text(get_value(info, 'city'))
    match_winner = s_text(get_value(get_value(info, 'outcome', {}), 'winner'))
    toss = get_value(info, 'toss', {}) or {}
    toss_winner = s_text(get_value(toss, 'winner'))
    toss_decision = s_text(get_value(toss, 'decision'))
    data_version = s_text(get_value(meta, 'data_version'))

    if match_date:
        counters['min_date'] = match_date if not counters['min_date'] or match_date < counters['min_date'] else counters['min_date']
        counters['max_date'] = match_date if not counters['max_date'] or match_date > counters['max_date'] else counters['max_date']
    if season:
        counters['seasons'].add(season)
    if venue:
        counters['venues'].add(venue)
    if match_type:
        counters['match_types'].add(match_type)
    if gender:
        counters['genders'].add(gender)
    if team_type:
        counters['team_types'].add(team_type)
    if data_version:
        counters['data_versions'][data_version] += 1

    wrote_row = False
    rows_before = counters['rows_written']
    file_row_count = 0
    for inning_index, inning in enumerate(innings_list, start=1):
        if not isinstance(inning, dict):
            continue
        batting_team = s_text(get_value(inning, 'team'))
        overs = get_value(inning, 'overs', []) or []

        for over in overs:
            if not isinstance(over, dict):
                continue
            over_number = s_int(get_value(over, 'over'))
            deliveries = get_value(over, 'deliveries', []) or []

            for delivery_index, delivery in enumerate(deliveries, start=1):
                if not isinstance(delivery, dict):
                    continue

                runs = get_value(delivery, 'runs', {}) or {}
                extras = get_value(delivery, 'extras', {}) or {}
                wickets = get_value(delivery, 'wickets', []) or []
                primary_wicket = choose_primary_wicket([w for w in wickets if isinstance(w, dict)])

                writer.writerow(
                    {
                        'match_id': match_id,
                        'source_folder': source_folder,
                        'data_version': data_version,
                        'match_date': match_date,
                        'season': season,
                        'match_type': match_type,
                        'gender': gender,
                        'team_type': team_type,
                        'venue': venue,
                        'city': city,
                        'inning': inning_index,
                        'batting_team': batting_team,
                        'over': over_number,
                        'ball_in_over': delivery_index,
                        'ball_id': f'{over_number}.{delivery_index}',
                        'batter': s_text(get_value(delivery, 'batter')),
                        'bowler': s_text(get_value(delivery, 'bowler')),
                        'non_striker': s_text(get_value(delivery, 'non_striker')),
                        'runs_batter': s_int(get_value(runs, 'batter')),
                        'runs_extras': s_int(get_value(runs, 'extras')),
                        'runs_total': s_int(get_value(runs, 'total')),
                        'non_boundary': s_int(get_value(runs, 'non_boundary', get_value(delivery, 'non_boundary'))),
                        'extras_wides': s_int(get_value(extras, 'wides')),
                        'extras_noballs': s_int(get_value(extras, 'noballs')),
                        'extras_byes': s_int(get_value(extras, 'byes')),
                        'extras_legbyes': s_int(get_value(extras, 'legbyes')),
                        'extras_penalty': s_int(get_value(extras, 'penalty')),
                        'wicket_count': len(wickets),
                        'wicket_player_out': s_text(get_value(primary_wicket, 'player_out')) if primary_wicket else '',
                        'wicket_kind': s_text(get_value(primary_wicket, 'kind')).lower() if primary_wicket else '',
                        'wicket_fielders': extract_fielders(primary_wicket) if primary_wicket else '',
                        'has_review': 1 if get_value(delivery, 'review') or get_value(delivery, 'reviews') else 0,
                        'has_replacements': 1 if get_value(delivery, 'replacements') else 0,
                        'match_winner': match_winner,
                        'toss_winner': toss_winner,
                        'toss_decision': toss_decision,
                    }
                )
                counters['rows_written'] += 1
                wrote_row = True
                file_row_count += 1
                if monitor and file_row_count % 5000 == 0:
                    monitor.touch(f"{candidate['path']} rows={file_row_count:,}")

                if wickets:
                    counters['deliveries_with_wickets'] += 1
                if get_value(delivery, 'review') or get_value(delivery, 'reviews'):
                    counters['deliveries_with_reviews'] += 1
                if get_value(delivery, 'replacements'):
                    counters['deliveries_with_replacements'] += 1

    source_summary['rows_written'] += counters['rows_written'] - rows_before
    counters['matches_with_rows'] += 1 if wrote_row else 0
    counters['matches_without_rows'] += 0 if wrote_row else 1


def cleanup_temp_paths(*paths: Path) -> None:
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


def main() -> int:
    args = parse_args()
    source_dir = args.source_dir.resolve()
    output_path = args.output.resolve()
    manifest_path = args.manifest.resolve()

    if not source_dir.exists():
        print(f'[error] source directory not found: {source_dir}', file=sys.stderr)
        return 1

    discovered_files = collect_json_files(source_dir)
    if args.max_files:
        discovered_files = discovered_files[: max(1, args.max_files)]
    if not discovered_files:
        print(f'[error] no JSON files found in: {source_dir}', file=sys.stderr)
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path = output_path.with_suffix(f'{output_path.suffix}.tmp')
    temp_manifest_path = manifest_path.with_suffix(f'{manifest_path.suffix}.tmp')
    cleanup_temp_paths(temp_output_path, temp_manifest_path)

    started = time.time()
    counters = new_counters(len(discovered_files))
    per_source = defaultdict(new_source_summary)
    selected_by_key: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []
    monitor = ProgressMonitor(args.stall_seconds, args.heartbeat_seconds)

    for index, (source_name, file_path) in enumerate(discovered_files, start=1):
        monitor.check('scan')
        monitor.touch(str(file_path))
        per_source[source_name]['json_files_found'] += 1

        try:
            payload = json.loads(file_path.read_text(encoding='utf-8'))
            candidate = build_match_identity(file_path, payload, source_name)
        except Exception as exc:  # noqa: BLE001
            counters['files_skipped'] += 1
            per_source[source_name]['skipped_files'] += 1
            errors.append({'path': str(file_path), 'error': str(exc)})
            continue

        existing = selected_by_key.get(candidate['identity_key'])
        if existing is None:
            selected_by_key[candidate['identity_key']] = candidate
        elif is_better_candidate(candidate, existing):
            selected_by_key[candidate['identity_key']] = candidate
        counters['duplicate_files'] += 1 if existing is not None else 0

        if index % max(1, args.progress_every) == 0:
            elapsed = max(time.time() - started, 0.001)
            print(
                f"[scan-progress] files={index:,}/{len(discovered_files):,} unique={len(selected_by_key):,} duplicates={counters['duplicate_files']:,} rate={index/elapsed:,.1f} files/s"
            )

    selected_candidates = sorted(
        selected_by_key.values(),
        key=lambda item: (
            item['source_folder'],
            str(item['path']).lower(),
            item['match_id'],
        ),
    )
    counters['files_selected'] = len(selected_candidates)

    for candidate in selected_candidates:
        per_source[candidate['source_folder']]['selected_match_files'] += 1

    for source_summary in per_source.values():
        source_summary['duplicate_files'] = max(
            0,
            source_summary['json_files_found'] - source_summary['selected_match_files'] - source_summary['skipped_files'],
        )

    if not selected_candidates:
        print('[error] no valid match files were selected after scanning.', file=sys.stderr)
        return 1

    try:
        with temp_output_path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
            writer.writeheader()

            for index, candidate in enumerate(selected_candidates, start=1):
                monitor.check('write')
                monitor.touch(str(candidate['path']))
                source_summary = per_source[candidate['source_folder']]
                try:
                    write_match_rows(writer, candidate, counters, source_summary, monitor)
                    counters['files_processed'] += 1
                except Exception as exc:  # noqa: BLE001
                    counters['files_skipped'] += 1
                    source_summary['skipped_files'] += 1
                    errors.append({'path': str(candidate['path']), 'error': str(exc)})
                    continue

                if index % max(1, args.progress_every) == 0:
                    elapsed = max(time.time() - started, 0.001)
                    print(
                        f"[write-progress] files={index:,}/{len(selected_candidates):,} rows={counters['rows_written']:,} rate={index/elapsed:,.1f} files/s"
                    )

        if counters['files_processed'] == 0 or counters['rows_written'] == 0:
            cleanup_temp_paths(temp_output_path, temp_manifest_path)
            print('[error] no cleaned rows were written.', file=sys.stderr)
            return 1

        elapsed = round(max(time.time() - started, 0.001), 2)
        manifest = {
            'built_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'source_dir': str(source_dir),
            'source_folders': sorted(per_source.keys()),
            'output_csv': str(output_path),
            'files_discovered': counters['files_total'],
            'files_selected': counters['files_selected'],
            'files_processed': counters['files_processed'],
            'files_skipped': counters['files_skipped'],
            'duplicate_files': counters['duplicate_files'],
            'rows_written': counters['rows_written'],
            'matches_with_rows': counters['matches_with_rows'],
            'matches_without_rows': counters['matches_without_rows'],
            'deliveries_with_wickets': counters['deliveries_with_wickets'],
            'deliveries_with_reviews': counters['deliveries_with_reviews'],
            'deliveries_with_replacements': counters['deliveries_with_replacements'],
            'min_date': counters['min_date'],
            'max_date': counters['max_date'],
            'distinct_seasons': len(counters['seasons']),
            'distinct_venues': len(counters['venues']),
            'match_types': sorted(counters['match_types']),
            'genders': sorted(counters['genders']),
            'team_types': sorted(counters['team_types']),
            'data_versions': dict(sorted(counters['data_versions'].items())),
            'per_source': {key: per_source[key] for key in sorted(per_source.keys())},
            'errors': errors[:50],
            'elapsed_seconds': elapsed,
        }
        temp_manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        os.replace(temp_output_path, output_path)
        os.replace(temp_manifest_path, manifest_path)

        print('[done]')
        print(json.dumps(manifest, indent=2))
        return 0
    except Exception as exc:  # noqa: BLE001
        cleanup_temp_paths(temp_output_path, temp_manifest_path)
        print(f'[error] failed to build cleaned dataset: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
