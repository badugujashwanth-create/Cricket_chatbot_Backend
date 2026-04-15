#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from io import StringIO
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup


USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/124.0.0.0 Safari/537.36'
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Scrape a cricket scorecard page and normalize it into the live-ingest match JSON shape.'
    )
    parser.add_argument('--url', required=True, help='Match scorecard URL')
    parser.add_argument('--timeout', type=int, default=20, help='Request timeout in seconds')
    parser.add_argument('--output', default='', help='Optional output file path')
    return parser.parse_args()


def s_text(value: Any) -> str:
    return ' '.join(str(value or '').strip().split())


def s_int(value: Any) -> int:
    text = str(value or '').strip().replace(',', '')
    if not text or text == '-':
        return 0
    try:
        return int(text)
    except Exception:
        try:
            return int(float(text))
        except Exception:
            return 0


def s_float(value: Any) -> float:
    text = str(value or '').strip().replace(',', '')
    if not text or text == '-':
        return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0


def fetch_html(url: str, timeout: int) -> str:
    response = requests.get(
        url,
        timeout=max(5, timeout),
        headers={
            'User-Agent': USER_AGENT,
            'Accept-Language': 'en-US,en;q=0.9',
        },
    )
    response.raise_for_status()
    return response.text


def extract_json_ld(soup: BeautifulSoup) -> dict[str, Any]:
    for script in soup.find_all('script', attrs={'type': 'application/ld+json'}):
        text = script.string or script.get_text(' ', strip=True)
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get('@type') in {'SportsEvent', 'Event'}:
                    return item
        if isinstance(payload, dict) and payload.get('@type') in {'SportsEvent', 'Event'}:
            return payload
    return {}


def extract_match_meta(soup: BeautifulSoup, url: str) -> dict[str, Any]:
    json_ld = extract_json_ld(soup)
    title = s_text(soup.title.get_text(' ', strip=True) if soup.title else '')
    heading_candidates = [
        s_text(node.get_text(' ', strip=True))
        for node in soup.select('h1, h2, .ds-text-title-lg, .ds-text-title-s, .ciPageTitle')
    ]
    heading = next((item for item in heading_candidates if ' vs ' in item.lower()), title)
    match_name = heading or s_text(json_ld.get('name')) or title
    team_matches = re.findall(r'([A-Za-z][A-Za-z .&-]+)\s+vs\s+([A-Za-z][A-Za-z .&-]+)', match_name, re.I)
    teams = []
    if team_matches:
      left, right = team_matches[0]
      teams = [s_text(left), s_text(right)]

    page_text = soup.get_text('\n', strip=True)
    status = ''
    for pattern in [
        r'([A-Za-z .&-]+ won by [^\n]+)',
        r'([A-Za-z .&-]+ beat [^\n]+)',
        r'(Match (?:tied|drawn)[^\n]*)',
        r'(No result[^\n]*)',
    ]:
        match = re.search(pattern, page_text, re.I)
        if match:
            status = s_text(match.group(1))
            break

    venue = s_text(json_ld.get('location', {}).get('name'))
    if not venue:
        venue_match = re.search(r'Venue\s*[:|-]\s*([^\n]+)', page_text, re.I)
        venue = s_text(venue_match.group(1) if venue_match else '')

    start_date = s_text(json_ld.get('startDate'))
    match_id_match = re.search(r'/([0-9]{5,})(?:[/?#]|$)', url)
    match_id = match_id_match.group(1) if match_id_match else re.sub(r'\W+', '-', match_name.lower())[:48]

    winner = ''
    if status:
        winner = s_text(re.split(r'\bwon\b|\bbeat\b', status, maxsplit=1, flags=re.I)[0])
    match_type = ''
    for candidate in ['Test', 'ODI', 'T20I', 'T20', 'IPL', 'First-class', 'List A']:
        if re.search(rf'\b{re.escape(candidate)}\b', title, re.I):
            match_type = candidate
            break

    return {
        'id': match_id,
        'name': match_name,
        'teams': teams,
        'match_type': match_type,
        'status': status,
        'venue': venue,
        'date': start_date,
        'date_time_gmt': start_date,
        'match_winner': winner,
        'source_url': url,
    }


def normalize_column_name(value: Any) -> str:
    return re.sub(r'[^a-z0-9]+', '_', s_text(value).lower()).strip('_')


def is_batting_table(df: pd.DataFrame) -> bool:
    columns = {normalize_column_name(column) for column in df.columns}
    return (
        bool({'r', 'b'} & columns) and
        any(column in columns for column in {'batter', 'batting', 'batsman'})
    )


def is_bowling_table(df: pd.DataFrame) -> bool:
    columns = {normalize_column_name(column) for column in df.columns}
    return (
        any(column in columns for column in {'bowler', 'bowling'}) and
        any(column in columns for column in {'o', 'overs'}) and
        any(column in columns for column in {'w', 'wickets'})
    )


def get_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ''):
            return row[key]
    return ''


def normalize_batting_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    records = df.fillna('').to_dict(orient='records')
    rows: list[dict[str, Any]] = []
    for record in records:
        normalized = {normalize_column_name(key): value for key, value in record.items()}
        name = s_text(get_value(normalized, 'batter', 'batting', 'batsman'))
        if not name or name.lower() in {'extras', 'total', 'did not bat', 'dnb'}:
            continue
        rows.append(
            {
                'batsman': {'id': '', 'name': name},
                'dismissal': s_text(get_value(normalized, 'out', 'dismissal', 'how_out')),
                'dismissal_text': s_text(get_value(normalized, 'out', 'dismissal', 'how_out')),
                'bowler': {'id': '', 'name': ''},
                'catcher': {'id': '', 'name': ''},
                'runs': s_int(get_value(normalized, 'r', 'runs')),
                'balls': s_int(get_value(normalized, 'b', 'balls')),
                'fours': s_int(get_value(normalized, '4s', 'fours')),
                'sixes': s_int(get_value(normalized, '6s', 'sixes')),
                'strike_rate': s_float(get_value(normalized, 'sr', 'strike_rate')),
            }
        )
    return rows


def normalize_bowling_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    records = df.fillna('').to_dict(orient='records')
    rows: list[dict[str, Any]] = []
    for record in records:
        normalized = {normalize_column_name(key): value for key, value in record.items()}
        name = s_text(get_value(normalized, 'bowler', 'bowling'))
        if not name or name.lower() in {'total', 'extras'}:
            continue
        rows.append(
            {
                'bowler': {'id': '', 'name': name},
                'overs': s_text(get_value(normalized, 'o', 'overs')),
                'maidens': s_int(get_value(normalized, 'm', 'maidens')),
                'runs_conceded': s_int(get_value(normalized, 'r', 'runs')),
                'wickets': s_int(get_value(normalized, 'w', 'wickets')),
                'noballs': s_int(get_value(normalized, 'nb', 'no_balls')),
                'wides': s_int(get_value(normalized, 'wd', 'wides')),
                'economy': s_float(get_value(normalized, 'eco', 'economy')),
            }
        )
    return rows


def extract_score_summary(soup: BeautifulSoup) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    text_blocks = [
        s_text(node.get_text(' ', strip=True))
        for node in soup.select('.ds-text-tight-m, .ds-text-tight-s, .ci-team-score, .ci-team-score-text')
    ]
    for text in text_blocks:
        match = re.search(r'([A-Za-z][A-Za-z .&-]+)\s+(\d+)(?:/(\d+))?(?:\s*\(([\d.]+)\))?', text)
        if not match:
            continue
        rows.append(
            {
                'inning': s_text(match.group(1)),
                'runs': s_int(match.group(2)),
                'wickets': s_int(match.group(3)),
                'overs': s_text(match.group(4)),
            }
        )
    unique_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, int, int, str]] = set()
    for row in rows:
        key = (row['inning'], row['runs'], row['wickets'], row['overs'])
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows[:4]


def parse_scorecard(html: str, teams: list[str]) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, 'html.parser')
    innings: list[dict[str, Any]] = []
    tables = soup.find_all('table')
    pending_batting: dict[str, Any] | None = None

    for table in tables:
        try:
            dataframes = pd.read_html(StringIO(str(table)))
        except Exception:
            continue
        if not dataframes:
            continue
        df = dataframes[0]
        heading_node = table.find_previous(['h2', 'h3', 'h4', 'span', 'strong'])
        heading = s_text(heading_node.get_text(' ', strip=True) if heading_node else '')

        if is_batting_table(df):
            batting_rows = normalize_batting_rows(df)
            if not batting_rows:
                continue
            pending_batting = {
                'inning': heading or f'Innings {len(innings) + 1}',
                'batting': batting_rows,
                'bowling': [],
                'extras': {},
                'totals': {}
            }
            continue

        if is_bowling_table(df) and pending_batting is not None:
            bowling_rows = normalize_bowling_rows(df)
            pending_batting['bowling'] = bowling_rows
            total_runs = sum(row['runs'] for row in pending_batting['batting'])
            total_wickets = sum(
                1 for row in pending_batting['batting'] if s_text(row.get('dismissal_text')).lower() not in {'', 'not out'}
            )
            total_overs = s_text(get_value(bowling_rows[0] if bowling_rows else {}, 'overs')) if bowling_rows else ''
            pending_batting['totals'] = {'r': total_runs, 'w': total_wickets, 'o': total_overs}
            innings.append(pending_batting)
            pending_batting = None

    if pending_batting is not None:
        pending_batting['totals'] = {
            'r': sum(row['runs'] for row in pending_batting['batting']),
            'w': sum(
                1 for row in pending_batting['batting'] if s_text(row.get('dismissal_text')).lower() not in {'', 'not out'}
            ),
            'o': '',
        }
        innings.append(pending_batting)

    for index, inning in enumerate(innings):
        if teams and teams[index % len(teams)] and teams[index % len(teams)] not in inning['inning']:
            inning['inning'] = f"{teams[index % len(teams)]} Innings"

    return innings


def build_output(url: str, html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, 'html.parser')
    meta = extract_match_meta(soup, url)
    score = extract_score_summary(soup)
    scorecard = parse_scorecard(html, meta['teams'])
    match = {
        'source': 'web_scraper',
        'id': meta['id'],
        'name': meta['name'],
        'match_type': meta['match_type'],
        'status': meta['status'],
        'venue': meta['venue'],
        'date': meta['date'],
        'date_time_gmt': meta['date_time_gmt'],
        'teams': meta['teams'],
        'score': score,
        'match_winner': meta['match_winner'],
        'match_started': True,
        'match_ended': True,
        'scorecard': scorecard,
        'source_url': url,
    }

    return {
        'provider': 'web_scraper',
        'source': 'web_scraper',
        'match': match,
        'dataset_bridge': {
            'meta': {
                'data_version': 'web_scraper_v1',
                'revision': 1,
                'source': 'web_scraper',
            },
            'info': {
                'match_id': meta['id'],
                'dates': [meta['date']] if meta['date'] else [],
                'season': meta['date'][:4] if meta['date'] else '',
                'match_type': meta['match_type'],
                'venue': meta['venue'],
                'teams': meta['teams'],
                'outcome': {'winner': meta['match_winner']},
            },
        },
    }


def main() -> int:
    args = parse_args()
    try:
        html = fetch_html(args.url, args.timeout)
        payload = build_output(args.url, html)
        output = json.dumps(payload, ensure_ascii=False, indent=2)
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as handle:
                handle.write(output)
        else:
            print(output)
        return 0
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({'error': str(exc), 'provider': 'web_scraper'}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
