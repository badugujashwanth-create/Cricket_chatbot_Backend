PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sync_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS players (
  id TEXT PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  dataset_name TEXT NOT NULL DEFAULT '',
  team TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT '',
  matches INTEGER NOT NULL DEFAULT 0,
  runs INTEGER NOT NULL DEFAULT 0,
  wickets INTEGER NOT NULL DEFAULT 0,
  dismissals INTEGER NOT NULL DEFAULT 0,
  batting_balls INTEGER NOT NULL DEFAULT 0,
  bowling_balls INTEGER NOT NULL DEFAULT 0,
  bowling_runs INTEGER NOT NULL DEFAULT 0,
  average REAL NOT NULL DEFAULT 0,
  strike_rate REAL NOT NULL DEFAULT 0,
  economy REAL NOT NULL DEFAULT 0,
  fours INTEGER NOT NULL DEFAULT 0,
  sixes INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 0,
  last_source TEXT NOT NULL DEFAULT 'archive',
  archive_synced_at TEXT NOT NULL DEFAULT '',
  live_updated_at TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_players_name ON players(canonical_name);
CREATE INDEX IF NOT EXISTS idx_players_team ON players(team);
CREATE INDEX IF NOT EXISTS idx_players_active ON players(is_active, canonical_name);

CREATE TABLE IF NOT EXISTS teams (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  matches INTEGER NOT NULL DEFAULT 0,
  wins INTEGER NOT NULL DEFAULT 0,
  losses INTEGER NOT NULL DEFAULT 0,
  no_result INTEGER NOT NULL DEFAULT 0,
  win_rate REAL NOT NULL DEFAULT 0,
  runs INTEGER NOT NULL DEFAULT 0,
  wickets_lost INTEGER NOT NULL DEFAULT 0,
  legal_balls INTEGER NOT NULL DEFAULT 0,
  average_score REAL NOT NULL DEFAULT 0,
  strike_rate REAL NOT NULL DEFAULT 0,
  last_source TEXT NOT NULL DEFAULT 'archive',
  archive_synced_at TEXT NOT NULL DEFAULT '',
  live_updated_at TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_teams_name ON teams(name);

CREATE TABLE IF NOT EXISTS matches (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  team1 TEXT NOT NULL DEFAULT '',
  team2 TEXT NOT NULL DEFAULT '',
  match_type TEXT NOT NULL DEFAULT '',
  date TEXT NOT NULL DEFAULT '',
  venue TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  winner TEXT NOT NULL DEFAULT '',
  summary TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT 'archive',
  synced_at TEXT NOT NULL DEFAULT '',
  payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date DESC);
CREATE INDEX IF NOT EXISTS idx_matches_teams ON matches(team1, team2);
