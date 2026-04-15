const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = process.env.SQLITE_DB_PATH || path.join(__dirname, 'cricket_runtime.sqlite3');
const SCHEMA_PATH = path.join(__dirname, 'schema.sql');

let dbInstance = null;

const PLAYER_METRICS = new Set([
  'runs',
  'wickets',
  'average',
  'strike_rate',
  'economy',
  'fours',
  'sixes',
  'matches',
]);

function nowIso() {
  return new Date().toISOString();
}

function normalizeKey(value = '') {
  return String(value || '').trim().toLowerCase();
}

function safeJsonParse(value, fallback) {
  try {
    return JSON.parse(String(value || ''));
  } catch (_) {
    return fallback;
  }
}

function ensureColumn(db, table, column, definition) {
  const columns = db.prepare(`PRAGMA table_info(${table})`).all();
  if (columns.some((item) => String(item.name) === column)) return;
  db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
}

function runMigrations(db) {
  ensureColumn(db, 'players', 'dismissals', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'players', 'batting_balls', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'players', 'bowling_balls', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'players', 'bowling_runs', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'teams', 'wickets_lost', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'teams', 'legal_balls', 'INTEGER NOT NULL DEFAULT 0');
}

function getDb() {
  if (dbInstance) return dbInstance;
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  db.exec(fs.readFileSync(SCHEMA_PATH, 'utf8'));
  runMigrations(db);
  dbInstance = db;
  return dbInstance;
}

function estimateDismissals(player = {}) {
  const runs = Number(player.runs || 0);
  const average = Number(player.average || 0);
  if (!runs || !average) return 0;
  return Math.max(1, Math.round(runs / average));
}

function estimateBattingBalls(player = {}) {
  const runs = Number(player.runs || 0);
  const strikeRate = Number(player.strike_rate || 0);
  if (!runs || !strikeRate) return 0;
  return Math.max(1, Math.round((runs * 100) / strikeRate));
}

function buildPlayerSeedRow(player = {}, syncedAt = nowIso()) {
  return {
    id: String(player.id || '').trim(),
    canonical_name: String(player.canonical_name || player.name || '').trim(),
    dataset_name: String(player.dataset_name || player.name || '').trim(),
    team: String(player.team || '').trim(),
    role: String(player.role || '').trim(),
    matches: Number(player.matches || 0),
    runs: Number(player.runs || 0),
    wickets: Number(player.wickets || 0),
    dismissals: estimateDismissals(player),
    batting_balls: estimateBattingBalls(player),
    bowling_balls: 0,
    bowling_runs: 0,
    average: Number(player.average || 0),
    strike_rate: Number(player.strike_rate || 0),
    economy: Number(player.economy || 0),
    fours: Number(player.fours || 0),
    sixes: Number(player.sixes || 0),
    is_active: 0,
    last_source: 'archive',
    archive_synced_at: syncedAt,
    live_updated_at: '',
    last_seen_at: '',
    payload_json: JSON.stringify(player || {}),
  };
}

function buildTeamSeedRow(team = {}, syncedAt = nowIso()) {
  const matches = Number(team.matches || 0);
  const runs = Number(team.runs || 0);
  return {
    id: String(team.id || '').trim(),
    name: String(team.name || '').trim(),
    matches,
    wins: Number(team.wins || 0),
    losses: Number(team.losses || 0),
    no_result: Number(team.no_result || 0),
    win_rate: Number(team.win_rate || 0),
    runs,
    wickets_lost: 0,
    legal_balls: 0,
    average_score: matches > 0 ? Number((runs / matches).toFixed(2)) : 0,
    strike_rate: Number(team.strike_rate || 0),
    last_source: 'archive',
    archive_synced_at: syncedAt,
    live_updated_at: '',
    payload_json: JSON.stringify(team || {}),
  };
}

function buildMatchSeedRow(match = {}, syncedAt = nowIso()) {
  const team1 = String(match.team1 || '').trim();
  const team2 = String(match.team2 || '').trim();
  const name = team1 && team2 ? `${team1} vs ${team2}` : 'Match Summary';
  return {
    id: String(match.id || '').trim(),
    name,
    team1,
    team2,
    match_type: String(match.format || '').trim(),
    date: String(match.date || '').trim(),
    venue: String(match.venue || '').trim(),
    status: String(match.status || '').trim(),
    winner: String(match.winner || '').trim(),
    summary: String(match.summary || '').trim(),
    source: 'archive',
    synced_at: syncedAt,
    payload_json: JSON.stringify(match || {}),
  };
}

function getMetaValue(key = '') {
  const row = getDb()
    .prepare('SELECT value FROM sync_meta WHERE key = ?')
    .get(String(key || '').trim());
  return row ? String(row.value || '').trim() : '';
}

function setMetaValue(key = '', value = '') {
  getDb()
    .prepare(
      `
        INSERT INTO sync_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
      `
    )
    .run(String(key || '').trim(), String(value || '').trim());
}

function getStoreStatus() {
  const db = getDb();
  return {
    players: Number(db.prepare('SELECT COUNT(*) AS count FROM players').get().count || 0),
    teams: Number(db.prepare('SELECT COUNT(*) AS count FROM teams').get().count || 0),
    matches: Number(db.prepare('SELECT COUNT(*) AS count FROM matches').get().count || 0),
    seeded_at: getMetaValue('archive_seeded_at'),
  };
}

function seedArchiveSnapshot({ players = [], teams = [], matches = [], manifest = null, force = false } = {}) {
  const db = getDb();
  const status = getStoreStatus();
  const manifestStamp = String(manifest?.built_at || '').trim();
  const currentManifestStamp = getMetaValue('archive_manifest_built_at');
  const manifestChanged =
    Boolean(manifestStamp) && manifestStamp !== String(currentManifestStamp || '').trim();
  if (!force && !manifestChanged && status.players > 0 && status.teams > 0) {
    return { seeded: false, reason: 'already_seeded', status };
  }
  const syncedAt = nowIso();

  const insertPlayer = db.prepare(
    `
      INSERT OR REPLACE INTO players (
        id, canonical_name, dataset_name, team, role, matches, runs, wickets, dismissals,
        batting_balls, bowling_balls, bowling_runs, average, strike_rate, economy,
        fours, sixes, is_active, last_source, archive_synced_at, live_updated_at, last_seen_at, payload_json
      ) VALUES (
        @id, @canonical_name, @dataset_name, @team, @role, @matches, @runs, @wickets, @dismissals,
        @batting_balls, @bowling_balls, @bowling_runs, @average, @strike_rate, @economy,
        @fours, @sixes, @is_active, @last_source, @archive_synced_at, @live_updated_at, @last_seen_at, @payload_json
      )
    `
  );
  const insertTeam = db.prepare(
    `
      INSERT OR REPLACE INTO teams (
        id, name, matches, wins, losses, no_result, win_rate, runs, wickets_lost,
        legal_balls, average_score, strike_rate, last_source, archive_synced_at, live_updated_at, payload_json
      ) VALUES (
        @id, @name, @matches, @wins, @losses, @no_result, @win_rate, @runs, @wickets_lost,
        @legal_balls, @average_score, @strike_rate, @last_source, @archive_synced_at, @live_updated_at, @payload_json
      )
    `
  );
  const insertMatch = db.prepare(
    `
      INSERT OR REPLACE INTO matches (
        id, name, team1, team2, match_type, date, venue, status, winner, summary, source, synced_at, payload_json
      ) VALUES (
        @id, @name, @team1, @team2, @match_type, @date, @venue, @status, @winner, @summary, @source, @synced_at, @payload_json
      )
    `
  );

  const transaction = db.transaction(() => {
    db.exec('DELETE FROM players; DELETE FROM teams; DELETE FROM matches; DELETE FROM sync_meta;');
    players.map((item) => buildPlayerSeedRow(item, syncedAt)).forEach((row) => insertPlayer.run(row));
    teams.map((item) => buildTeamSeedRow(item, syncedAt)).forEach((row) => insertTeam.run(row));
    matches.map((item) => buildMatchSeedRow(item, syncedAt)).forEach((row) => insertMatch.run(row));
    setMetaValue('archive_seeded_at', syncedAt);
    if (manifestStamp) setMetaValue('archive_manifest_built_at', manifestStamp);
  });

  transaction();
  return { seeded: true, status: getStoreStatus() };
}

function mapPlayerRow(row = {}) {
  if (!row) return null;
  const payload = safeJsonParse(row.payload_json, {});
  return {
    id: String(row.id || '').trim(),
    name: String(row.canonical_name || '').trim(),
    canonical_name: String(row.canonical_name || '').trim(),
    dataset_name: String(row.dataset_name || '').trim(),
    team: String(row.team || '').trim(),
    role: String(row.role || '').trim(),
    matches: Number(row.matches || 0),
    runs: Number(row.runs || 0),
    average: Number(row.average || 0),
    strike_rate: Number(row.strike_rate || 0),
    wickets: Number(row.wickets || 0),
    economy: Number(row.economy || 0),
    fours: Number(row.fours || 0),
    sixes: Number(row.sixes || 0),
    is_active: Boolean(row.is_active),
    payload,
  };
}

function mapTeamRow(row = {}) {
  if (!row) return null;
  const payload = safeJsonParse(row.payload_json, {});
  return {
    id: String(row.id || '').trim(),
    name: String(row.name || '').trim(),
    matches: Number(row.matches || 0),
    wins: Number(row.wins || 0),
    losses: Number(row.losses || 0),
    no_result: Number(row.no_result || 0),
    win_rate: Number(row.win_rate || 0),
    runs: Number(row.runs || 0),
    average_score: Number(row.average_score || 0),
    strike_rate: Number(row.strike_rate || 0),
    payload,
  };
}

function preferNumeric(primary, fallback) {
  const primaryNumber = Number(primary);
  if (Number.isFinite(primaryNumber) && primaryNumber > 0) return primaryNumber;
  const fallbackNumber = Number(fallback);
  return Number.isFinite(fallbackNumber) ? fallbackNumber : 0;
}

function findPlayerStats(candidate = {}) {
  const db = getDb();
  const id = String(candidate.id || '').trim();
  const names = [
    String(candidate.canonical_name || '').trim(),
    String(candidate.name || '').trim(),
    String(candidate.dataset_name || '').trim(),
  ].filter(Boolean);

  let row = null;
  if (id) {
    row = db.prepare('SELECT * FROM players WHERE id = ? LIMIT 1').get(id);
  }
  if (!row) {
    const statement = db.prepare(
      `
        SELECT * FROM players
        WHERE lower(canonical_name) = lower(?)
           OR lower(dataset_name) = lower(?)
        LIMIT 1
      `
    );
    for (const name of names) {
      row = statement.get(name, name);
      if (row) break;
    }
  }
  return mapPlayerRow(row);
}

function findTeamStats(candidate = {}) {
  const db = getDb();
  const id = String(candidate.id || '').trim();
  const name = String(candidate.name || '').trim();
  let row = null;
  if (id) {
    row = db.prepare('SELECT * FROM teams WHERE id = ? LIMIT 1').get(id);
  }
  if (!row && name) {
    row = db.prepare('SELECT * FROM teams WHERE lower(name) = lower(?) LIMIT 1').get(name);
  }
  return mapTeamRow(row);
}

function getPlayerByIdFromSql(id = '') {
  const cleanId = String(id || '').trim();
  if (!cleanId) return null;
  const row = getDb().prepare('SELECT * FROM players WHERE id = ? LIMIT 1').get(cleanId);
  return mapPlayerRow(row);
}

function getTeamByIdFromSql(id = '') {
  const cleanId = String(id || '').trim();
  if (!cleanId) return null;
  const row = getDb().prepare('SELECT * FROM teams WHERE id = ? LIMIT 1').get(cleanId);
  return mapTeamRow(row);
}

function getMatchByIdFromSql(id = '') {
  const cleanId = String(id || '').trim();
  if (!cleanId) return null;
  const row = getDb().prepare('SELECT * FROM matches WHERE id = ? LIMIT 1').get(cleanId);
  if (!row) return null;
  return {
    id: String(row.id || '').trim(),
    name: String(row.name || '').trim(),
    team1: String(row.team1 || '').trim(),
    team2: String(row.team2 || '').trim(),
    match_type: String(row.match_type || '').trim(),
    date: String(row.date || '').trim(),
    venue: String(row.venue || '').trim(),
    status: String(row.status || '').trim(),
    winner: String(row.winner || '').trim(),
    summary: String(row.summary || '').trim(),
    source: String(row.source || '').trim(),
    synced_at: String(row.synced_at || '').trim(),
    payload: safeJsonParse(row.payload_json, {}),
  };
}

function mergePlayerWithSql(player = {}) {
  const sqlPlayer = findPlayerStats(player);
  if (!sqlPlayer) return player;
  return {
    ...player,
    id: sqlPlayer.id || player.id,
    name: sqlPlayer.canonical_name || player.name,
    canonical_name: sqlPlayer.canonical_name || player.canonical_name || player.name,
    dataset_name: sqlPlayer.dataset_name || player.dataset_name || player.name,
    team: sqlPlayer.team || player.team,
    role: sqlPlayer.role || player.role,
    matches: preferNumeric(sqlPlayer.matches, player.matches),
    runs: preferNumeric(sqlPlayer.runs, player.runs),
    average: preferNumeric(sqlPlayer.average, player.average),
    strike_rate: preferNumeric(sqlPlayer.strike_rate, player.strike_rate),
    wickets: preferNumeric(sqlPlayer.wickets, player.wickets),
    economy: preferNumeric(sqlPlayer.economy, player.economy),
    fours: preferNumeric(sqlPlayer.fours, player.fours),
    sixes: preferNumeric(sqlPlayer.sixes, player.sixes),
    is_active: sqlPlayer.is_active,
  };
}

function mergeTeamWithSql(team = {}) {
  const sqlTeam = findTeamStats(team);
  if (!sqlTeam) return team;
  return {
    ...team,
    id: sqlTeam.id || team.id,
    name: sqlTeam.name || team.name,
    matches: preferNumeric(sqlTeam.matches, team.matches),
    wins: preferNumeric(sqlTeam.wins, team.wins),
    losses: preferNumeric(sqlTeam.losses, team.losses),
    no_result: preferNumeric(sqlTeam.no_result, team.no_result),
    win_rate: preferNumeric(sqlTeam.win_rate, team.win_rate),
    runs: preferNumeric(sqlTeam.runs, team.runs),
    strike_rate: preferNumeric(sqlTeam.strike_rate, team.strike_rate),
    average_score: preferNumeric(sqlTeam.average_score, team.average_score),
  };
}

function getTopPlayersByMetricFromSql(metric = 'runs', { limit = 10 } = {}) {
  const resolvedMetric = PLAYER_METRICS.has(String(metric || '').trim()) ? String(metric || '').trim() : 'runs';
  const rows = getDb()
    .prepare(
      `
        SELECT id, canonical_name, team, role, matches, runs, wickets, average, strike_rate, economy, fours, sixes
        FROM players
        ORDER BY ${resolvedMetric} DESC, matches DESC, canonical_name ASC
        LIMIT ?
      `
    )
    .all(Math.max(1, Number(limit) || 10));

  return rows.map((row, index) => ({
    rank: index + 1,
    player: String(row.canonical_name || '').trim(),
    team: String(row.team || '').trim(),
    role: String(row.role || '').trim(),
    value: Number(row[resolvedMetric] || 0),
    matches: Number(row.matches || 0),
    runs: Number(row.runs || 0),
    average: Number(row.average || 0),
    strike_rate: Number(row.strike_rate || 0),
    wickets: Number(row.wickets || 0),
    economy: Number(row.economy || 0),
    fours: Number(row.fours || 0),
    sixes: Number(row.sixes || 0),
  }));
}

function inferTeamFromInning(inning = '', teams = []) {
  const normalizedInning = normalizeKey(inning);
  const matched = teams.find((team) => normalizedInning.includes(normalizeKey(team)));
  return matched || teams[0] || '';
}

function getOtherTeam(current = '', teams = []) {
  return teams.find((team) => normalizeKey(team) !== normalizeKey(current)) || '';
}

function isDismissed(row = {}) {
  const dismissal = normalizeKey(row.dismissal || row.dismissal_text || '');
  return Boolean(dismissal && dismissal !== 'not out');
}

function ballsFromOvers(value) {
  const text = String(value ?? '').trim();
  if (!text) return 0;
  if (text.includes('.')) {
    const [whole, part] = text.split('.');
    return Number(whole || 0) * 6 + Number((part || '0').slice(0, 1));
  }
  return Number(text || 0) * 6;
}

function extractPlayerDeltasFromMatch(match = {}) {
  const deltas = new Map();
  const teams = Array.isArray(match.teams) ? match.teams.map((team) => String(team || '').trim()).filter(Boolean) : [];

  function ensurePlayer(name = '', team = '') {
    const cleanName = String(name || '').trim();
    if (!cleanName) return null;
    if (!deltas.has(cleanName)) {
      deltas.set(cleanName, {
        id: cleanName,
        canonical_name: cleanName,
        dataset_name: cleanName,
        team: String(team || '').trim(),
        role: '',
        matches_added: 0,
        runs_added: 0,
        wickets_added: 0,
        dismissals_added: 0,
        batting_balls_added: 0,
        bowling_balls_added: 0,
        bowling_runs_added: 0,
        fours_added: 0,
        sixes_added: 0,
      });
    }
    const delta = deltas.get(cleanName);
    if (!delta.team && team) delta.team = String(team).trim();
    return delta;
  }

  for (const inning of Array.isArray(match.scorecard) ? match.scorecard : []) {
    const battingTeam = inferTeamFromInning(inning.inning, teams);
    const bowlingTeam = getOtherTeam(battingTeam, teams);

    for (const row of Array.isArray(inning.batting) ? inning.batting : []) {
      const name = String(row.batsman?.name || '').trim();
      const delta = ensurePlayer(name, battingTeam);
      if (!delta) continue;
      delta.matches_added = 1;
      delta.runs_added += Number(row.runs || 0);
      delta.batting_balls_added += Number(row.balls || 0);
      delta.fours_added += Number(row.fours || 0);
      delta.sixes_added += Number(row.sixes || 0);
      delta.dismissals_added += isDismissed(row) ? 1 : 0;
    }

    for (const row of Array.isArray(inning.bowling) ? inning.bowling : []) {
      const name = String(row.bowler?.name || '').trim();
      const delta = ensurePlayer(name, bowlingTeam);
      if (!delta) continue;
      delta.matches_added = 1;
      delta.wickets_added += Number(row.wickets || 0);
      delta.bowling_balls_added += ballsFromOvers(row.overs);
      delta.bowling_runs_added += Number(row.runs_conceded || 0);
    }
  }

  return [...deltas.values()];
}

function extractTeamTotalsFromMatch(match = {}) {
  const teams = Array.isArray(match.teams) ? match.teams.map((team) => String(team || '').trim()).filter(Boolean) : [];
  const totals = new Map();

  function ensureTeam(teamName = '') {
    const cleanName = String(teamName || '').trim();
    if (!cleanName) return null;
    if (!totals.has(cleanName)) {
      totals.set(cleanName, {
        id: cleanName,
        name: cleanName,
        matches_added: 1,
        wins_added: 0,
        losses_added: 0,
        no_result_added: 0,
        runs_added: 0,
        wickets_lost_added: 0,
        legal_balls_added: 0,
      });
    }
    return totals.get(cleanName);
  }

  const winner = String(match.match_winner || '').trim();
  for (const team of teams) {
    const row = ensureTeam(team);
    if (!row) continue;
    if (!winner) row.no_result_added += 1;
    else if (normalizeKey(team) === normalizeKey(winner)) row.wins_added += 1;
    else row.losses_added += 1;
  }

  for (const inning of Array.isArray(match.scorecard) ? match.scorecard : []) {
    const battingTeam = inferTeamFromInning(inning.inning, teams);
    const row = ensureTeam(battingTeam);
    if (!row) continue;
    const totalsRow = inning.totals && typeof inning.totals === 'object' ? inning.totals : {};
    const runs =
      Number(totalsRow.r || 0) ||
      (Array.isArray(inning.batting) ? inning.batting.reduce((sum, item) => sum + Number(item.runs || 0), 0) : 0);
    const wickets =
      Number(totalsRow.w || 0) ||
      (Array.isArray(inning.batting) ? inning.batting.filter((item) => isDismissed(item)).length : 0);
    const balls = ballsFromOvers(totalsRow.o);
    row.runs_added += runs;
    row.wickets_lost_added += wickets;
    row.legal_balls_added += balls;
  }

  return [...totals.values()];
}

function buildMatchSummary(match = {}, narrative = '') {
  const winner = String(match.match_winner || '').trim();
  const scoreLines = (Array.isArray(match.score) ? match.score : [])
    .map((row) => `${row.inning || 'Innings'} ${row.runs || 0}/${row.wickets ?? '-'} (${row.overs ?? '-'})`)
    .join(', ');
  return [winner ? `${winner} won.` : '', scoreLines ? `Score summary: ${scoreLines}.` : '', String(narrative || '').trim()]
    .filter(Boolean)
    .join(' ');
}

function recordCompletedMatch(match = {}, { narrative = '' } = {}) {
  const db = getDb();
  const matchId = String(match.id || '').trim();
  if (!matchId) {
    return { applied: false, reason: 'missing_match_id' };
  }

  const playerDeltas = extractPlayerDeltasFromMatch(match);
  const teamDeltas = extractTeamTotalsFromMatch(match);
  const syncedAt = nowIso();
  const matchSummary = buildMatchSummary(match, narrative);

  const applyTransaction = db.transaction(() => {
    const existingMatch = db.prepare('SELECT id FROM matches WHERE id = ? LIMIT 1').get(matchId);
    if (existingMatch) {
      return { applied: false, reason: 'already_synced', playerDeltas: [], teamDeltas: [] };
    }

    db.prepare(
      `
        INSERT INTO matches (
          id, name, team1, team2, match_type, date, venue, status, winner, summary, source, synced_at, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live_sync', ?, ?)
      `
    ).run(
      matchId,
      String(match.name || '').trim(),
      String(match.teams?.[0] || '').trim(),
      String(match.teams?.[1] || '').trim(),
      String(match.match_type || '').trim(),
      String(match.date_time_gmt || match.date || '').trim(),
      String(match.venue || '').trim(),
      String(match.status || '').trim(),
      String(match.match_winner || '').trim(),
      matchSummary,
      syncedAt,
      JSON.stringify({ ...match, narrative }),
    );

    const upsertPlayer = db.prepare(
      `
        INSERT INTO players (
          id, canonical_name, dataset_name, team, role, matches, runs, wickets, dismissals,
          batting_balls, bowling_balls, bowling_runs, average, strike_rate, economy,
          fours, sixes, is_active, last_source, archive_synced_at, live_updated_at, last_seen_at, payload_json
        ) VALUES (
          @id, @canonical_name, @dataset_name, @team, @role, @matches, @runs, @wickets, @dismissals,
          @batting_balls, @bowling_balls, @bowling_runs, @average, @strike_rate, @economy,
          @fours, @sixes, 1, 'live_sync', '', @live_updated_at, @last_seen_at, @payload_json
        )
        ON CONFLICT(id) DO UPDATE SET
          team = CASE WHEN excluded.team != '' THEN excluded.team ELSE players.team END,
          matches = players.matches + @matches_added,
          runs = players.runs + @runs_added,
          wickets = players.wickets + @wickets_added,
          dismissals = players.dismissals + @dismissals_added,
          batting_balls = players.batting_balls + @batting_balls_added,
          bowling_balls = players.bowling_balls + @bowling_balls_added,
          bowling_runs = players.bowling_runs + @bowling_runs_added,
          fours = players.fours + @fours_added,
          sixes = players.sixes + @sixes_added,
          average = CASE
            WHEN players.dismissals + @dismissals_added > 0
              THEN ROUND((players.runs + @runs_added) * 1.0 / (players.dismissals + @dismissals_added), 2)
            ELSE players.average
          END,
          strike_rate = CASE
            WHEN players.batting_balls + @batting_balls_added > 0
              THEN ROUND((players.runs + @runs_added) * 100.0 / (players.batting_balls + @batting_balls_added), 2)
            ELSE players.strike_rate
          END,
          economy = CASE
            WHEN players.bowling_balls + @bowling_balls_added > 0
              THEN ROUND((players.bowling_runs + @bowling_runs_added) * 6.0 / (players.bowling_balls + @bowling_balls_added), 2)
            ELSE players.economy
          END,
          is_active = 1,
          last_source = 'live_sync',
          live_updated_at = @live_updated_at,
          last_seen_at = @last_seen_at,
          payload_json = @payload_json
      `
    );

    for (const delta of playerDeltas) {
      upsertPlayer.run({
        id: delta.id,
        canonical_name: delta.canonical_name,
        dataset_name: delta.dataset_name,
        team: delta.team,
        role: delta.role,
        matches: delta.matches_added,
        runs: delta.runs_added,
        wickets: delta.wickets_added,
        dismissals: delta.dismissals_added,
        batting_balls: delta.batting_balls_added,
        bowling_balls: delta.bowling_balls_added,
        bowling_runs: delta.bowling_runs_added,
        average: delta.dismissals_added > 0 ? Number((delta.runs_added / delta.dismissals_added).toFixed(2)) : 0,
        strike_rate: delta.batting_balls_added > 0 ? Number(((delta.runs_added * 100) / delta.batting_balls_added).toFixed(2)) : 0,
        economy: delta.bowling_balls_added > 0 ? Number(((delta.bowling_runs_added * 6) / delta.bowling_balls_added).toFixed(2)) : 0,
        fours: delta.fours_added,
        sixes: delta.sixes_added,
        matches_added: delta.matches_added,
        runs_added: delta.runs_added,
        wickets_added: delta.wickets_added,
        dismissals_added: delta.dismissals_added,
        batting_balls_added: delta.batting_balls_added,
        bowling_balls_added: delta.bowling_balls_added,
        bowling_runs_added: delta.bowling_runs_added,
        fours_added: delta.fours_added,
        sixes_added: delta.sixes_added,
        live_updated_at: syncedAt,
        last_seen_at: syncedAt,
        payload_json: JSON.stringify(delta),
      });
    }

    const upsertTeam = db.prepare(
      `
        INSERT INTO teams (
          id, name, matches, wins, losses, no_result, win_rate, runs, wickets_lost,
          legal_balls, average_score, strike_rate, last_source, archive_synced_at, live_updated_at, payload_json
        ) VALUES (
          @id, @name, @matches_added, @wins_added, @losses_added, @no_result_added, @win_rate,
          @runs_added, @wickets_lost_added, @legal_balls_added, @average_score, @strike_rate,
          'live_sync', '', @live_updated_at, @payload_json
        )
        ON CONFLICT(id) DO UPDATE SET
          matches = teams.matches + @matches_added,
          wins = teams.wins + @wins_added,
          losses = teams.losses + @losses_added,
          no_result = teams.no_result + @no_result_added,
          runs = teams.runs + @runs_added,
          wickets_lost = teams.wickets_lost + @wickets_lost_added,
          legal_balls = teams.legal_balls + @legal_balls_added,
          win_rate = CASE
            WHEN teams.matches + @matches_added > 0
              THEN ROUND((teams.wins + @wins_added) * 100.0 / (teams.matches + @matches_added), 1)
            ELSE teams.win_rate
          END,
          average_score = CASE
            WHEN teams.matches + @matches_added > 0
              THEN ROUND((teams.runs + @runs_added) * 1.0 / (teams.matches + @matches_added), 2)
            ELSE teams.average_score
          END,
          strike_rate = CASE
            WHEN teams.legal_balls + @legal_balls_added > 0
              THEN ROUND((teams.runs + @runs_added) * 100.0 / (teams.legal_balls + @legal_balls_added), 2)
            ELSE teams.strike_rate
          END,
          last_source = 'live_sync',
          live_updated_at = @live_updated_at,
          payload_json = @payload_json
      `
    );

    for (const delta of teamDeltas) {
      const nextMatches = Number(delta.matches_added || 0);
      const averageScore = nextMatches > 0 ? Number((delta.runs_added / nextMatches).toFixed(2)) : 0;
      const strikeRate =
        Number(delta.legal_balls_added || 0) > 0
          ? Number(((delta.runs_added * 100) / delta.legal_balls_added).toFixed(2))
          : 0;
      const winRate =
        nextMatches > 0 ? Number(((delta.wins_added * 100) / nextMatches).toFixed(1)) : 0;
      upsertTeam.run({
        ...delta,
        win_rate: winRate,
        average_score: averageScore,
        strike_rate: strikeRate,
        live_updated_at: syncedAt,
        payload_json: JSON.stringify(delta),
      });
    }

    setMetaValue('last_live_sync_at', syncedAt);
    return { applied: true, reason: '', playerDeltas, teamDeltas, summary: matchSummary };
  });

  return applyTransaction();
}

module.exports = {
  DB_PATH,
  getDb,
  getStoreStatus,
  seedArchiveSnapshot,
  getPlayerByIdFromSql,
  getTeamByIdFromSql,
  getMatchByIdFromSql,
  mergePlayerWithSql,
  mergeTeamWithSql,
  getTopPlayersByMetricFromSql,
  recordCompletedMatch,
  extractPlayerDeltasFromMatch,
  extractTeamTotalsFromMatch,
};
