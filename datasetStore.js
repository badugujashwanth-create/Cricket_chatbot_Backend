const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const {
  normalizeText,
  slugify,
  toNumber,
  round,
  safeDivide,
  toOverString,
  parseDateToIso,
  seasonIncludes,
  chooseMostFrequent,
  buildPairKey,
  similarityScore
} = require('./textUtils');

const CSV_PATH = path.join(__dirname, 'cleaned_balls_two_folders.csv');
const CREDITED_WICKET_KINDS = new Set([
  'bowled',
  'caught',
  'caught and bowled',
  'lbw',
  'stumped',
  'hit wicket'
]);

const state = {
  status: 'idle',
  error: null,
  progress: {
    rows_processed: 0,
    started_at: null,
    updated_at: null
  },
  cache: null,
  loadPromise: null
};

function nowIso() {
  return new Date().toISOString();
}

function getOrCreate(map, key, creator) {
  if (!map.has(key)) map.set(key, creator());
  return map.get(key);
}

function createPlayerAggregate(name) {
  return {
    id: slugify(name),
    name,
    matches: new Set(),
    teamsCount: new Map(),
    batting: {
      runs: 0,
      balls: 0,
      dismissals: 0,
      fours: 0,
      sixes: 0
    },
    bowling: {
      balls: 0,
      runsConceded: 0,
      wickets: 0,
      dotBalls: 0
    },
    byMatch: new Map()
  };
}

function createTeamAggregate(name) {
  return {
    id: slugify(name),
    name,
    matches: new Set(),
    byMatch: new Map(),
    topBattersRaw: new Map()
  };
}

function createMatchAggregate(row) {
  const id = String(row.match_id || '').trim();
  return {
    id,
    date: parseDateToIso(row.match_date || ''),
    season: String(row.season || '').trim(),
    format: String(row.match_type || '').trim(),
    venue: String(row.venue || '').trim(),
    city: String(row.city || '').trim(),
    winner: String(row.match_winner || '').trim(),
    teams: new Set(),
    innings: new Map(),
    batters: new Map(),
    bowlers: new Map(),
    rows: 0
  };
}

function createPlayerMatchRow(match) {
  return {
    match_id: match.id,
    date: match.date,
    season: match.season,
    format: match.format,
    venue: match.venue,
    runs: 0,
    balls: 0,
    dismissals: 0,
    fours: 0,
    sixes: 0,
    wickets: 0,
    balls_bowled: 0,
    runs_conceded: 0,
    dot_balls: 0,
    teams: new Set(),
    opponents: new Set()
  };
}

function addTeamMatchRow(teamAgg, match) {
  return getOrCreate(teamAgg.byMatch, match.id, () => ({
    match_id: match.id,
    date: match.date,
    season: match.season,
    format: match.format,
    venue: match.venue,
    city: match.city,
    runs: 0,
    wickets_lost: 0,
    legal_balls: 0
  }));
}

function pushRecent(list, item, limit = 5) {
  list.push(item);
  list.sort(
    (a, b) =>
      String(b.date || '').localeCompare(String(a.date || '')) ||
      String(b.match_id || '').localeCompare(String(a.match_id || ''))
  );
  if (list.length > limit) {
    list.length = limit;
  }
}

function passesMatchFilters(match = {}, filters = {}) {
  const formatFilter = normalizeText(filters.format || '');
  const seasonFilter = String(filters.season || '').trim();
  const venueFilter = normalizeText(filters.venue || '');
  const teamFilter = normalizeText(filters.team || '');

  if (formatFilter && normalizeText(match.format || '') !== formatFilter) return false;
  if (seasonFilter && !seasonIncludes(match.season || '', seasonFilter)) return false;
  if (venueFilter && normalizeText(match.venue || '') !== venueFilter) return false;
  if (teamFilter) {
    const teams = Array.isArray(match.teams) ? match.teams : [];
    if (!teams.some((team) => normalizeText(team) === teamFilter)) return false;
  }
  return true;
}

function computePlayerStatsFromRows(rows = [], matchMap = new Map()) {
  let runs = 0;
  let balls = 0;
  let dismissals = 0;
  let fours = 0;
  let sixes = 0;
  let wickets = 0;
  let ballsBowled = 0;
  let runsConceded = 0;
  let dotBalls = 0;
  let matches = 0;
  let innings = 0;
  let bowlingInnings = 0;
  const recentMatches = [];

  for (const row of rows) {
    matches += 1;
    runs += row.runs;
    balls += row.balls;
    dismissals += row.dismissals;
    fours += row.fours;
    sixes += row.sixes;
    wickets += row.wickets;
    ballsBowled += row.balls_bowled;
    runsConceded += row.runs_conceded;
    dotBalls += row.dot_balls;

    if (row.runs > 0 || row.balls > 0 || row.dismissals > 0) innings += 1;
    if (row.balls_bowled > 0 || row.wickets > 0) bowlingInnings += 1;

    const match = matchMap.get(String(row.match_id)) || null;
    pushRecent(recentMatches, {
      match_id: row.match_id,
      date: row.date,
      season: row.season,
      format: row.format,
      venue: row.venue,
      runs: row.runs,
      wickets: row.wickets,
      teams: match?.teams || [],
      result: match?.result || ''
    });
  }

  const average = dismissals > 0 ? safeDivide(runs, dismissals) : runs > 0 ? runs : 0;
  const strikeRate = balls > 0 ? safeDivide(runs * 100, balls) : 0;
  const economy = ballsBowled > 0 ? safeDivide(runsConceded * 6, ballsBowled) : 0;

  return {
    matches,
    innings,
    bowling_innings: bowlingInnings,
    runs,
    wickets,
    average: round(average, 2),
    strike_rate: round(strikeRate, 2),
    economy: round(economy, 2),
    balls_faced: balls,
    balls_bowled: ballsBowled,
    dismissals,
    fours,
    sixes,
    dot_balls: dotBalls,
    overs_bowled: toOverString(ballsBowled),
    recent_matches: recentMatches
  };
}

function computeTeamStatsFromRows(teamName, rows = [], matchMap = new Map()) {
  let runs = 0;
  let wicketsLost = 0;
  let legalBalls = 0;
  let wins = 0;
  let losses = 0;
  let noResult = 0;
  const recentMatches = [];

  for (const row of rows) {
    runs += row.runs;
    wicketsLost += row.wickets_lost;
    legalBalls += row.legal_balls;
    const match = matchMap.get(String(row.match_id));
    const winner = String(match?.winner || '').trim();
    const teams = Array.isArray(match?.teams) ? match.teams : [];
    if (!winner) {
      noResult += 1;
    } else if (normalizeText(winner) === normalizeText(teamName)) {
      wins += 1;
    } else if (teams.length >= 2) {
      losses += 1;
    }

    const scoreline = (match?.innings || [])
      .filter((inn) => normalizeText(inn.batting_team) === normalizeText(teamName))
      .map((inn) => `${inn.runs}/${inn.wickets}`)
      .join(', ');

    pushRecent(recentMatches, {
      match_id: row.match_id,
      date: row.date,
      season: row.season,
      format: row.format,
      venue: row.venue,
      teams: match?.teams || [],
      winner,
      result: match?.result || '',
      scoreline
    });
  }

  const matches = rows.length;
  const averageScore = matches > 0 ? runs / matches : 0;
  const strikeRate = legalBalls > 0 ? (runs * 100) / legalBalls : 0;
  const winRate = matches > 0 ? (wins * 100) / matches : 0;

  return {
    matches,
    wins,
    losses,
    no_result: noResult,
    win_rate: round(winRate, 2),
    runs,
    wickets_lost: wicketsLost,
    average_score: round(averageScore, 2),
    strike_rate: round(strikeRate, 2),
    recent_matches: recentMatches
  };
}

function buildMatchSummary(match) {
  const teams = [...match.teams];
  const innings = [...match.innings.values()]
    .sort((a, b) => a.inning - b.inning)
    .map((row) => ({
      inning: row.inning,
      batting_team: row.battingTeam,
      runs: row.runs,
      wickets: row.wickets,
      overs: toOverString(row.legalBalls)
    }));

  const topBatters = [...match.batters.values()]
    .sort((a, b) => b.runs - a.runs || a.balls - b.balls || a.name.localeCompare(b.name))
    .slice(0, 4)
    .map((row) => ({
      name: row.name,
      team: row.team,
      runs: row.runs,
      balls: row.balls
    }));

  const topBowlers = [...match.bowlers.values()]
    .sort(
      (a, b) =>
        b.wickets - a.wickets ||
        a.runsConceded - b.runsConceded ||
        a.name.localeCompare(b.name)
    )
    .slice(0, 4)
    .map((row) => ({
      name: row.name,
      wickets: row.wickets,
      runs_conceded: row.runsConceded,
      overs: toOverString(row.balls),
      economy: round(row.balls > 0 ? (row.runsConceded * 6) / row.balls : 0, 2)
    }));

  const winner = String(match.winner || '').trim();
  const result = winner ? `${winner} won` : 'Result not available';
  const summary = winner
    ? `${winner} won this match at ${match.venue || 'the venue'}`
    : `Result is not available for this match`;

  return {
    id: match.id,
    date: match.date,
    season: match.season,
    format: match.format,
    venue: match.venue,
    city: match.city,
    teams,
    winner,
    result,
    summary,
    innings,
    top_batters: topBatters,
    top_bowlers: topBowlers
  };
}

function rankByQuery(items, query, accessor) {
  const cleaned = normalizeText(query || '');
  if (!cleaned) return items;
  return [...items]
    .map((item) => ({
      item,
      score: similarityScore(cleaned, accessor(item))
    }))
    .filter((row) => row.score > 0.35)
    .sort((a, b) => b.score - a.score || accessor(a.item).localeCompare(accessor(b.item)))
    .map((row) => row.item);
}

function buildCache(raw) {
  const matches = [...raw.matches.values()].map(buildMatchSummary);
  matches.sort(
    (a, b) =>
      String(b.date || '').localeCompare(String(a.date || '')) ||
      String(b.id || '').localeCompare(String(a.id || ''))
  );
  const matchMap = new Map(matches.map((match) => [String(match.id), match]));

  const players = [...raw.players.values()].map((player) => {
    const stats = computePlayerStatsFromRows([...player.byMatch.values()], matchMap);
    const team = chooseMostFrequent(player.teamsCount) || 'Unknown';
    return {
      id: player.id,
      name: player.name,
      team,
      stats,
      by_match: [...player.byMatch.values()]
    };
  });

  const teams = [...raw.teams.values()].map((team) => {
    const stats = computeTeamStatsFromRows(team.name, [...team.byMatch.values()], matchMap);
    return {
      id: team.id,
      name: team.name,
      stats,
      by_match: [...team.byMatch.values()]
    };
  });

  players.sort((a, b) => b.stats.runs - a.stats.runs || a.name.localeCompare(b.name));
  teams.sort((a, b) => b.stats.matches - a.stats.matches || a.name.localeCompare(b.name));

  const playerMapById = new Map(players.map((player) => [player.id, player]));
  const teamMapById = new Map(teams.map((team) => [team.id, team]));

  const seasons = [...raw.seasons].sort((a, b) => String(a).localeCompare(String(b)));
  const venues = [...raw.venues].sort((a, b) => a.localeCompare(b));
  const formats = [...raw.formats].sort((a, b) => a.localeCompare(b));

  const seasonMin = seasons.length ? seasons[0] : '';
  const seasonMax = seasons.length ? seasons[seasons.length - 1] : '';

  return {
    meta: {
      rows: raw.rows,
      matches: matches.length,
      players: players.length,
      teams: teams.length,
      venues: venues.length,
      formats: formats.length,
      season_min: seasonMin,
      season_max: seasonMax,
      seasons
    },
    players,
    teams,
    matches,
    playerMapById,
    teamMapById,
    matchMap,
    venues,
    formats
  };
}

function loadCsvCache() {
  if (!fs.existsSync(CSV_PATH)) {
    return Promise.reject(new Error(`CSV file not found at ${CSV_PATH}`));
  }

  const raw = {
    rows: 0,
    seasons: new Set(),
    venues: new Set(),
    formats: new Set(),
    players: new Map(),
    teams: new Map(),
    matches: new Map()
  };

  state.progress.started_at = nowIso();
  state.progress.updated_at = nowIso();
  state.progress.rows_processed = 0;

  return new Promise((resolve, reject) => {
    fs.createReadStream(CSV_PATH)
      .pipe(csv())
      .on('data', (row) => {
        raw.rows += 1;
        state.progress.rows_processed = raw.rows;
        if (raw.rows % 10000 === 0) {
          state.progress.updated_at = nowIso();
        }

        const matchId = String(row.match_id || '').trim();
        if (!matchId) return;

        const match = getOrCreate(raw.matches, matchId, () => createMatchAggregate(row));
        match.rows += 1;
        match.date = parseDateToIso(row.match_date || match.date);
        match.season = String(row.season || match.season).trim();
        match.format = String(row.match_type || match.format).trim();
        match.venue = String(row.venue || match.venue).trim();
        match.city = String(row.city || match.city).trim();
        match.winner = String(row.match_winner || match.winner).trim();

        if (match.season) raw.seasons.add(match.season);
        if (match.venue) raw.venues.add(match.venue);
        if (match.format) raw.formats.add(match.format);

        const battingTeam = String(row.batting_team || '').trim();
        if (battingTeam) {
          match.teams.add(battingTeam);
        }

        const inningNumber = toNumber(row.inning, 0);
        const runsBatter = toNumber(row.runs_batter, 0);
        const runsTotal = toNumber(row.runs_total, 0);
        const wides = toNumber(row.extras_wides, 0);
        const noBalls = toNumber(row.extras_noballs, 0);
        const byes = toNumber(row.extras_byes, 0);
        const legByes = toNumber(row.extras_legbyes, 0);
        const legalBall = wides === 0 && noBalls === 0;
        const wicketCount = toNumber(row.wicket_count, 0);
        const wicketPlayerOut = String(row.wicket_player_out || '').trim();
        const wicketKind = String(row.wicket_kind || '').trim().toLowerCase();
        const nonBoundary = toNumber(row.non_boundary, 0);

        if (battingTeam) {
          const inningKey = `${inningNumber}|${battingTeam}`;
          const inning = getOrCreate(match.innings, inningKey, () => ({
            inning: inningNumber,
            battingTeam,
            runs: 0,
            wickets: 0,
            legalBalls: 0
          }));
          inning.runs += runsTotal;
          inning.wickets += wicketCount;
          if (legalBall) inning.legalBalls += 1;

          const team = getOrCreate(raw.teams, battingTeam, () => createTeamAggregate(battingTeam));
          team.matches.add(matchId);
          const teamMatch = addTeamMatchRow(team, match);
          teamMatch.runs += runsTotal;
          teamMatch.wickets_lost += wicketCount;
          if (legalBall) teamMatch.legal_balls += 1;
        }

        const batterName = String(row.batter || '').trim();
        if (batterName) {
          const batter = getOrCreate(raw.players, batterName, () => createPlayerAggregate(batterName));
          batter.matches.add(matchId);
          if (battingTeam) {
            batter.teamsCount.set(battingTeam, (batter.teamsCount.get(battingTeam) || 0) + 1);
          }
          batter.batting.runs += runsBatter;
          if (legalBall) batter.batting.balls += 1;
          if (wicketPlayerOut && wicketPlayerOut === batterName) batter.batting.dismissals += 1;
          if (runsBatter === 4 && nonBoundary !== 1) batter.batting.fours += 1;
          if (runsBatter === 6) batter.batting.sixes += 1;

          const playerMatch = getOrCreate(batter.byMatch, matchId, () => createPlayerMatchRow(match));
          playerMatch.date = match.date;
          playerMatch.season = match.season;
          playerMatch.format = match.format;
          playerMatch.venue = match.venue;
          playerMatch.runs += runsBatter;
          if (legalBall) playerMatch.balls += 1;
          if (wicketPlayerOut && wicketPlayerOut === batterName) playerMatch.dismissals += 1;
          if (runsBatter === 4 && nonBoundary !== 1) playerMatch.fours += 1;
          if (runsBatter === 6) playerMatch.sixes += 1;
          if (battingTeam) playerMatch.teams.add(battingTeam);

          const team = getOrCreate(raw.teams, battingTeam, () => createTeamAggregate(battingTeam));
          const teamBatter = getOrCreate(team.topBattersRaw, batterName, () => ({ name: batterName, runs: 0 }));
          teamBatter.runs += runsBatter;

          const matchBatter = getOrCreate(match.batters, batterName, () => ({
            name: batterName,
            team: battingTeam || 'Unknown',
            runs: 0,
            balls: 0
          }));
          if (battingTeam) matchBatter.team = battingTeam;
          matchBatter.runs += runsBatter;
          if (legalBall) matchBatter.balls += 1;
        }

        const bowlerName = String(row.bowler || '').trim();
        if (bowlerName) {
          const bowler = getOrCreate(raw.players, bowlerName, () => createPlayerAggregate(bowlerName));
          bowler.matches.add(matchId);

          const runsConceded = runsBatter + wides + noBalls;
          bowler.bowling.runsConceded += runsConceded;
          if (legalBall) bowler.bowling.balls += 1;
          if (legalBall && runsConceded === 0 && byes === 0 && legByes === 0) {
            bowler.bowling.dotBalls += 1;
          }
          if (wicketCount > 0 && wicketPlayerOut && CREDITED_WICKET_KINDS.has(wicketKind)) {
            bowler.bowling.wickets += 1;
          }

          const playerMatch = getOrCreate(bowler.byMatch, matchId, () => createPlayerMatchRow(match));
          playerMatch.date = match.date;
          playerMatch.season = match.season;
          playerMatch.format = match.format;
          playerMatch.venue = match.venue;
          playerMatch.runs_conceded += runsConceded;
          if (legalBall) playerMatch.balls_bowled += 1;
          if (legalBall && runsConceded === 0 && byes === 0 && legByes === 0) {
            playerMatch.dot_balls += 1;
          }
          if (wicketCount > 0 && wicketPlayerOut && CREDITED_WICKET_KINDS.has(wicketKind)) {
            playerMatch.wickets += 1;
          }
          if (battingTeam) playerMatch.opponents.add(battingTeam);

          const matchBowler = getOrCreate(match.bowlers, bowlerName, () => ({
            name: bowlerName,
            wickets: 0,
            runsConceded: 0,
            balls: 0
          }));
          matchBowler.runsConceded += runsConceded;
          if (legalBall) matchBowler.balls += 1;
          if (wicketCount > 0 && wicketPlayerOut && CREDITED_WICKET_KINDS.has(wicketKind)) {
            matchBowler.wickets += 1;
          }
        }
      })
      .on('end', () => {
        const cache = buildCache(raw);
        resolve(cache);
      })
      .on('error', (error) => reject(error));
  });
}

function start() {
  if (state.cache) {
    state.status = 'ready';
    return Promise.resolve(state.cache);
  }
  if (state.loadPromise) return state.loadPromise;

  state.status = 'loading';
  state.error = null;
  state.loadPromise = loadCsvCache()
    .then((cache) => {
      state.cache = cache;
      state.status = 'ready';
      state.error = null;
      state.progress.updated_at = nowIso();
      state.loadPromise = null;
      return cache;
    })
    .catch((error) => {
      state.status = 'error';
      state.error = error;
      state.loadPromise = null;
      throw error;
    });

  return state.loadPromise;
}

async function waitUntilReady(timeoutMs = 2000) {
  if (state.cache) return state.cache;
  if (!state.loadPromise) start();
  if (!state.loadPromise) return null;

  const timeout = new Promise((resolve) => setTimeout(() => resolve(null), timeoutMs));
  try {
    return await Promise.race([state.loadPromise, timeout]);
  } catch (_) {
    return null;
  }
}

function getStatus() {
  return {
    status: state.status,
    rows_processed: state.progress.rows_processed,
    started_at: state.progress.started_at,
    updated_at: state.progress.updated_at,
    ...(state.cache?.meta || {}),
    ...(state.error ? { error: state.error.message } : {})
  };
}

function getCache() {
  return state.cache;
}

function getAbout() {
  if (!state.cache) return null;
  return {
    matches: state.cache.meta.matches,
    players: state.cache.meta.players,
    teams: state.cache.meta.teams,
    season_min: state.cache.meta.season_min,
    season_max: state.cache.meta.season_max,
    seasons: state.cache.meta.seasons,
    venues_count: state.cache.meta.venues,
    venues: state.cache.venues,
    formats: state.cache.formats
  };
}

function getHomeData() {
  if (!state.cache) return null;
  return {
    quick_stats: {
      matches: state.cache.meta.matches,
      players: state.cache.meta.players,
      teams: state.cache.meta.teams,
      seasons: `${state.cache.meta.season_min || 'N/A'} - ${state.cache.meta.season_max || 'N/A'}`
    },
    top_players: state.cache.players.slice(0, 6).map((player) => ({
      id: player.id,
      name: player.name,
      team: player.team,
      runs: player.stats.runs,
      wickets: player.stats.wickets,
      average: player.stats.average,
      strike_rate: player.stats.strike_rate
    })),
    recent_matches: state.cache.matches.slice(0, 6)
  };
}

function searchPlayers({ q = '', page = 1, limit = 12 } = {}) {
  if (!state.cache) return null;
  const cleanPage = Math.max(1, Number(page) || 1);
  const cleanLimit = Math.min(50, Math.max(1, Number(limit) || 12));
  const ranked = rankByQuery(state.cache.players, q, (player) => player.name);
  const source = q ? ranked : state.cache.players;
  const total = source.length;
  const totalPages = Math.max(1, Math.ceil(total / cleanLimit));
  const offset = (cleanPage - 1) * cleanLimit;
  const items = source.slice(offset, offset + cleanLimit).map((player) => ({
    id: player.id,
    name: player.name,
    team: player.team,
    stats: {
      matches: player.stats.matches,
      runs: player.stats.runs,
      wickets: player.stats.wickets,
      average: player.stats.average,
      strike_rate: player.stats.strike_rate,
      economy: player.stats.economy
    }
  }));

  return {
    items,
    pagination: {
      page: cleanPage,
      limit: cleanLimit,
      total,
      total_pages: totalPages
    }
  };
}

function getPlayerById(playerId = '') {
  if (!state.cache) return null;
  return state.cache.playerMapById.get(String(playerId || '').trim()) || null;
}

function getTeamById(teamId = '') {
  if (!state.cache) return null;
  return state.cache.teamMapById.get(String(teamId || '').trim()) || null;
}

function getMatchById(matchId = '') {
  if (!state.cache) return null;
  return state.cache.matchMap.get(String(matchId || '').trim()) || null;
}

function getMatches({ team = '', season = '', venue = '', limit = 10, offset = 0 } = {}) {
  if (!state.cache) return null;
  const cleanLimit = Math.min(50, Math.max(1, Number(limit) || 10));
  const cleanOffset = Math.max(0, Number(offset) || 0);
  const teamNorm = normalizeText(team);
  const seasonText = String(season || '').trim();
  const venueNorm = normalizeText(venue);

  const filtered = state.cache.matches.filter((match) => {
    if (teamNorm && !(match.teams || []).some((name) => normalizeText(name) === teamNorm)) {
      return false;
    }
    if (seasonText && !seasonIncludes(match.season || '', seasonText)) {
      return false;
    }
    if (venueNorm && normalizeText(match.venue || '') !== venueNorm) {
      return false;
    }
    return true;
  });

  return {
    items: filtered.slice(cleanOffset, cleanOffset + cleanLimit),
    pagination: {
      offset: cleanOffset,
      limit: cleanLimit,
      total: filtered.length
    }
  };
}

function getPlayerSummary(playerId, filters = {}) {
  if (!state.cache) return null;
  const player = getPlayerById(playerId);
  if (!player) return null;

  const rows = player.by_match.filter((row) => {
    const match = state.cache.matchMap.get(String(row.match_id));
    if (!match) return false;
    return passesMatchFilters(match, filters);
  });
  const stats = computePlayerStatsFromRows(rows, state.cache.matchMap);
  return {
    id: player.id,
    name: player.name,
    team: player.team,
    stats
  };
}

function getTeamSummary(teamId, filters = {}) {
  if (!state.cache) return null;
  const team = getTeamById(teamId);
  if (!team) return null;

  const rows = team.by_match.filter((row) => {
    const match = state.cache.matchMap.get(String(row.match_id));
    if (!match) return false;
    return passesMatchFilters(match, filters);
  });
  const stats = computeTeamStatsFromRows(team.name, rows, state.cache.matchMap);
  return {
    id: team.id,
    name: team.name,
    stats
  };
}

function searchTeams(query = '') {
  if (!state.cache) return null;
  const ranked = rankByQuery(state.cache.teams, query, (team) => team.name);
  const source = query ? ranked : state.cache.teams;
  return source.map((team) => ({
    id: team.id,
    name: team.name
  }));
}

function computeHeadToHead(teamA, teamB, filters = {}) {
  if (!state.cache) return null;
  const matchList = state.cache.matches.filter((match) => {
    const hasTeamA = (match.teams || []).some(
      (team) => normalizeText(team) === normalizeText(teamA)
    );
    const hasTeamB = (match.teams || []).some(
      (team) => normalizeText(team) === normalizeText(teamB)
    );
    if (!hasTeamA || !hasTeamB) return false;
    return passesMatchFilters(match, filters);
  });

  let winsA = 0;
  let winsB = 0;
  let noResult = 0;
  for (const match of matchList) {
    const winner = normalizeText(match.winner || '');
    if (!winner) noResult += 1;
    else if (winner === normalizeText(teamA)) winsA += 1;
    else if (winner === normalizeText(teamB)) winsB += 1;
  }

  return {
    matches: matchList.length,
    wins_team_a: winsA,
    wins_team_b: winsB,
    no_result: noResult,
    recent_matches: matchList.slice(0, 8)
  };
}

function listTopPlayers({ metric = 'runs', season = '', format = '', limit = 10, minBalls = 200, minOvers = 20 } = {}) {
  if (!state.cache) return null;
  const cleanLimit = Math.min(50, Math.max(1, Number(limit) || 10));
  const rows = [];

  for (const player of state.cache.players) {
    const filteredRows = player.by_match.filter((row) => {
      const match = state.cache.matchMap.get(String(row.match_id));
      if (!match) return false;
      return passesMatchFilters(match, { season, format });
    });
    const stats = computePlayerStatsFromRows(filteredRows, state.cache.matchMap);

    if (metric === 'runs') {
      rows.push({ player: player.name, team: player.team, value: stats.runs, stats });
    } else if (metric === 'wickets') {
      rows.push({ player: player.name, team: player.team, value: stats.wickets, stats });
    } else if (metric === 'strike_rate') {
      if (stats.balls_faced < Number(minBalls || 0)) continue;
      rows.push({ player: player.name, team: player.team, value: stats.strike_rate, stats });
    } else if (metric === 'economy') {
      const overs = stats.balls_bowled / 6;
      if (overs < Number(minOvers || 0)) continue;
      rows.push({ player: player.name, team: player.team, value: stats.economy, stats });
    }
  }

  const sorted = [...rows].sort((a, b) => {
    if (metric === 'economy') return a.value - b.value || a.player.localeCompare(b.player);
    return b.value - a.value || a.player.localeCompare(b.player);
  });

  return sorted.slice(0, cleanLimit).map((row, index) => ({
    rank: index + 1,
    player: row.player,
    team: row.team,
    value: round(row.value, 2),
    runs: row.stats.runs,
    wickets: row.stats.wickets,
    strike_rate: row.stats.strike_rate,
    economy: row.stats.economy
  }));
}

function findMatchByTeams({ team1 = '', team2 = '', season = '', date = '' } = {}) {
  if (!state.cache) return null;
  const team1Norm = normalizeText(team1);
  const team2Norm = normalizeText(team2);
  const dateNorm = String(date || '').trim();
  const candidates = state.cache.matches.filter((match) => {
    const teams = match.teams || [];
    const has1 = team1Norm
      ? teams.some((team) => normalizeText(team) === team1Norm)
      : true;
    const has2 = team2Norm
      ? teams.some((team) => normalizeText(team) === team2Norm)
      : true;
    if (!has1 || !has2) return false;
    if (season && !seasonIncludes(match.season || '', String(season))) return false;
    if (dateNorm && String(match.date || '') !== dateNorm) return false;
    return true;
  });
  return candidates[0] || null;
}

function comparePlayers(playerId1, playerId2, filters = {}) {
  if (!state.cache) return null;
  const left = getPlayerSummary(playerId1, filters);
  const right = getPlayerSummary(playerId2, filters);
  if (!left || !right) return null;
  return { left, right };
}

function compareTeams(teamId1, teamId2, filters = {}) {
  if (!state.cache) return null;
  const left = getTeamSummary(teamId1, filters);
  const right = getTeamSummary(teamId2, filters);
  if (!left || !right) return null;
  return { left, right };
}

function getVenueStats({ venue = '', playerId = '', teamId = '', season = '', format = '' } = {}) {
  if (!state.cache) return null;
  const venueNorm = normalizeText(venue);
  if (!venueNorm) return null;

  if (playerId) {
    const player = getPlayerById(playerId);
    if (!player) return null;
    const rows = player.by_match.filter((row) => {
      const match = state.cache.matchMap.get(String(row.match_id));
      if (!match) return false;
      if (normalizeText(match.venue || '') !== venueNorm) return false;
      return passesMatchFilters(match, { season, format });
    });
    return {
      type: 'player',
      player: { id: player.id, name: player.name, team: player.team },
      venue,
      stats: computePlayerStatsFromRows(rows, state.cache.matchMap)
    };
  }

  if (teamId) {
    const team = getTeamById(teamId);
    if (!team) return null;
    const rows = team.by_match.filter((row) => {
      const match = state.cache.matchMap.get(String(row.match_id));
      if (!match) return false;
      if (normalizeText(match.venue || '') !== venueNorm) return false;
      return passesMatchFilters(match, { season, format });
    });
    return {
      type: 'team',
      team: { id: team.id, name: team.name },
      venue,
      stats: computeTeamStatsFromRows(team.name, rows, state.cache.matchMap)
    };
  }

  const filtered = state.cache.matches.filter(
    (match) => normalizeText(match.venue || '') === venueNorm
  );
  return {
    type: 'venue',
    venue,
    matches: filtered.length,
    recent_matches: filtered.slice(0, 10)
  };
}

function getEntityIndex() {
  if (!state.cache) return null;
  const players = state.cache.players.map((player) => ({
    id: player.id,
    name: player.name,
    team: player.team,
    aliases: [player.name]
  }));
  const teams = state.cache.teams.map((team) => ({
    id: team.id,
    name: team.name,
    aliases: [team.name]
  }));
  const venues = state.cache.venues.map((venue) => ({
    id: slugify(venue),
    name: venue,
    aliases: [venue]
  }));

  return { players, teams, venues };
}

function getTeamHeadToHeadTable(filters = {}) {
  if (!state.cache) return null;
  const map = new Map();
  for (const match of state.cache.matches) {
    if (!passesMatchFilters(match, filters)) continue;
    if (!Array.isArray(match.teams) || match.teams.length < 2) continue;
    const [a, b] = match.teams;
    const key = buildPairKey(a, b);
    const row = getOrCreate(map, key, () => ({
      team_a: [a, b].sort((x, y) => x.localeCompare(y))[0],
      team_b: [a, b].sort((x, y) => x.localeCompare(y))[1],
      matches: 0,
      wins_a: 0,
      wins_b: 0,
      no_result: 0
    }));
    row.matches += 1;
    const winner = normalizeText(match.winner || '');
    if (!winner) row.no_result += 1;
    else if (winner === normalizeText(row.team_a)) row.wins_a += 1;
    else if (winner === normalizeText(row.team_b)) row.wins_b += 1;
  }
  return [...map.values()];
}

module.exports = {
  start,
  waitUntilReady,
  getStatus,
  getCache,
  getAbout,
  getHomeData,
  searchPlayers,
  getPlayerById,
  getTeamById,
  getMatchById,
  getMatches,
  getPlayerSummary,
  getTeamSummary,
  searchTeams,
  computeHeadToHead,
  listTopPlayers,
  findMatchByTeams,
  comparePlayers,
  compareTeams,
  getVenueStats,
  getEntityIndex,
  getTeamHeadToHeadTable
};
