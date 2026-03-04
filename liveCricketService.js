const https = require('https');
const { URL } = require('url');

const CACHE_TTL_MS = Number(process.env.LIVE_CACHE_TTL_MS || 15000);
const DEFAULT_TIMEOUT_MS = Number(process.env.LIVE_API_TIMEOUT_MS || 10000);

const state = {
  cache: null,
  fetchedAtMs: 0,
  inFlight: null,
  lastError: null,
  lastSuccessAt: null
};

function nowIso() {
  return new Date().toISOString();
}

function normalizeText(value = '') {
  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function round(n, d = 2) {
  if (!Number.isFinite(n)) return 0;
  const f = 10 ** d;
  return Math.round(n * f) / f;
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
}

function toNumber(value, fallback = null) {
  if (value == null || value === '') return fallback;
  const n = Number(String(value).replace(/[^\d.-]/g, ''));
  return Number.isFinite(n) ? n : fallback;
}

function coalesce(...values) {
  for (const v of values) {
    if (v !== undefined && v !== null && v !== '') return v;
  }
  return null;
}

function parseScoreString(score = '') {
  const text = String(score || '').trim();
  if (!text) return null;
  const match = text.match(/(\d+)\s*\/\s*(\d+)(?:.*?\(?(\d+(?:\.\d+)?)\s*ov)?/i);
  if (match) {
    return {
      runs: Number(match[1]),
      wickets: Number(match[2]),
      overs: match[3] ? Number(match[3]) : null
    };
  }
  const alt = text.match(/(\d+)\s*\/\s*(\d+)/);
  if (alt) {
    return { runs: Number(alt[1]), wickets: Number(alt[2]), overs: null };
  }
  return null;
}

function httpJson(urlString, { headers = {}, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlString);
    const req = https.request(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port || (url.protocol === 'https:' ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method: 'GET',
        headers
      },
      (res) => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          body += chunk;
        });
        res.on('end', () => {
          if (res.statusCode < 200 || res.statusCode >= 300) {
            return reject(new Error(`Live API HTTP ${res.statusCode}`));
          }
          try {
            resolve(JSON.parse(body));
          } catch (error) {
            reject(new Error(`Live API returned invalid JSON: ${error.message}`));
          }
        });
      }
    );

    req.on('error', reject);
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`Live API timeout after ${timeoutMs}ms`));
    });
    req.end();
  });
}

function getConfig() {
  const provider = String(process.env.LIVE_CRICKET_PROVIDER || process.env.CRICKET_LIVE_PROVIDER || '').trim().toLowerCase();
  const apiUrl = String(
    process.env.LIVE_CRICKET_API_URL ||
      process.env.CRICKET_LIVE_API_URL ||
      process.env.CRICKET_API_URL ||
      ''
  ).trim();
  const apiKey = String(
    process.env.LIVE_CRICKET_API_KEY ||
      process.env.CRICKET_API_KEY ||
      process.env.CRICAPI_KEY ||
      process.env.RAPIDAPI_KEY ||
      process.env.X_RAPIDAPI_KEY ||
      ''
  ).trim();
  const rapidHost = String(process.env.RAPIDAPI_HOST || process.env.X_RAPIDAPI_HOST || '').trim();
  const defaultMatchId = String(process.env.LIVE_MATCH_ID || '').trim();
  const defaultTeams = String(process.env.LIVE_MATCH_TEAMS || '').trim();

  return {
    provider,
    apiUrl,
    apiKey,
    rapidHost,
    defaultMatchId,
    defaultTeams,
    configured: Boolean(apiUrl || apiKey || provider)
  };
}

function buildRequest(config) {
  const provider = config.provider || '';

  if (provider === 'cricapi') {
    const base = config.apiUrl || 'https://api.cricapi.com/v1/currentMatches';
    const url = new URL(base);
    if (config.apiKey && !url.searchParams.get('apikey')) url.searchParams.set('apikey', config.apiKey);
    if (!url.searchParams.get('offset')) url.searchParams.set('offset', '0');
    return { url: url.toString(), headers: {} };
  }

  if (provider === 'rapidapi') {
    if (!config.apiUrl) throw new Error('LIVE_CRICKET_API_URL is required for rapidapi provider');
    const headers = {};
    if (config.apiKey) headers['x-rapidapi-key'] = config.apiKey;
    if (config.rapidHost) headers['x-rapidapi-host'] = config.rapidHost;
    return { url: config.apiUrl, headers };
  }

  if (config.apiUrl) {
    const headers = {};
    if (config.apiKey) {
      headers['x-api-key'] = config.apiKey;
      headers.authorization = `Bearer ${config.apiKey}`;
    }
    if (config.rapidHost) headers['x-rapidapi-host'] = config.rapidHost;
    return { url: config.apiUrl, headers };
  }

  // Smart fallback if only a CricAPI-style key was provided.
  if (config.apiKey) {
    const url = new URL('https://api.cricapi.com/v1/currentMatches');
    url.searchParams.set('apikey', config.apiKey);
    url.searchParams.set('offset', '0');
    return { url: url.toString(), headers: {} };
  }

  throw new Error('Live cricket API is not configured');
}

function extractTeamNames(match = {}) {
  const names = [];

  const addName = (value) => {
    const name = String(value || '').trim();
    if (name && !names.some((n) => normalizeText(n) === normalizeText(name))) names.push(name);
  };

  for (const team of asArray(match.teams)) {
    if (typeof team === 'string') addName(team);
    else addName(team?.name || team?.teamName || team?.shortname);
  }

  for (const team of asArray(match.teamInfo)) {
    addName(team?.name || team?.shortname || team?.teamName);
  }

  addName(match.teamA?.name || match.teamA || match.teama?.name || match.teama);
  addName(match.teamB?.name || match.teamB || match.teamb?.name || match.teamb);
  addName(match.home?.name || match.homeTeam?.name);
  addName(match.away?.name || match.awayTeam?.name);

  return names.slice(0, 2);
}

function extractInningsScores(match = {}) {
  const rows = [];

  const pushRow = (row, fallbackTeam = '') => {
    if (!row) return;
    const parsed = parseScoreString(row.score || row.r || row.inningScore || '');
    const runs = coalesce(toNumber(row.runs), parsed?.runs);
    const wickets = coalesce(toNumber(row.wickets), parsed?.wickets);
    const overs = coalesce(toNumber(row.overs), parsed?.overs);
    const team = String(row.inning || row.team || row.battingTeam || row.name || fallbackTeam || '').trim();
    if (runs == null && !team && overs == null) return;
    rows.push({
      team,
      runs: runs ?? 0,
      wickets: wickets ?? 0,
      overs: overs ?? null,
      scoreText: String(row.score || '').trim() || null
    });
  };

  for (const s of asArray(match.score)) pushRow(s);
  for (const s of asArray(match.scores)) pushRow(s);
  for (const inn of asArray(match.innings)) pushRow(inn);
  for (const inn of asArray(match.scorecard?.innings)) pushRow(inn);

  // Some APIs expose score strings under keys like `team1Score` / `team2Score`.
  const teams = extractTeamNames(match);
  const t1 = parseScoreString(match.team1Score || match.score1 || '');
  const t2 = parseScoreString(match.team2Score || match.score2 || '');
  if (t1) rows.push({ team: teams[0] || 'Team 1', runs: t1.runs, wickets: t1.wickets, overs: t1.overs, scoreText: String(match.team1Score || match.score1) });
  if (t2) rows.push({ team: teams[1] || 'Team 2', runs: t2.runs, wickets: t2.wickets, overs: t2.overs, scoreText: String(match.team2Score || match.score2) });

  const deduped = [];
  for (const row of rows) {
    const exists = deduped.some(
      (x) =>
        normalizeText(x.team) === normalizeText(row.team) &&
        x.runs === row.runs &&
        x.wickets === row.wickets &&
        String(x.overs) === String(row.overs)
    );
    if (!exists) deduped.push(row);
  }
  return deduped.slice(0, 4);
}

function extractPlayers(match = {}) {
  const batters = [];
  const bowlers = [];

  const addBatter = (p) => {
    if (!p) return;
    const name = String(p.name || p.batsman || p.player || '').trim();
    if (!name) return;
    batters.push({
      name,
      runs: coalesce(toNumber(p.runs), toNumber(p.r)),
      balls: coalesce(toNumber(p.balls), toNumber(p.b)),
      strikeRate: coalesce(toNumber(p.strikeRate), toNumber(p.sr)),
      onStrike: Boolean(p.onStrike || p.striker)
    });
  };
  const addBowler = (p) => {
    if (!p) return;
    const name = String(p.name || p.bowler || p.player || '').trim();
    if (!name) return;
    bowlers.push({
      name,
      overs: coalesce(toNumber(p.overs), toNumber(p.o)),
      runs: coalesce(toNumber(p.runs), toNumber(p.r)),
      wickets: coalesce(toNumber(p.wickets), toNumber(p.w)),
      economy: coalesce(toNumber(p.economy), toNumber(p.econ))
    });
  };

  for (const p of asArray(match.batters)) addBatter(p);
  for (const p of asArray(match.batsmen)) addBatter(p);
  for (const p of asArray(match.scorecard?.batting)) addBatter(p);
  for (const p of asArray(match.scorecard?.currentBatters)) addBatter(p);
  for (const p of asArray(match.live?.batters)) addBatter(p);

  for (const p of asArray(match.bowlers)) addBowler(p);
  for (const p of asArray(match.scorecard?.bowling)) addBowler(p);
  for (const p of asArray(match.scorecard?.currentBowlers)) addBowler(p);
  for (const p of asArray(match.live?.bowlers)) addBowler(p);

  return {
    batters: batters.slice(0, 6),
    bowlers: bowlers.slice(0, 6)
  };
}

function extractCommentary(match = {}) {
  const rows = [];
  const raw = [
    ...asArray(match.commentary),
    ...asArray(match.live?.commentary),
    ...asArray(match.scorecard?.commentary),
    ...asArray(match.comments)
  ];

  for (const item of raw) {
    if (!item) continue;
    const text = String(item.text || item.commentary || item.event || item.msg || '').trim();
    if (!text) continue;
    rows.push({
      over: coalesce(String(item.over || item.ball || '').trim(), null),
      text,
      runs: toNumber(item.runs, null),
      wicket: Boolean(item.wicket || /wicket/i.test(text))
    });
  }

  return rows.slice(0, 12);
}

function inferLiveStatus(match = {}) {
  const text = normalizeText(coalesce(match.status, match.matchStatus, match.state, match.status_str, match.liveStatus, ''));
  const liveHints = ['live', 'in progress', 'innings break', 'stumps', 'rain break', 'delayed', 'drinks'];
  const finishedHints = ['won', 'result', 'completed', 'finish', 'ended', 'abandoned'];
  if (liveHints.some((x) => text.includes(x))) return true;
  if (finishedHints.some((x) => text.includes(x))) return false;
  return Boolean(match.isLive || match.live);
}

function normalizeMatch(match = {}) {
  const teams = extractTeamNames(match);
  const scores = extractInningsScores(match);
  const players = extractPlayers(match);
  const commentary = extractCommentary(match);
  const statusText = String(coalesce(match.status, match.matchStatus, match.state, match.status_str, match.liveStatus, 'Status unavailable')).trim();
  const isLive = inferLiveStatus(match);
  const matchId = String(coalesce(match.id, match.match_id, match.matchId, match.unique_id, match.fixture_id, '') || '').trim();

  const target = coalesce(toNumber(match.target), toNumber(match.live?.target));
  const runsNeeded = coalesce(toNumber(match.runsNeeded), toNumber(match.live?.runsNeeded));
  const ballsRemaining = coalesce(toNumber(match.ballsRemaining), toNumber(match.live?.ballsRemaining));
  const currentRunRate = coalesce(toNumber(match.currentRunRate), toNumber(match.crr), toNumber(match.live?.currentRunRate));
  const requiredRunRate = coalesce(toNumber(match.requiredRunRate), toNumber(match.rrr), toNumber(match.live?.requiredRunRate));

  const battingTeam = String(coalesce(match.battingTeam, match.live?.battingTeam, players.batters[0]?.team, '') || '').trim();
  const bowlingTeam = String(coalesce(match.bowlingTeam, match.live?.bowlingTeam, '') || '').trim();

  return {
    id: matchId,
    teams,
    shortTitle: teams.length >= 2 ? `${teams[0]} vs ${teams[1]}` : (String(match.name || match.title || match.matchTitle || '').trim() || 'Live match'),
    series: String(coalesce(match.series, match.series_name, match.competition, match.tournament, match.event, '') || '').trim(),
    format: String(coalesce(match.matchType, match.match_type, match.type, match.format, '') || '').trim(),
    venue: String(coalesce(match.venue, match.ground, match.location, match.city, '') || '').trim(),
    startTime: String(coalesce(match.dateTimeGMT, match.dateTime, match.date, match.startTime, '') || '').trim(),
    statusText,
    isLive,
    innings: scores,
    batters: players.batters,
    bowlers: players.bowlers,
    commentary,
    target,
    runsNeeded,
    ballsRemaining,
    currentRunRate,
    requiredRunRate,
    battingTeam,
    bowlingTeam,
    raw: match
  };
}

function extractMatchList(payload) {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload.data)) return payload.data;
  if (Array.isArray(payload.response)) return payload.response;
  if (Array.isArray(payload.matches)) return payload.matches;
  if (Array.isArray(payload.matchList)) return payload.matchList;
  if (Array.isArray(payload.results)) return payload.results;
  if (payload.match) return [payload.match];
  return [];
}

function parseVsQuery(query = '') {
  const text = String(query || '').trim();
  if (!text) return null;
  const match = text.match(/(.+?)\s+(?:vs|versus)\s+(.+)/i);
  if (!match) return null;
  return { left: match[1].trim(), right: match[2].trim() };
}

function scoreMatchAgainstQuery(match, query) {
  const vs = parseVsQuery(query);
  if (!vs) return 0;
  const [a, b] = (match.teams || []).map((t) => normalizeText(t));
  if (!a || !b) return 0;
  const left = normalizeText(vs.left);
  const right = normalizeText(vs.right);

  const direct =
    ((a.includes(left) || left.includes(a)) && (b.includes(right) || right.includes(b))) ||
    ((a.includes(right) || right.includes(a)) && (b.includes(left) || left.includes(b)));
  if (direct) return 100;

  let score = 0;
  for (const token of normalizeText(query).split(' ').filter(Boolean)) {
    if (token.length < 3) continue;
    if (a.includes(token) || b.includes(token)) score += 10;
  }
  return score;
}

function chooseMatch(matches = [], { query = '' } = {}) {
  if (!matches.length) return null;
  const config = getConfig();

  if (config.defaultMatchId) {
    const byId = matches.find((m) => String(m.id) === String(config.defaultMatchId));
    if (byId) return byId;
  }

  if (config.defaultTeams) {
    const scored = matches
      .map((m) => ({ m, s: scoreMatchAgainstQuery(m, config.defaultTeams) }))
      .sort((x, y) => y.s - x.s);
    if (scored[0]?.s > 0) return scored[0].m;
  }

  if (query) {
    const scored = matches
      .map((m) => ({ m, s: scoreMatchAgainstQuery(m, query) }))
      .sort((x, y) => y.s - x.s);
    if (scored[0]?.s > 0) return scored[0].m;
  }

  return matches.find((m) => m.isLive) || matches[0];
}

function inferCurrentContext(match) {
  const innings = Array.isArray(match.innings) ? match.innings : [];
  const lastInnings = innings[innings.length - 1] || null;
  const prevInnings = innings.length > 1 ? innings[innings.length - 2] : null;

  const battingTeam = match.battingTeam || lastInnings?.team || '';
  const bowlingTeam = match.bowlingTeam || ((match.teams || []).find((t) => normalizeText(t) !== normalizeText(battingTeam)) || '');
  const target = match.target ?? (prevInnings ? (Number(prevInnings.runs || 0) + 1) : null);
  const runsNeeded =
    match.runsNeeded ??
    (target != null && lastInnings ? Math.max(0, Number(target) - Number(lastInnings.runs || 0)) : null);

  let oversText = '';
  if (lastInnings && lastInnings.overs != null) oversText = `${lastInnings.overs}`;

  return {
    battingTeam,
    bowlingTeam,
    target,
    runsNeeded,
    ballsRemaining: match.ballsRemaining ?? null,
    currentRunRate: match.currentRunRate ?? (lastInnings?.overs ? round((Number(lastInnings.runs || 0) / Number(lastInnings.overs || 1)), 2) : null),
    requiredRunRate: match.requiredRunRate ?? null,
    oversText
  };
}

function computeMomentum(match) {
  const commentary = Array.isArray(match.commentary) ? match.commentary : [];
  const current = inferCurrentContext(match);
  let score = 0;
  let sample = 0;

  for (const item of commentary.slice(0, 8)) {
    const runs = Number.isFinite(Number(item.runs)) ? Number(item.runs) : null;
    if (runs != null) {
      score += runs;
      sample += 1;
    }
    if (item.wicket) {
      score -= 8;
      sample += 1;
    }
  }

  if (!sample && Number.isFinite(Number(current.currentRunRate)) && Number.isFinite(Number(current.requiredRunRate))) {
    score = (Number(current.currentRunRate) - Number(current.requiredRunRate)) * 3.4;
    sample = 1;
  } else if (!sample && Number.isFinite(Number(current.currentRunRate))) {
    score = (Number(current.currentRunRate) - 7.5) * 2.4;
    sample = 1;
  }

  const momentum = clamp(round(score, 1), -20, 20);
  const normalized = clamp(round(momentum / 20, 2), -1, 1);
  const phaseOvers = toNumber(current.oversText, null);
  const phase =
    phaseOvers == null ? 'live' : phaseOvers <= 6 ? 'powerplay' : phaseOvers <= 15 ? 'middle overs' : 'death overs';
  const label = normalized > 0.3 ? 'Batting side on top' : normalized < -0.3 ? 'Bowling side on top' : 'Balanced';
  const explanation =
    sample > 1
      ? `Momentum uses the latest commentary events (runs/wickets) and current scoring pace.`
      : `Momentum is approximated from current scoring pace and required rate due to limited ball-by-ball data.`;

  return {
    score: normalized,
    label,
    phase,
    battingTeam: current.battingTeam || null,
    explanation
  };
}

function buildProgress(match) {
  const current = inferCurrentContext(match);
  const innings = Array.isArray(match.innings) ? match.innings : [];
  const last = innings[innings.length - 1] || null;
  let completion = null;

  if (current.target && last && Number.isFinite(Number(last.runs))) {
    completion = clamp(Number(last.runs) / Number(current.target), 0, 1);
  } else if (last && Number.isFinite(Number(last.overs))) {
    completion = clamp(Number(last.overs) / 20, 0, 1);
  }

  return {
    inningsCount: innings.length,
    completion: completion == null ? null : round(completion, 2),
    current
  };
}

function buildPlayerHighlights(match) {
  const batters = Array.isArray(match.batters) ? [...match.batters] : [];
  const bowlers = Array.isArray(match.bowlers) ? [...match.bowlers] : [];

  const topBatters = batters
    .sort((a, b) => (Number(b.runs || 0) - Number(a.runs || 0)) || (Number(a.balls || 999) - Number(b.balls || 999)))
    .slice(0, 3)
    .map((p) => ({
      type: 'batter',
      name: p.name,
      primary: `${p.runs ?? 0}${p.balls != null ? ` (${p.balls})` : ''}`,
      secondary: p.strikeRate != null ? `SR ${p.strikeRate}` : ''
    }));

  const topBowlers = bowlers
    .sort((a, b) => (Number(b.wickets || 0) - Number(a.wickets || 0)) || (Number(a.economy || 99) - Number(b.economy || 99)))
    .slice(0, 2)
    .map((p) => ({
      type: 'bowler',
      name: p.name,
      primary: `${p.wickets ?? 0}/${p.runs ?? 0}`,
      secondary: p.economy != null ? `Econ ${p.economy}` : ''
    }));

  return [...topBatters, ...topBowlers].slice(0, 5);
}

function normalizeLiveResponse(payload, { query = '' } = {}) {
  const rawMatches = extractMatchList(payload);
  const matches = rawMatches.map(normalizeMatch).filter((m) => m.teams.length || m.id || m.statusText);
  const selected = chooseMatch(matches, { query });

  if (!matches.length) {
    return {
      available: false,
      configured: true,
      fetchedAt: nowIso(),
      message: 'Live API responded, but no live cricket matches were found in the payload.',
      matches: [],
      match: null
    };
  }

  const match = selected || matches[0];
  const progress = buildProgress(match);
  const momentum = computeMomentum(match);
  const playerHighlights = buildPlayerHighlights(match);

  return {
    available: true,
    configured: true,
    fetchedAt: nowIso(),
    providerMessage: String(coalesce(payload.status, payload.message, payload.msg, '') || '').trim() || null,
    matches,
    match,
    progress,
    momentum,
    playerHighlights
  };
}

async function fetchLivePayload() {
  const config = getConfig();
  if (!config.configured) {
    return {
      available: false,
      configured: false,
      fetchedAt: nowIso(),
      message: 'Live cricket API is not configured. Set your existing live cricket API URL/key env vars to enable live scores.'
    };
  }

  const request = buildRequest(config);
  const payload = await httpJson(request.url, { headers: request.headers });
  return normalizeLiveResponse(payload);
}

async function getSnapshot({ query = '', force = false } = {}) {
  const cacheFresh = state.cache && Date.now() - state.fetchedAtMs < CACHE_TTL_MS;
  if (!force && cacheFresh) {
    if (query && state.cache?.matches?.length) {
      const selected = chooseMatch(state.cache.matches, { query });
      if (selected) {
        return {
          ...state.cache,
          match: selected,
          progress: buildProgress(selected),
          momentum: computeMomentum(selected),
          playerHighlights: buildPlayerHighlights(selected)
        };
      }
    }
    return state.cache;
  }

  if (state.inFlight && !force) return state.inFlight;

  state.inFlight = (async () => {
    try {
      const result = await fetchLivePayload();
      state.cache = result;
      state.fetchedAtMs = Date.now();
      state.lastError = null;
      state.lastSuccessAt = nowIso();

      if (query && result?.matches?.length) {
        const selected = chooseMatch(result.matches, { query });
        if (selected) {
          result.match = selected;
          result.progress = buildProgress(selected);
          result.momentum = computeMomentum(selected);
          result.playerHighlights = buildPlayerHighlights(selected);
        }
      }
      return result;
    } catch (error) {
      state.lastError = { message: error.message, at: nowIso() };
      const fallback = state.cache
        ? {
            ...state.cache,
            stale: true,
            available: Boolean(state.cache.available),
            fetchedAt: state.cache.fetchedAt || nowIso(),
            message: `Live feed fetch failed. Showing last cached update. (${error.message})`
          }
        : {
            available: false,
            configured: getConfig().configured,
            fetchedAt: nowIso(),
            message: `Live data unavailable right now. ${error.message}`
          };
      return fallback;
    } finally {
      state.inFlight = null;
    }
  })();

  return state.inFlight;
}

function getStatus() {
  const config = getConfig();
  return {
    configured: config.configured,
    provider: config.provider || (config.apiUrl ? 'custom' : null),
    apiUrlConfigured: Boolean(config.apiUrl),
    apiKeyConfigured: Boolean(config.apiKey),
    rapidHostConfigured: Boolean(config.rapidHost),
    defaultMatchIdConfigured: Boolean(config.defaultMatchId),
    cacheTtlMs: CACHE_TTL_MS,
    lastSuccessAt: state.lastSuccessAt,
    lastError: state.lastError,
    cacheAvailable: Boolean(state.cache),
    cacheFetchedAt: state.cache?.fetchedAt || null
  };
}

module.exports = {
  getSnapshot,
  getStatus
};
