require('./loadEnv');

const axios = require('axios');

const { normalizeText, similarityScore } = require('./textUtils');

const BASE_URL = 'https://api.cricapi.com/v1/';
const API_KEY = String(process.env.CRICAPI_KEY || '').trim();
const REQUEST_TIMEOUT_MS = Number(process.env.CRICAPI_TIMEOUT_MS || 10000);
const CRICBUZZ_ENABLED = !['0', 'false', 'no'].includes(
  normalizeText(process.env.CRICBUZZ_ENABLED || 'true')
);
const CRICBUZZ_RAPIDAPI_KEY = String(process.env.CRICBUZZ_RAPIDAPI_KEY || '').trim();
const CRICBUZZ_RAPIDAPI_HOST = String(
  process.env.CRICBUZZ_RAPIDAPI_HOST || 'cricbuzz-cricket.p.rapidapi.com'
).trim();
const CRICBUZZ_BASE_URL = String(
  process.env.CRICBUZZ_BASE_URL || `https://${CRICBUZZ_RAPIDAPI_HOST}/`
).trim();
const CRICBUZZ_TIMEOUT_MS = Number(process.env.CRICBUZZ_TIMEOUT_MS || 12000);
const CRICBUZZ_SEARCH_PATH = String(
  process.env.CRICBUZZ_SEARCH_PATH || 'stats/v1/player/search'
).trim();
const CRICBUZZ_PLAYER_STATS_PATH = String(
  process.env.CRICBUZZ_PLAYER_STATS_PATH || 'stats/v1/player/{playerId}'
).trim();
const CRICBUZZ_PLAYER_BIO_PATH = String(
  process.env.CRICBUZZ_PLAYER_BIO_PATH || 'stats/v1/player/{playerId}/bio'
).trim();

const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 50;

const CACHE_TTL = {
  live: 30 * 1000,
  players: 15 * 60 * 1000,
  playerInfo: 60 * 60 * 1000,
  scorecard: 60 * 60 * 1000,
  schedule: 15 * 60 * 1000,
  series: 30 * 60 * 1000,
  seriesInfo: 15 * 60 * 1000,
  cricbuzzSearch: 15 * 60 * 1000,
  cricbuzzPlayerStats: 15 * 60 * 1000,
  cricbuzzPlayerBio: 60 * 60 * 1000
};

const responseCache = new Map();

function logCricbuzzDebug(message = '', payload = null) {
  if (String(process.env.NODE_ENV || '').trim() === 'production') return;
  if (payload === null || payload === undefined) {
    console.log(`[cricbuzz] ${message}`);
    return;
  }
  console.log(`[cricbuzz] ${message}`, payload);
}

class CricApiError extends Error {
  constructor(message, statusCode = 502, details = {}) {
    super(message);
    this.name = 'CricApiError';
    this.statusCode = statusCode;
    this.details = details;
  }
}

class CricApiConfigError extends CricApiError {
  constructor(message = 'CricAPI key is not configured.') {
    super(message, 503, {
      provider: 'cricapi',
      source: 'external'
    });
    this.name = 'CricApiConfigError';
  }
}

class CricbuzzApiError extends Error {
  constructor(message, statusCode = 502, details = {}) {
    super(message);
    this.name = 'CricbuzzApiError';
    this.statusCode = statusCode;
    this.details = details;
  }
}

class CricbuzzApiConfigError extends CricbuzzApiError {
  constructor(message = 'Cricbuzz RapidAPI key is not configured.') {
    super(message, 503, {
      provider: 'cricbuzz',
      source: 'external'
    });
    this.name = 'CricbuzzApiConfigError';
  }
}

function now() {
  return Date.now();
}

function toPositiveInteger(value, fallback = DEFAULT_LIMIT, { min = 0, max = MAX_LIMIT } = {}) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

function toBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  const normalized = normalizeText(value);
  if (['1', 'true', 'yes', 'y'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'n'].includes(normalized)) return false;
  return fallback;
}

function buildUrl(pathname, params = {}) {
  const url = new URL(String(pathname || '').replace(/^\/+/, ''), BASE_URL);
  url.searchParams.set('apikey', API_KEY);

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue;
    url.searchParams.set(key, String(value));
  }

  return url;
}

function buildCricbuzzUrl(pathname, params = {}) {
  const base = String(CRICBUZZ_BASE_URL || `https://${CRICBUZZ_RAPIDAPI_HOST}/`).replace(/\/+$/, '/');
  const url = new URL(String(pathname || '').replace(/^\/+/, ''), base);

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue;
    url.searchParams.set(key, String(value));
  }

  return url;
}

function replacePathTokens(pathname = '', replacements = {}) {
  return String(pathname || '').replace(/\{([^}]+)\}/g, (_, token) => {
    const value = replacements[token];
    return value === undefined || value === null ? '' : encodeURIComponent(String(value));
  });
}

function getCachedValue(key) {
  const entry = responseCache.get(key);
  if (!entry) return null;
  if (entry.expiresAt <= now()) {
    responseCache.delete(key);
    return null;
  }
  return entry.value;
}

function setCachedValue(key, value, ttlMs) {
  if (!ttlMs || ttlMs <= 0) return;
  responseCache.set(key, {
    value,
    expiresAt: now() + ttlMs
  });
}

function buildMeta(info = {}, extras = {}) {
  const hitsToday = Number(info.hitsToday);
  const hitsLimit = Number(info.hitsLimit);
  return {
    provider: 'cricapi',
    source: 'external',
    query_time_ms: Number(info.queryTime || 0) || 0,
    cache_hit: Boolean(Number(info.cache || 0)),
    offset_rows: Number.isFinite(Number(info.offsetRows)) ? Number(info.offsetRows) : null,
    total_rows: Number.isFinite(Number(info.totalRows)) ? Number(info.totalRows) : null,
    hits_today: Number.isFinite(hitsToday) ? hitsToday : null,
    hits_limit: Number.isFinite(hitsLimit) ? hitsLimit : null,
    hits_remaining:
      Number.isFinite(hitsToday) && Number.isFinite(hitsLimit) ? Math.max(0, hitsLimit - hitsToday) : null,
    ...extras
  };
}

function stringifyUpstreamPayload(payload) {
  if (payload === undefined || payload === null) return '';
  if (typeof payload === 'string') return payload;
  try {
    return JSON.stringify(payload);
  } catch (_) {
    return String(payload);
  }
}

async function requestCricApi(pathname, params = {}, { cacheKey, ttlMs = 0 } = {}) {
  if (!API_KEY) {
    throw new CricApiConfigError();
  }

  const cached = cacheKey ? getCachedValue(cacheKey) : null;
  if (cached) return cached;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    const response = await fetch(buildUrl(pathname, params), {
      method: 'GET',
      signal: controller.signal
    });

    if (!response.ok) {
      throw new CricApiError(`CricAPI request failed (${response.status}).`, 502, {
        provider: 'cricapi',
        source: 'external',
        upstream_status: response.status
      });
    }

    const payload = await response.json().catch(() => {
      throw new CricApiError('CricAPI returned invalid JSON.', 502, {
        provider: 'cricapi',
        source: 'external'
      });
    });

    if (payload?.status && String(payload.status).toLowerCase() !== 'success') {
      throw new CricApiError(payload.reason || payload.message || 'CricAPI request failed.', 502, {
        provider: 'cricapi',
        source: 'external',
        upstream_status: payload.status
      });
    }

    setCachedValue(cacheKey, payload, ttlMs);
    return payload;
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new CricApiError('CricAPI request timed out.', 504, {
        provider: 'cricapi',
        source: 'external'
      });
    }
    if (error instanceof CricApiError) throw error;
    throw new CricApiError(error.message || 'Failed to reach CricAPI.', 502, {
      provider: 'cricapi',
      source: 'external'
    });
  } finally {
    clearTimeout(timeout);
  }
}

async function requestCricbuzz(pathname, params = {}, { cacheKey, ttlMs = 0 } = {}) {
  if (!CRICBUZZ_ENABLED) {
    throw new CricbuzzApiConfigError('Cricbuzz enrichment is disabled by configuration.');
  }

  if (!CRICBUZZ_RAPIDAPI_KEY) {
    throw new CricbuzzApiConfigError();
  }

  const cached = cacheKey ? getCachedValue(cacheKey) : null;
  if (cached) return cached;

  try {
    const rapidApiKey = String(process.env.CRICBUZZ_RAPIDAPI_KEY || CRICBUZZ_RAPIDAPI_KEY).trim();
    const rapidApiHost = String(process.env.CRICBUZZ_RAPIDAPI_HOST || CRICBUZZ_RAPIDAPI_HOST).trim();
    const response = await axios.get(buildCricbuzzUrl(pathname, params).toString(), {
      headers: {
        'X-RapidAPI-Key': rapidApiKey,
        'X-RapidAPI-Host': rapidApiHost
      },
      timeout: CRICBUZZ_TIMEOUT_MS,
      validateStatus: () => true
    });
    logCricbuzzDebug('API response status', {
      status: response.status,
      path: pathname
    });

    if (response.status < 200 || response.status >= 300) {
      const payloadText = stringifyUpstreamPayload(response.data);
      const normalizedPayloadText = normalizeText(payloadText);
      const subscriptionRequired =
        response.status === 403 &&
        (normalizedPayloadText.includes('not subscribed') || normalizedPayloadText.includes('subscription'));
      throw new CricbuzzApiError(
        subscriptionRequired
          ? 'Cricbuzz subscription unavailable for this endpoint.'
          : `Cricbuzz request failed (${response.status}).${payloadText ? ` ${payloadText}` : ''}`,
        subscriptionRequired ? 503 : 502,
        {
          provider: 'cricbuzz',
          source: 'external',
          upstream_status: response.status,
          subscription_required: subscriptionRequired,
          upstream_message: payloadText
        }
      );
    }

    let payload = response.data;
    if (typeof payload === 'string') {
      try {
        payload = payload ? JSON.parse(payload) : {};
      } catch (_) {
        throw new CricbuzzApiError('Cricbuzz returned invalid JSON.', 502, {
          provider: 'cricbuzz',
          source: 'external'
        });
      }
    }
    if (!payload || typeof payload !== 'object') {
      throw new CricbuzzApiError('Cricbuzz returned invalid JSON.', 502, {
        provider: 'cricbuzz',
        source: 'external'
      });
    }

    setCachedValue(cacheKey, payload, ttlMs);
    return payload;
  } catch (error) {
    if (axios.isAxiosError(error) && error.code === 'ECONNABORTED') {
      throw new CricbuzzApiError('Cricbuzz request timed out.', 504, {
        provider: 'cricbuzz',
        source: 'external'
      });
    }
    if (error instanceof CricbuzzApiError) throw error;
    throw new CricbuzzApiError(error.message || 'Failed to reach Cricbuzz.', 502, {
      provider: 'cricbuzz',
      source: 'external'
    });
  }
}

function toArray(value) {
  return Array.isArray(value) ? value : [];
}

function flattenNestedArrayCandidates(payload = {}) {
  const directCandidates = [
    payload?.data,
    payload?.player,
    payload?.players,
    payload?.playerList,
    payload?.player_list,
    payload?.results,
    payload?.items
  ];
  for (const candidate of directCandidates) {
    if (Array.isArray(candidate)) return candidate;
  }
  for (const candidate of directCandidates) {
    if (candidate && typeof candidate === 'object') {
      const firstArray = Object.values(candidate).find((value) => Array.isArray(value));
      if (Array.isArray(firstArray)) return firstArray;
    }
  }
  return [];
}

function unwrapNestedEntity(value = {}) {
  if (value && typeof value === 'object') {
    if (value.player && typeof value.player === 'object') return value.player;
    if (value.data && typeof value.data === 'object' && !Array.isArray(value.data)) return value.data;
  }
  return value && typeof value === 'object' ? value : {};
}

function maybeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function firstNonEmptyString(...values) {
  for (const value of values) {
    const clean = String(value || '').trim();
    if (clean) return clean;
  }
  return '';
}

function normalizeCricbuzzSearchItem(item = {}) {
  const source = unwrapNestedEntity(item);
  return {
    source: 'cricbuzz',
    id: firstNonEmptyString(source.id, source.playerId, source.pid),
    name: firstNonEmptyString(source.name, source.fullName, source.playerName, source.nickName),
    team: firstNonEmptyString(source.teamName, source.team, source.intlTeam, source.domesticTeam),
    country: firstNonEmptyString(source.country, source.countryName, source.intlTeam),
    role: firstNonEmptyString(source.role, source.playingRole),
    batting_style: firstNonEmptyString(source.battingStyle, source.batStyle),
    bowling_style: firstNonEmptyString(source.bowlingStyle, source.bowlStyle),
    image_url: firstNonEmptyString(source.imageUrl, source.image_url),
    raw: source
  };
}

function extractCareerSections(payload = {}) {
  const root = unwrapNestedEntity(payload);
  return toArray(root.career || root.stats || root.values || root.records || payload.career || payload.stats);
}

function buildStatsFromEntries(entries = []) {
  const stats = {};
  const statLabels = {
    matches: ['matches', 'mat'],
    innings: ['innings', 'inns', 'inn'],
    runs: ['runs'],
    average: ['average', 'avg'],
    strike_rate: ['strike rate', 'strike_rate', 'sr'],
    high_score: ['high score', 'highest score', 'hs'],
    wickets: ['wickets', 'wkts'],
    economy: ['economy', 'econ', 'eco'],
    best_bowling: ['best bowling', 'best bowling innings', 'bbi']
  };

  for (const entry of entries) {
    const label = normalizeText(
      firstNonEmptyString(entry.label, entry.name, entry.title, entry.stat, entry.type)
    );
    const value = entry.value ?? entry.val ?? entry.text ?? entry.number;
    if (!label || value === undefined || value === null || value === '') continue;

    for (const [targetKey, aliases] of Object.entries(statLabels)) {
      if (aliases.some((alias) => label === alias || label.includes(alias))) {
        stats[targetKey] = maybeNumber(value) ?? String(value).trim();
        break;
      }
    }
  }

  return stats;
}

function normalizeCricbuzzPlayerProfile({ searchItem = {}, statsPayload = {}, bioPayload = {} } = {}) {
  const search = normalizeCricbuzzSearchItem(searchItem);
  const statsRoot = unwrapNestedEntity(statsPayload);
  const bioRoot = unwrapNestedEntity(bioPayload);
  const statsEntries = [
    ...extractCareerSections(statsPayload),
    ...toArray(statsRoot.headers).flatMap((header) => toArray(header.values)),
    ...toArray(statsRoot.values),
    ...toArray(bioRoot.values)
  ].filter((entry) => entry && typeof entry === 'object');
  const stats = buildStatsFromEntries(statsEntries);

  return {
    source: 'cricbuzz',
    id: firstNonEmptyString(search.id, statsRoot.id, statsRoot.playerId, bioRoot.id),
    name: firstNonEmptyString(search.name, statsRoot.name, statsRoot.playerName, bioRoot.name),
    team: firstNonEmptyString(search.team, statsRoot.teamName, bioRoot.teamName),
    country: firstNonEmptyString(search.country, statsRoot.country, bioRoot.country),
    role: firstNonEmptyString(search.role, statsRoot.role, bioRoot.role, bioRoot.playingRole),
    batting_style: firstNonEmptyString(search.batting_style, statsRoot.batStyle, bioRoot.batStyle, bioRoot.battingStyle),
    bowling_style: firstNonEmptyString(search.bowling_style, statsRoot.bowlStyle, bioRoot.bowlStyle, bioRoot.bowlingStyle),
    image_url: firstNonEmptyString(search.image_url, statsRoot.imageUrl, bioRoot.imageUrl),
    bio: firstNonEmptyString(
      bioRoot.bio,
      bioRoot.description,
      bioRoot.doB ? `Born ${bioRoot.doB}` : '',
      bioRoot.birthPlace ? `Birthplace: ${bioRoot.birthPlace}` : ''
    ),
    stats,
    raw: {
      stats: statsPayload,
      bio: bioPayload
    }
  };
}

function normalizeScoreLine(score = {}) {
  return {
    runs: Number(score.r || 0),
    wickets: Number.isFinite(Number(score.w)) ? Number(score.w) : null,
    overs: Number.isFinite(Number(score.o)) ? Number(score.o) : null,
    inning: String(score.inning || '')
  };
}

function normalizeMatch(match = {}) {
  return {
    source: 'cricapi',
    id: String(match.id || ''),
    name: String(match.name || ''),
    match_type: String(match.matchType || ''),
    status: String(match.status || ''),
    venue: String(match.venue || ''),
    date: String(match.date || ''),
    date_time_gmt: String(match.dateTimeGMT || ''),
    teams: Array.isArray(match.teams) ? match.teams : [],
    series_id: String(match.series_id || ''),
    score: Array.isArray(match.score) ? match.score.map(normalizeScoreLine) : [],
    live: Boolean(match.matchStarted && !match.matchEnded),
    match_started: Boolean(match.matchStarted),
    match_ended: Boolean(match.matchEnded),
    has_squad: Boolean(match.hasSquad),
    fantasy_enabled: Boolean(match.fantasyEnabled),
    ball_by_ball_enabled: Boolean(match.bbbEnabled)
  };
}

function normalizeScorecardPlayer(player = {}) {
  return {
    id: String(player.id || ''),
    name: String(player.name || '')
  };
}

function normalizeScorecardBatting(row = {}) {
  return {
    batsman: normalizeScorecardPlayer(row.batsman || {}),
    dismissal: String(row.dismissal || ''),
    dismissal_text: String(row['dismissal-text'] || row.dismissal_text || ''),
    bowler: normalizeScorecardPlayer(row.bowler || {}),
    catcher: normalizeScorecardPlayer(row.catcher || {}),
    runs: Number(row.r || 0),
    balls: Number(row.b || 0),
    fours: Number(row['4s'] || 0),
    sixes: Number(row['6s'] || 0),
    strike_rate: Number.isFinite(Number(row.sr)) ? Number(row.sr) : null
  };
}

function normalizeScorecardBowling(row = {}) {
  return {
    bowler: normalizeScorecardPlayer(row.bowler || {}),
    overs: Number.isFinite(Number(row.o)) ? Number(row.o) : 0,
    maidens: Number(row.m || 0),
    runs_conceded: Number(row.r || 0),
    wickets: Number(row.w || 0),
    noballs: Number(row.nb || 0),
    wides: Number(row.wd || 0),
    economy: Number.isFinite(Number(row.eco)) ? Number(row.eco) : null
  };
}

function normalizeScorecardInning(row = {}) {
  return {
    inning: String(row.inning || ''),
    batting: Array.isArray(row.batting) ? row.batting.map(normalizeScorecardBatting) : [],
    bowling: Array.isArray(row.bowling) ? row.bowling.map(normalizeScorecardBowling) : [],
    extras: row.extras && typeof row.extras === 'object' ? row.extras : {},
    totals: row.totals && typeof row.totals === 'object' ? row.totals : {}
  };
}

function normalizePlayer(player = {}) {
  return {
    source: 'cricapi',
    id: String(player.id || ''),
    name: String(player.name || ''),
    country: String(player.country || ''),
    image_url: String(player.playerImg || '')
  };
}

function normalizeSeries(series = {}) {
  return {
    source: 'cricapi',
    id: String(series.id || ''),
    name: String(series.name || ''),
    start_date: String(series.startDate || series.startdate || ''),
    end_date: String(series.endDate || series.enddate || ''),
    formats: {
      odi: Number(series.odi || 0),
      t20: Number(series.t20 || 0),
      test: Number(series.test || 0),
      squads: Number(series.squads || 0)
    },
    matches: Number(series.matches || 0)
  };
}

function filterMatches(items = [], { team = '', matchType = '', seriesId = '' } = {}) {
  const teamQuery = normalizeText(team);
  const typeQuery = normalizeText(matchType);
  const seriesQuery = normalizeText(seriesId);

  return items.filter((item) => {
    if (teamQuery) {
      const matchedTeam = (item.teams || []).some((name) => normalizeText(name).includes(teamQuery));
      if (!matchedTeam) return false;
    }

    if (typeQuery && normalizeText(item.match_type) !== typeQuery) {
      return false;
    }

    if (seriesQuery && normalizeText(item.series_id) !== seriesQuery) {
      return false;
    }

    return true;
  });
}

function rankByQuery(items = [], query = '', key = 'name') {
  const normalizedQuery = normalizeText(query);
  if (!normalizedQuery) return items;

  return items
    .map((item) => {
      const value = normalizeText(item[key] || '');
      let score = similarityScore(normalizedQuery, value);
      if (value === normalizedQuery) score += 1;
      else if (value.startsWith(normalizedQuery)) score += 0.4;
      else if (value.includes(normalizedQuery)) score += 0.2;
      return { item, score };
    })
    .filter((row) => row.score >= 0.3)
    .sort((left, right) => right.score - left.score || left.item[key].localeCompare(right.item[key]))
    .map((row) => row.item);
}

function sortByDateAsc(items = []) {
  return [...items].sort((left, right) => {
    const a = Date.parse(left.date_time_gmt || left.date || '');
    const b = Date.parse(right.date_time_gmt || right.date || '');
    if (Number.isNaN(a) && Number.isNaN(b)) return 0;
    if (Number.isNaN(a)) return 1;
    if (Number.isNaN(b)) return -1;
    return a - b;
  });
}

function sortByDateDesc(items = []) {
  return sortByDateAsc(items).reverse();
}

async function getLiveScores({ offset = 0, limit = DEFAULT_LIMIT, includeRecent = false, team = '', matchType = '' } = {}) {
  const safeOffset = toPositiveInteger(offset, 0, { min: 0, max: 5000 });
  const safeLimit = toPositiveInteger(limit, DEFAULT_LIMIT);

  const payload = await requestCricApi(
    'currentMatches',
    { offset: safeOffset },
    {
      cacheKey: `cricapi:currentMatches:${safeOffset}`,
      ttlMs: CACHE_TTL.live
    }
  );

  let items = Array.isArray(payload.data) ? payload.data.map(normalizeMatch) : [];
  items = filterMatches(items, { team, matchType });

  if (!includeRecent) {
    items = items.filter((item) => item.live);
  } else {
    items = sortByDateDesc(items);
  }

  return {
    provider: 'cricapi',
    source: 'external',
    filters: {
      offset: safeOffset,
      limit: safeLimit,
      include_recent: Boolean(includeRecent),
      team: String(team || ''),
      match_type: String(matchType || '')
    },
    items: items.slice(0, safeLimit),
    meta: buildMeta(payload.info, {
      returned: Math.min(items.length, safeLimit)
    })
  };
}

async function searchPlayers({ q = '', offset = 0, limit = DEFAULT_LIMIT } = {}) {
  const query = String(q || '').trim();
  if (!query) {
    throw new CricApiError('Query parameter "q" is required.', 400, {
      provider: 'cricapi',
      source: 'external'
    });
  }

  const safeOffset = toPositiveInteger(offset, 0, { min: 0, max: 5000 });
  const safeLimit = toPositiveInteger(limit, DEFAULT_LIMIT);

  const payload = await requestCricApi(
    'players',
    { offset: safeOffset, search: query },
    {
      cacheKey: `cricapi:players:${safeOffset}:${normalizeText(query)}`,
      ttlMs: CACHE_TTL.players
    }
  );

  const ranked = rankByQuery(
    (Array.isArray(payload.data) ? payload.data : []).map(normalizePlayer),
    query
  );

  return {
    provider: 'cricapi',
    source: 'external',
    query,
    items: ranked.slice(0, safeLimit),
    meta: buildMeta(payload.info, {
      offset: safeOffset,
      returned: Math.min(ranked.length, safeLimit)
    })
  };
}

async function getPlayerInfo(playerId = '') {
  const id = String(playerId || '').trim();
  if (!id) {
    throw new CricApiError('Player id is required.', 400, {
      provider: 'cricapi',
      source: 'external'
    });
  }

  const payload = await requestCricApi(
    'players_info',
    { id },
    {
      cacheKey: `cricapi:playerInfo:${id}`,
      ttlMs: CACHE_TTL.playerInfo
    }
  );

  return {
    provider: 'cricapi',
    source: 'external',
    player: normalizePlayer(payload.data || {}),
    meta: buildMeta(payload.info)
  };
}

async function getMatchScorecard(matchId = '') {
  const id = String(matchId || '').trim();
  if (!id) {
    throw new CricApiError('Match id is required.', 400, {
      provider: 'cricapi',
      source: 'external'
    });
  }

  const payload = await requestCricApi(
    'match_scorecard',
    { offset: 0, id },
    {
      cacheKey: `cricapi:match_scorecard:${id}`,
      ttlMs: CACHE_TTL.scorecard
    }
  );

  const data = payload.data && typeof payload.data === 'object' ? payload.data : {};
  return {
    provider: 'cricapi',
    source: 'external',
    match: {
      source: 'cricapi',
      id: String(data.id || ''),
      name: String(data.name || ''),
      match_type: String(data.matchType || ''),
      status: String(data.status || ''),
      venue: String(data.venue || ''),
      date: String(data.date || ''),
      date_time_gmt: String(data.dateTimeGMT || ''),
      teams: Array.isArray(data.teams) ? data.teams : [],
      team_info: Array.isArray(data.teamInfo) ? data.teamInfo : [],
      score: Array.isArray(data.score) ? data.score.map(normalizeScoreLine) : [],
      toss_winner: String(data.tossWinner || ''),
      toss_choice: String(data.tossChoice || ''),
      match_winner: String(data.matchWinner || ''),
      series_id: String(data.series_id || ''),
      match_started: Boolean(data.matchStarted),
      match_ended: Boolean(data.matchEnded),
      scorecard: Array.isArray(data.scorecard) ? data.scorecard.map(normalizeScorecardInning) : []
    },
    meta: buildMeta(payload.info)
  };
}

async function getMatchSchedule({
  offset = 0,
  limit = DEFAULT_LIMIT,
  team = '',
  matchType = '',
  seriesId = '',
  upcomingOnly = true
} = {}) {
  const safeOffset = toPositiveInteger(offset, 0, { min: 0, max: 5000 });
  const safeLimit = toPositiveInteger(limit, DEFAULT_LIMIT);

  const payload = await requestCricApi(
    'matches',
    { offset: safeOffset },
    {
      cacheKey: `cricapi:matches:${safeOffset}`,
      ttlMs: CACHE_TTL.schedule
    }
  );

  let items = Array.isArray(payload.data) ? payload.data.map(normalizeMatch) : [];
  items = filterMatches(items, { team, matchType, seriesId });

  if (upcomingOnly) {
    items = items.filter((item) => !item.match_started);
    items = sortByDateAsc(items);
  }

  return {
    provider: 'cricapi',
    source: 'external',
    filters: {
      offset: safeOffset,
      limit: safeLimit,
      team: String(team || ''),
      match_type: String(matchType || ''),
      series_id: String(seriesId || ''),
      upcoming_only: Boolean(upcomingOnly)
    },
    items: items.slice(0, safeLimit),
    meta: buildMeta(payload.info, {
      returned: Math.min(items.length, safeLimit)
    })
  };
}

async function getSeriesList({ q = '', offset = 0, limit = DEFAULT_LIMIT } = {}) {
  const query = String(q || '').trim();
  const safeOffset = toPositiveInteger(offset, 0, { min: 0, max: 5000 });
  const safeLimit = toPositiveInteger(limit, DEFAULT_LIMIT);

  const payload = await requestCricApi(
    'series',
    { offset: safeOffset },
    {
      cacheKey: `cricapi:series:${safeOffset}`,
      ttlMs: CACHE_TTL.series
    }
  );

  let items = (Array.isArray(payload.data) ? payload.data : []).map(normalizeSeries);
  if (query) {
    items = rankByQuery(items, query);
  }

  return {
    provider: 'cricapi',
    source: 'external',
    query,
    items: items.slice(0, safeLimit),
    meta: buildMeta(payload.info, {
      offset: safeOffset,
      returned: Math.min(items.length, safeLimit)
    })
  };
}

async function getSeriesInfo(seriesId = '') {
  const id = String(seriesId || '').trim();
  if (!id) {
    throw new CricApiError('Series id is required.', 400, {
      provider: 'cricapi',
      source: 'external'
    });
  }

  const payload = await requestCricApi(
    'series_info',
    { id },
    {
      cacheKey: `cricapi:seriesInfo:${id}`,
      ttlMs: CACHE_TTL.seriesInfo
    }
  );

  return {
    provider: 'cricapi',
    source: 'external',
    series: normalizeSeries(payload.data?.info || {}),
    matches: (Array.isArray(payload.data?.matchList) ? payload.data.matchList : []).map(normalizeMatch),
    meta: buildMeta(payload.info)
  };
}

async function searchCricbuzzPlayers({ q = '', limit = DEFAULT_LIMIT } = {}) {
  const query = String(q || '').trim();
  if (!query) {
    throw new CricbuzzApiError('Query parameter "q" is required.', 400, {
      provider: 'cricbuzz',
      source: 'external'
    });
  }

  const safeLimit = toPositiveInteger(limit, DEFAULT_LIMIT);
  const payload = await requestCricbuzz(
    CRICBUZZ_SEARCH_PATH,
    { q: query },
    {
      cacheKey: `cricbuzz:search:${normalizeText(query)}:${safeLimit}`,
      ttlMs: CACHE_TTL.cricbuzzSearch
    }
  );

  const ranked = flattenNestedArrayCandidates(payload)
    .map(normalizeCricbuzzSearchItem)
    .filter((item) => item.id || item.name)
    .sort((left, right) => {
      const leftScore = similarityScore(normalizeText(query), normalizeText(left.name || ''));
      const rightScore = similarityScore(normalizeText(query), normalizeText(right.name || ''));
      return rightScore - leftScore || String(left.name || '').localeCompare(String(right.name || ''));
    });

  return {
    provider: 'cricbuzz',
    source: 'external',
    query,
    items: ranked.slice(0, safeLimit),
    raw: payload
  };
}

async function getCricbuzzPlayerStats(playerId = '') {
  const id = String(playerId || '').trim();
  if (!id) {
    throw new CricbuzzApiError('Player id is required.', 400, {
      provider: 'cricbuzz',
      source: 'external'
    });
  }

  return requestCricbuzz(
    replacePathTokens(CRICBUZZ_PLAYER_STATS_PATH, { playerId: id }),
    {},
    {
      cacheKey: `cricbuzz:player-stats:${id}`,
      ttlMs: CACHE_TTL.cricbuzzPlayerStats
    }
  );
}

async function getCricbuzzPlayerBio(playerId = '') {
  const id = String(playerId || '').trim();
  if (!id) {
    throw new CricbuzzApiError('Player id is required.', 400, {
      provider: 'cricbuzz',
      source: 'external'
    });
  }

  return requestCricbuzz(
    replacePathTokens(CRICBUZZ_PLAYER_BIO_PATH, { playerId: id }),
    {},
    {
      cacheKey: `cricbuzz:player-bio:${id}`,
      ttlMs: CACHE_TTL.cricbuzzPlayerBio
    }
  );
}

async function getCricbuzzPlayerCardByName(name = '') {
  const query = String(name || '').trim();
  if (!query) {
    throw new CricbuzzApiError('Player name is required.', 400, {
      provider: 'cricbuzz',
      source: 'external'
    });
  }

  const searchResult = await searchCricbuzzPlayers({ q: query, limit: 3 });
  const topPlayer = searchResult.items[0];
  if (!topPlayer) {
    return {
      provider: 'cricbuzz',
      source: 'external',
      query,
      player: null
    };
  }

  const [statsPayload, bioPayload] = await Promise.all([
    getCricbuzzPlayerStats(topPlayer.id).catch(() => ({})),
    getCricbuzzPlayerBio(topPlayer.id).catch(() => ({}))
  ]);

  return {
    provider: 'cricbuzz',
    source: 'external',
    query,
    player: normalizeCricbuzzPlayerProfile({
      searchItem: topPlayer,
      statsPayload,
      bioPayload
    })
  };
}

module.exports = {
  CricApiError,
  CricApiConfigError,
  CricbuzzApiError,
  CricbuzzApiConfigError,
  toBoolean,
  toPositiveInteger,
  getLiveScores,
  searchPlayers,
  getPlayerInfo,
  getMatchScorecard,
  getMatchSchedule,
  getSeriesList,
  getSeriesInfo,
  searchCricbuzzPlayers,
  getCricbuzzPlayerStats,
  getCricbuzzPlayerBio,
  getCricbuzzPlayerCardByName
};
