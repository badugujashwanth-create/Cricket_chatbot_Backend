require('./loadEnv');

const espn = require('espn-cricket-api');
const { normalizeText, similarityScore } = require('./textUtils');

const DEFAULT_TIMEOUT_MS = Number(process.env.ESPN_TIMEOUT_MS || 30000);
const CACHE_TTL_MS = Number(process.env.ESPN_CACHE_TTL_MS || 30 * 60 * 1000);
const responseCache = new Map();

class EspnServiceError extends Error {
  constructor(message, statusCode = 502, details = {}) {
    super(message);
    this.name = 'EspnServiceError';
    this.statusCode = statusCode;
    this.details = details;
  }
}

function now() {
  return Date.now();
}

function getCachedValue(key = '') {
  const entry = responseCache.get(key);
  if (!entry) return null;
  if (entry.expiresAt <= now()) {
    responseCache.delete(key);
    return null;
  }
  return entry.value;
}

function setCachedValue(key = '', value, ttlMs = CACHE_TTL_MS) {
  if (!key || !ttlMs) return value;
  responseCache.set(key, {
    value,
    expiresAt: now() + ttlMs
  });
  return value;
}

async function withTimeout(loader, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const timeout = Number(timeoutMs) || DEFAULT_TIMEOUT_MS;
  let timeoutId = null;
  try {
    return await Promise.race([
      Promise.resolve().then(loader),
      new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new EspnServiceError('ESPN request timed out.', 504, { provider: 'espn' }));
        }, timeout);
      })
    ]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

function pickFirst(...values) {
  for (const value of values) {
    const clean = String(value || '').trim();
    if (clean) return clean;
  }
  return '';
}

function parseIdFromSearchItem(item = {}) {
  const directId = pickFirst(item.id, item.objectId, item.playerId);
  if (directId) return directId;
  const links = [item.url, item.link, item.href, item.mobileUrl]
    .map((value) => String(value || '').trim())
    .filter(Boolean);
  for (const link of links) {
    const match = link.match(/\/player\/(?:[^/]+\/)?(\d+)\b/i) || link.match(/\b(\d{4,})\b/);
    if (match?.[1]) return match[1];
  }
  return '';
}

function normalizeSearchItem(item = {}) {
  return {
    source: 'espn',
    id: parseIdFromSearchItem(item),
    name: pickFirst(item.displayName, item.name, item.title),
    team_slug: pickFirst(item.teamSlug, item.team_slug),
    team: pickFirst(item.subtitle, item.team, item.description),
    raw: item
  };
}

function sumFormatStats(formats = [], extractor) {
  return formats.reduce((sum, formatKey) => sum + Number(extractor(formatKey) || 0), 0);
}

function safeFormatStats(record = {}) {
  return record && typeof record === 'object' ? record : {};
}

function buildCareerSummary(batting = {}, bowling = {}) {
  const formats = ['tests', 'odis', 't20is'];
  const battingSum = (key) => sumFormatStats(formats, (formatKey) => safeFormatStats(batting[formatKey])[key]);
  const bowlingSum = (key) => sumFormatStats(formats, (formatKey) => safeFormatStats(bowling[formatKey])[key]);

  const innings = battingSum('innings');
  const notouts = battingSum('notouts');
  const dismissals = Math.max(innings - notouts, 0);
  const runs = battingSum('runs');
  const ballsFaced = battingSum('ballsFaced');
  const bowlingBalls = bowlingSum('balls');
  const bowlingRuns = bowlingSum('runs');
  const wickets = bowlingSum('wickets');

  return {
    matches: battingSum('matches') || bowlingSum('matches'),
    runs,
    average: dismissals > 0 ? Number((runs / dismissals).toFixed(2)) : 0,
    strike_rate: ballsFaced > 0 ? Number(((runs * 100) / ballsFaced).toFixed(2)) : 0,
    wickets,
    economy: bowlingBalls > 0 ? Number(((bowlingRuns * 6) / bowlingBalls).toFixed(2)) : 0,
    fours: battingSum('fours'),
    sixes: battingSum('sixes')
  };
}

function normalizePlayerDetails(details = {}, searchItem = {}) {
  const batting = safeFormatStats(details.batting);
  const bowling = safeFormatStats(details.bowling);
  return {
    source: 'espn',
    id: pickFirst(searchItem.id, details.id),
    name: pickFirst(details.name, searchItem.name),
    full_name: pickFirst(details.fullName, details.name, searchItem.name),
    team: pickFirst(details.teamName, searchItem.team),
    team_slug: pickFirst(searchItem.team_slug, details.teamSlug),
    born: pickFirst(details.born),
    major_teams: Array.isArray(details.majorTeams) ? details.majorTeams : [],
    role: pickFirst(details.playingRole),
    batting_style: pickFirst(details.battingStyle),
    bowling_style: pickFirst(details.bowlingStyle),
    image_url: pickFirst(details.avatar),
    stats_by_format: {
      batting,
      bowling
    },
    career_summary: buildCareerSummary(batting, bowling),
    raw: details
  };
}

function rankSearchItems(items = [], query = '') {
  const normalizedQuery = normalizeText(query);
  return items
    .map((item) => {
      const normalizedName = normalizeText(item.name);
      let score = similarityScore(normalizedQuery, normalizedName);
      if (normalizedName === normalizedQuery) score += 1;
      else if (normalizedName.startsWith(normalizedQuery)) score += 0.4;
      else if (normalizedName.includes(normalizedQuery)) score += 0.2;
      return { ...item, score };
    })
    .sort((left, right) => right.score - left.score || left.name.localeCompare(right.name));
}

async function searchPlayers({ q = '', limit = 5 } = {}) {
  const query = String(q || '').trim();
  if (!query) {
    throw new EspnServiceError('Query parameter "q" is required.', 400, { provider: 'espn' });
  }

  const cacheKey = `espn:search:${normalizeText(query)}:${limit}`;
  const cached = getCachedValue(cacheKey);
  if (cached) return cached;

  try {
    const results = await withTimeout(() => espn.search({ query, limit: Math.max(1, Number(limit) || 5) }));
    const items = rankSearchItems(
      (Array.isArray(results) ? results : []).map(normalizeSearchItem).filter((item) => item.id && item.name),
      query
    ).slice(0, Math.max(1, Number(limit) || 5));

    return setCachedValue(cacheKey, {
      provider: 'espn',
      source: 'external',
      query,
      items
    });
  } catch (error) {
    if (error instanceof EspnServiceError) throw error;
    throw new EspnServiceError(error?.message || 'Failed to search ESPN Cricinfo.', 502, {
      provider: 'espn'
    });
  }
}

async function getPlayerCareer({ playerId = '', teamSlug = 'ci', searchItem = null } = {}) {
  const cleanPlayerId = String(playerId || '').trim();
  if (!cleanPlayerId) {
    throw new EspnServiceError('Player id is required.', 400, { provider: 'espn' });
  }
  const cleanTeamSlug = String(teamSlug || 'ci').trim() || 'ci';
  const cacheKey = `espn:player:${cleanPlayerId}:${cleanTeamSlug}`;
  const cached = getCachedValue(cacheKey);
  if (cached) return cached;

  try {
    const details = await withTimeout(() =>
      espn.getPlayerDetails({
        playerID: cleanPlayerId,
        teamSlug: cleanTeamSlug
      })
    );

    const payload = {
      provider: 'espn',
      source: 'external',
      player: normalizePlayerDetails(details || {}, searchItem || { id: cleanPlayerId, team_slug: cleanTeamSlug })
    };
    return setCachedValue(cacheKey, payload);
  } catch (error) {
    if (error instanceof EspnServiceError) throw error;
    throw new EspnServiceError(error?.message || 'Failed to fetch ESPN player details.', 502, {
      provider: 'espn',
      player_id: cleanPlayerId
    });
  }
}

async function getPlayerCareerByQuery(query = '', { limit = 3 } = {}) {
  const searchResult = await searchPlayers({ q: query, limit });
  const top = Array.isArray(searchResult.items) ? searchResult.items[0] : null;
  if (!top?.id) {
    throw new EspnServiceError('No ESPN player matched that query.', 404, {
      provider: 'espn',
      query
    });
  }
  return getPlayerCareer({
    playerId: top.id,
    teamSlug: top.team_slug || 'ci',
    searchItem: top
  });
}

module.exports = {
  EspnServiceError,
  searchPlayers,
  getPlayerCareer,
  getPlayerCareerByQuery
};
