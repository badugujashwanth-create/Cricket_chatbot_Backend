const { getCollectionDocs, queryVectorDb } = require('./chromaService');
const { normalizeText, similarityScore, tokenize } = require('./textUtils');
const { buildPlayerAliases, getCanonicalPlayerName } = require('./playerMaster');

const INDEX_TTL_MS = 10 * 60 * 1000;
const PLAYER_LIMIT = 20000;
const TEAM_LIMIT = 2000;
const MATCH_LIMIT = 30000;

const cacheState = {
  players: { expiresAt: 0, items: [] },
  teams: { expiresAt: 0, items: [] },
  matches: { expiresAt: 0, items: [] }
};

function titleCaseMetric(metric = '') {
  return String(metric || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .trim();
}

function formatNumber(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '-';
  if (Number.isInteger(numeric)) return numeric.toLocaleString('en-IN');
  return numeric.toLocaleString('en-IN', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2
  });
}

function parseNumber(text = '', pattern) {
  const match = String(text || '').match(pattern);
  if (!match) return 0;
  const numeric = Number(String(match[1] || '').replace(/,/g, ''));
  return Number.isFinite(numeric) ? numeric : 0;
}

function parseString(text = '', pattern) {
  const match = String(text || '').match(pattern);
  return match ? String(match[1] || '').trim() : '';
}

function parsePlayerProfile(row = {}) {
  const document = String(row.document || '');
  const metadata = row.metadata && typeof row.metadata === 'object' ? row.metadata : {};
  const playerName =
    String(metadata.player || '').trim() ||
    parseString(document, /Cricket player profile\s+(.+?)\./i);

  return {
    id: String(row.id || '').trim(),
    type: 'player_profile',
    name: playerName,
    canonical_name: getCanonicalPlayerName(playerName) || playerName,
    team: String(metadata.team || '').trim(),
    role: String(metadata.role || '').trim(),
    matches: Number(metadata.matches || 0),
    runs: Number(metadata.runs || 0),
    wickets: Number(metadata.wickets || 0),
    average: parseNumber(document, /batting average\s+([0-9.]+)/i),
    strike_rate: Number(metadata.strike_rate || 0),
    economy: Number(metadata.economy || 0),
    fours: parseNumber(document, /Fours\s+([0-9,]+)/i),
    sixes: parseNumber(document, /sixes\s+([0-9,]+)/i),
    document
  };
}

function parseTeamSummary(row = {}) {
  const document = String(row.document || '');
  const metadata = row.metadata && typeof row.metadata === 'object' ? row.metadata : {};
  const teamName =
    String(metadata.team || '').trim() ||
    parseString(document, /Cricket team summary for\s+(.+?)\./i);

  return {
    id: String(row.id || '').trim(),
    type: 'team_summary',
    name: teamName,
    matches: Number(metadata.matches || 0),
    wins: Number(metadata.wins || 0),
    win_rate: Number(metadata.win_rate || 0),
    losses: parseNumber(document, /losses\s+([0-9,]+)/i),
    no_result: parseNumber(document, /no result\s+([0-9,]+)/i),
    runs: parseNumber(document, /Total batting runs\s+([0-9,]+)/i),
    strike_rate: parseNumber(document, /team strike rate\s+([0-9.]+)/i),
    document
  };
}

function parseMatchSummary(row = {}) {
  const document = String(row.document || '');
  const metadata = row.metadata && typeof row.metadata === 'object' ? row.metadata : {};
  const teamMatch = document.match(/Teams:\s*(.+?)\s+vs\s+(.+?)\./i);

  return {
    id: String(metadata.match_id || row.id || '').trim(),
    type: 'match_summary',
    date: String(metadata.date || '').trim(),
    season: String(metadata.season || '').trim(),
    format: String(metadata.match_type || '').trim(),
    venue: String(metadata.venue || '').trim(),
    city: String(metadata.city || '').trim(),
    winner: String(metadata.winner || '').trim(),
    team1: teamMatch ? String(teamMatch[1] || '').trim() : '',
    team2: teamMatch ? String(teamMatch[2] || '').trim() : '',
    innings_summary: parseString(document, /Innings summary:\s+(.+?)\.\s+Top batters:/i),
    document
  };
}

function isFresh(bucket) {
  return bucket.expiresAt > Date.now() && Array.isArray(bucket.items) && bucket.items.length > 0;
}

function setCache(bucketName, items) {
  cacheState[bucketName] = {
    expiresAt: Date.now() + INDEX_TTL_MS,
    items
  };
  return items;
}

async function loadPlayerProfiles(force = false) {
  if (!force && isFresh(cacheState.players)) return cacheState.players.items;
  const payload = await getCollectionDocs({ doc_type: 'player_profile' }, { limit: PLAYER_LIMIT });
  const items = (payload.docs || [])
    .map(parsePlayerProfile)
    .filter((item) => item.name);
  return setCache('players', items);
}

async function loadTeamSummaries(force = false) {
  if (!force && isFresh(cacheState.teams)) return cacheState.teams.items;
  const payload = await getCollectionDocs({ doc_type: 'team_summary' }, { limit: TEAM_LIMIT });
  const items = (payload.docs || [])
    .map(parseTeamSummary)
    .filter((item) => item.name);
  return setCache('teams', items);
}

async function loadMatchSummaries(force = false) {
  if (!force && isFresh(cacheState.matches)) return cacheState.matches.items;
  const payload = await getCollectionDocs({ doc_type: 'match_summary' }, { limit: MATCH_LIMIT });
  const items = (payload.docs || [])
    .map(parseMatchSummary)
    .filter((item) => item.id);
  return setCache('matches', items);
}

function teamAliasMap(name = '') {
  const clean = String(name || '').trim();
  const normalized = normalizeText(clean);
  const aliases = new Set([clean]);
  if (!clean) return aliases;

  const tokens = tokenize(clean);
  if (tokens.length) {
    aliases.add(tokens.map((token) => token[0]).join('').toUpperCase());
  }

  if (normalized.includes('royal challengers bengaluru') || normalized.includes('royal challengers bangalore')) {
    aliases.add('RCB');
    aliases.add('Royal Challengers Bangalore');
    aliases.add('Royal Challengers Bengaluru');
  }
  if (normalized.includes('mumbai indians')) aliases.add('MI');
  if (normalized.includes('chennai super kings')) aliases.add('CSK');
  if (normalized.includes('kolkata knight riders')) aliases.add('KKR');
  if (normalized.includes('sunrisers hyderabad')) aliases.add('SRH');
  if (normalized.includes('rajasthan royals')) aliases.add('RR');
  if (normalized.includes('delhi capitals') || normalized.includes('delhi daredevils')) {
    aliases.add('DC');
    aliases.add('Delhi Daredevils');
    aliases.add('Delhi Capitals');
  }
  if (normalized.includes('punjab kings') || normalized.includes('kings xi punjab')) {
    aliases.add('PBKS');
    aliases.add('KXIP');
    aliases.add('Punjab Kings');
    aliases.add('Kings XI Punjab');
  }
  if (normalized.includes('gujarat titans')) aliases.add('GT');
  if (normalized.includes('lucknow super giants')) aliases.add('LSG');
  if (normalized === 'australia') aliases.add('AUS');
  if (normalized === 'india') aliases.add('IND');
  if (normalized === 'england') aliases.add('ENG');
  if (normalized === 'pakistan') aliases.add('PAK');
  if (normalized === 'new zealand') aliases.add('NZ');
  if (normalized === 'south africa') aliases.add('SA');
  if (normalized === 'west indies') aliases.add('WI');

  return aliases;
}

function scoreCandidate(query = '', aliases = []) {
  const normalizedQuery = normalizeText(query);
  if (!normalizedQuery) return 0;

  let best = 0;
  for (const alias of aliases) {
    const candidate = String(alias || '').trim();
    const normalizedCandidate = normalizeText(candidate);
    if (!normalizedCandidate) continue;

    let score = similarityScore(normalizedQuery, normalizedCandidate);
    if (normalizedCandidate === normalizedQuery) score += 1;
    else if (normalizedCandidate.startsWith(normalizedQuery)) score += 0.45;
    else if (normalizedCandidate.includes(normalizedQuery)) score += 0.2;

    const queryTokens = tokenize(normalizedQuery);
    const candidateTokens = tokenize(normalizedCandidate);
    if (queryTokens.length === 1 && candidateTokens.includes(queryTokens[0])) {
      score += 0.3;
    }
    if (queryTokens.length === 1 && candidateTokens[candidateTokens.length - 1] === queryTokens[0]) {
      score += 0.4;
    }

    best = Math.max(best, score);
  }
  return best;
}

async function searchPlayers(query = '', limit = 20) {
  const cleanQuery = String(query || '').trim();
  if (!cleanQuery) return [];
  const players = await loadPlayerProfiles();
  const scored = players
    .map((player) => {
      const aliases = new Set([
        player.name,
        player.canonical_name,
        ...buildPlayerAliases(player.canonical_name || player.name)
      ]);
      return {
        ...player,
        score: scoreCandidate(cleanQuery, [...aliases])
      };
    })
    .filter((player) => player.score >= 0.45)
    .sort((left, right) => right.score - left.score || right.runs - left.runs || right.matches - left.matches);

  return scored.slice(0, Math.max(1, Number(limit) || 20));
}

async function resolvePlayer(query = '') {
  const matches = await searchPlayers(query, 5);
  if (!matches.length) return { found: false, item: null, score: 0 };
  return {
    found: matches[0].score >= 0.55,
    item: matches[0],
    score: matches[0].score,
    matches
  };
}

async function getPlayerById(id = '') {
  const cleanId = String(id || '').trim();
  if (!cleanId) return null;
  const players = await loadPlayerProfiles();
  return players.find((player) => String(player.id || '').trim() === cleanId) || null;
}

async function searchTeams(query = '', limit = 20) {
  const cleanQuery = String(query || '').trim();
  if (!cleanQuery) return [];
  const teams = await loadTeamSummaries();
  const scored = teams
    .map((team) => ({
      ...team,
      score: scoreCandidate(cleanQuery, [...teamAliasMap(team.name)])
    }))
    .filter((team) => team.score >= 0.45)
    .sort((left, right) => right.score - left.score || right.wins - left.wins || right.matches - left.matches);

  return scored.slice(0, Math.max(1, Number(limit) || 20));
}

async function resolveTeam(query = '') {
  const matches = await searchTeams(query, 5);
  if (!matches.length) return { found: false, item: null, score: 0 };
  return {
    found: matches[0].score >= 0.55,
    item: matches[0],
    score: matches[0].score,
    matches
  };
}

async function getTeamById(id = '') {
  const cleanId = String(id || '').trim();
  if (!cleanId) return null;
  const teams = await loadTeamSummaries();
  return teams.find((team) => String(team.id || '').trim() === cleanId) || null;
}

async function getTopPlayersByMetric(metric = 'runs', { limit = 10 } = {}) {
  const players = await loadPlayerProfiles();
  const key = String(metric || '').trim();
  const ranked = [...players]
    .sort((left, right) => Number(right[key] || 0) - Number(left[key] || 0) || right.matches - left.matches)
    .slice(0, Math.max(1, Number(limit) || 10))
    .map((player, index) => ({
      rank: index + 1,
      player: player.canonical_name || player.name,
      team: player.team,
      value: Number(player[key] || 0),
      matches: player.matches,
      runs: player.runs,
      average: player.average,
      strike_rate: player.strike_rate,
      wickets: player.wickets,
      sixes: player.sixes,
      fours: player.fours
    }));

  return ranked;
}

async function getTopPlayersForTeam(teamName = '', metric = 'runs', { limit = 10 } = {}) {
  const players = await loadPlayerProfiles();
  const cleanTeam = normalizeText(teamName);
  const key = String(metric || '').trim();
  const ranked = players
    .filter((player) => normalizeText(player.team) === cleanTeam)
    .sort((left, right) => Number(right[key] || 0) - Number(left[key] || 0) || right.matches - left.matches)
    .slice(0, Math.max(1, Number(limit) || 10))
    .map((player, index) => ({
      rank: index + 1,
      player: player.canonical_name || player.name,
      team: player.team,
      value: Number(player[key] || 0),
      matches: player.matches,
      runs: player.runs,
      average: player.average,
      strike_rate: player.strike_rate,
      wickets: player.wickets,
      economy: player.economy,
      sixes: player.sixes,
      fours: player.fours
    }));

  return ranked;
}

async function findMatchesByTeams(teamNames = [], { limit = 10, year = '', format = '' } = {}) {
  const matches = await loadMatchSummaries();
  const cleanTeams = teamNames.map((name) => String(name || '').trim()).filter(Boolean);

  return matches
    .filter((match) => {
      const teams = [match.team1, match.team2];
      const teamMatch = cleanTeams.every((name) => teams.includes(name));
      if (!teamMatch) return false;
      if (year && !String(match.date || '').startsWith(String(year))) return false;
      if (format && normalizeText(match.format) !== normalizeText(format)) return false;
      return true;
    })
    .sort((left, right) => String(right.date || '').localeCompare(String(left.date || '')))
    .slice(0, Math.max(1, Number(limit) || 10));
}

function matchHasTeam(match = {}, teamName = '') {
  const cleanTeam = normalizeText(teamName);
  if (!cleanTeam) return false;
  return [match.team1, match.team2].some((team) => normalizeText(team) === cleanTeam);
}

async function findMatchesForTeam(teamName = '', { limit = 10, offset = 0, year = '', format = '' } = {}) {
  const matches = await loadMatchSummaries();
  const cleanTeam = String(teamName || '').trim();

  return matches
    .filter((match) => {
      if (cleanTeam && !matchHasTeam(match, cleanTeam)) return false;
      if (year && !String(match.date || '').startsWith(String(year))) return false;
      if (format && normalizeText(match.format) !== normalizeText(format)) return false;
      return true;
    })
    .sort((left, right) => String(right.date || '').localeCompare(String(left.date || '')))
    .slice(Math.max(0, Number(offset) || 0), Math.max(0, Number(offset) || 0) + Math.max(1, Number(limit) || 10));
}

async function getMatchById(id = '') {
  const cleanId = String(id || '').trim();
  if (!cleanId) return null;
  const matches = await loadMatchSummaries();
  return matches.find((match) => String(match.id || '').trim() === cleanId) || null;
}

async function findRelevantVectorContext(query = '', limit = 5) {
  const payload = await queryVectorDb(query, { k: limit });
  return Array.isArray(payload.results) ? payload.results : [];
}

module.exports = {
  formatNumber,
  titleCaseMetric,
  loadPlayerProfiles,
  loadTeamSummaries,
  loadMatchSummaries,
  searchPlayers,
  resolvePlayer,
  getPlayerById,
  searchTeams,
  resolveTeam,
  getTeamById,
  getTopPlayersByMetric,
  getTopPlayersForTeam,
  getMatchById,
  findMatchesByTeams,
  findMatchesForTeam,
  findRelevantVectorContext
};
