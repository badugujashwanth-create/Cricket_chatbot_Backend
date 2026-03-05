const datasetStore = require('./datasetStore');
const { routeQuestion } = require('./llamaRouter');
const { resolvePlayer, resolveTeam } = require('./entityResolver');
const { executeAction, unavailableResult } = require('./statsService');
const { NOT_AVAILABLE_MESSAGE, SUPPORTED_ACTIONS } = require('./constants');
const { normalizeText } = require('./textUtils');

const YEAR_REGEX = /\b(19\d{2}|20\d{2})\b/;
const MATCH_ID_REGEX = /\b(\d{5,})\b/;
const GENERIC_WORDS = new Set([
  'about',
  'against',
  'and',
  'average',
  'best',
  'compare',
  'economy',
  'for',
  'head',
  'how',
  'in',
  'is',
  'match',
  'matches',
  'most',
  'odi',
  'of',
  'player',
  'players',
  'rate',
  'run',
  'runs',
  'season',
  'show',
  'stats',
  'strike',
  'summary',
  't20',
  'team',
  'teams',
  'test',
  'to',
  'top',
  'versus',
  'vs',
  'what',
  'wicket',
  'wickets',
  'with'
]);

function pickFirst(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const clean = String(value).trim();
    if (clean) return clean;
  }
  return '';
}

function toSeason(value = '') {
  const text = String(value || '').trim();
  if (!text) return '';
  const match = text.match(YEAR_REGEX);
  return match ? match[1] : text;
}

function parseVsSides(question = '') {
  const text = String(question || '').trim();
  if (!text) return null;
  const match =
    text.match(/^compare\s+(.+?)\s+(?:with|and)\s+(.+)$/i) ||
    text.match(/^(.+?)\s+(?:vs|versus)\s+(.+)$/i);
  if (!match) return null;
  return {
    left: match[1].trim(),
    right: match[2].trim()
  };
}

function guessSeason(text = '') {
  const match = String(text || '').match(YEAR_REGEX);
  return match ? match[1] : '';
}

function guessMatchId(text = '') {
  const match = String(text || '').match(MATCH_ID_REGEX);
  return match ? match[1] : '';
}

function guessFormat(text = '') {
  const query = normalizeText(text);
  if (/\bodi\b|\bone day\b/.test(query)) return 'ODI';
  if (/\bt20\b|\bipl\b/.test(query)) return 'T20';
  if (/\btest\b/.test(query)) return 'Test';
  return '';
}

function guessMetric(text = '') {
  const query = normalizeText(text);
  if (/\bwickets?\b/.test(query)) return 'wickets';
  if (/\bstrike rate\b|\bsr\b/.test(query)) return 'strike_rate';
  if (/\beconomy\b/.test(query)) return 'economy';
  return 'runs';
}

function removeGenericWords(text = '') {
  const tokens = normalizeText(text).split(' ').filter(Boolean);
  return tokens
    .filter((token) => !GENERIC_WORDS.has(token) && !YEAR_REGEX.test(token))
    .join(' ')
    .trim();
}

function buildResponse(result = {}) {
  return {
    answer: String(result.answer || NOT_AVAILABLE_MESSAGE),
    data: result.data || {},
    followups: Array.isArray(result.followups) ? result.followups : []
  };
}

function normalizeRoute(route = {}) {
  const entities = route?.entities && typeof route.entities === 'object' ? route.entities : {};
  const merged = { ...entities, ...route };
  const action = SUPPORTED_ACTIONS.includes(merged.action) ? merged.action : 'not_supported';
  return {
    action,
    player: pickFirst(merged.player, merged.query),
    player1: pickFirst(merged.player1),
    player2: pickFirst(merged.player2),
    team: pickFirst(merged.team, merged.query),
    team1: pickFirst(merged.team1),
    team2: pickFirst(merged.team2),
    season: pickFirst(merged.season),
    format: pickFirst(merged.format),
    match_id: pickFirst(merged.match_id),
    date: pickFirst(merged.date),
    metric: pickFirst(merged.metric, merged.list_type),
    term: pickFirst(merged.term),
    limit: pickFirst(merged.limit),
    min_balls: pickFirst(merged.min_balls),
    min_overs: pickFirst(merged.min_overs)
  };
}

function resolvePlayerBest(query = '') {
  const result = resolvePlayer(query);
  if (result.status === 'resolved') return result.item;
  if (result.status === 'clarify' && Array.isArray(result.choices) && result.choices.length) {
    const retry = resolvePlayer(result.choices[0]);
    if (retry.status === 'resolved') return retry.item;
  }
  return null;
}

function resolveTeamBest(query = '') {
  const result = resolveTeam(query);
  if (result.status === 'resolved') return result.item;
  if (result.status === 'clarify' && Array.isArray(result.choices) && result.choices.length) {
    const retry = resolveTeam(result.choices[0]);
    if (retry.status === 'resolved') return retry.item;
  }
  return null;
}

function runPlayerAction(action, route, question) {
  const playerQuery = pickFirst(route.player, removeGenericWords(question), question);
  const player = resolvePlayerBest(playerQuery);
  if (!player) return unavailableResult();

  const filters = {
    season: toSeason(pickFirst(route.season, guessSeason(question))),
    format: pickFirst(route.format, guessFormat(question))
  };
  return executeAction(action, { playerId: player.id, filters });
}

function runTeamStats(route, question) {
  const teamQuery = pickFirst(route.team, removeGenericWords(question), question);
  const team = resolveTeamBest(teamQuery);
  if (!team) return unavailableResult();

  const filters = {
    season: toSeason(pickFirst(route.season, guessSeason(question))),
    format: pickFirst(route.format, guessFormat(question))
  };
  return executeAction('team_stats', { teamId: team.id, filters });
}

function runMatchSummary(route, question) {
  const matchId = pickFirst(route.match_id, guessMatchId(question));
  if (matchId) return executeAction('match_summary', { matchId });

  const vs = parseVsSides(question) || {};
  const leftQuery = pickFirst(route.team1, vs.left);
  const rightQuery = pickFirst(route.team2, vs.right);
  const left = leftQuery ? resolveTeamBest(leftQuery) : null;
  const right = rightQuery ? resolveTeamBest(rightQuery) : null;
  if (!left && !right) return unavailableResult();

  return executeAction('match_summary', {
    team1: left?.name || '',
    team2: right?.name || '',
    season: toSeason(pickFirst(route.season, guessSeason(question))),
    date: pickFirst(route.date)
  });
}

function runComparePlayers(route, question) {
  const vs = parseVsSides(question) || {};
  const leftQuery = pickFirst(route.player1, vs.left);
  const rightQuery = pickFirst(route.player2, vs.right);
  const left = resolvePlayerBest(leftQuery);
  const right = resolvePlayerBest(rightQuery);
  if (!left || !right) return unavailableResult();

  const filters = {
    season: toSeason(pickFirst(route.season, guessSeason(question))),
    format: pickFirst(route.format, guessFormat(question))
  };
  return executeAction('compare_players', {
    playerId1: left.id,
    playerId2: right.id,
    filters
  });
}

function runHeadToHead(route, question) {
  const vs = parseVsSides(question) || {};
  const leftQuery = pickFirst(route.team1, vs.left);
  const rightQuery = pickFirst(route.team2, vs.right);
  const left = resolveTeamBest(leftQuery);
  const right = resolveTeamBest(rightQuery);
  if (!left || !right) return unavailableResult();

  const filters = {
    season: toSeason(pickFirst(route.season, guessSeason(question))),
    format: pickFirst(route.format, guessFormat(question))
  };
  return executeAction('head_to_head', {
    team1Name: left.name,
    team2Name: right.name,
    filters
  });
}

function runTopPlayers(route, question) {
  return executeAction('top_players', {
    entities: {
      metric: pickFirst(route.metric, guessMetric(question)),
      season: toSeason(pickFirst(route.season, guessSeason(question))),
      format: pickFirst(route.format, guessFormat(question)),
      limit: Number(route.limit || 10),
      min_balls: Number(route.min_balls || 200),
      min_overs: Number(route.min_overs || 20)
    }
  });
}

function runGlossary(route, question) {
  return executeAction('glossary', {
    term: pickFirst(route.term, question)
  });
}

async function handleQuery({ question = '', query = '' } = {}) {
  const text = pickFirst(question, query);
  if (!text) {
    return {
      statusCode: 400,
      response: {
        answer: 'Please type your question.',
        data: {},
        followups: []
      }
    };
  }

  const cache = await datasetStore.waitUntilReady(60000);
  if (!cache) {
    return {
      statusCode: 503,
      response: {
        answer: 'Data is loading. Please try again in a moment.',
        data: {},
        followups: []
      }
    };
  }

  const route = normalizeRoute(await routeQuestion(text, {}));
  let result = unavailableResult();

  if (route.action === 'player_stats' || route.action === 'player_season_stats') {
    result = runPlayerAction(route.action, route, text);
  } else if (route.action === 'team_stats') {
    result = runTeamStats(route, text);
  } else if (route.action === 'match_summary') {
    result = runMatchSummary(route, text);
  } else if (route.action === 'compare_players') {
    result = runComparePlayers(route, text);
  } else if (route.action === 'head_to_head') {
    result = runHeadToHead(route, text);
  } else if (route.action === 'top_players') {
    result = runTopPlayers(route, text);
  } else if (route.action === 'glossary') {
    result = runGlossary(route, text);
  }

  return {
    statusCode: 200,
    response: buildResponse(result)
  };
}

module.exports = {
  handleQuery
};
