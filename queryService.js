const datasetStore = require('./datasetStore');
const { routeQuestion, extractJsonFromText } = require('./llamaRouter');
const { callLlama } = require('./llamaClient');
const { queryVectorDb } = require('./chromaService');
const { resolvePlayer, resolveTeam } = require('./entityResolver');
const { getPlayerProfile } = require('./playerProfileService');
const { executeAction, unavailableResult } = require('./statsService');
const { NOT_AVAILABLE_MESSAGE, SUPPORTED_ACTIONS } = require('./constants');
const {
  CricApiConfigError,
  getLiveScores,
  searchPlayers: searchCricApiPlayers,
  getPlayerInfo,
  getMatchSchedule,
  getSeriesList,
  getSeriesInfo
} = require('./cricApiService');
const { normalizeText } = require('./textUtils');
const { cleanEntitySegment, parseVsSides } = require('./queryParser');

const YEAR_REGEX = /\b(19\d{2}|20\d{2})\b/;
const MATCH_ID_REGEX = /\b(\d{5,})\b/;
const LIVE_QUERY_REGEX = /\b(live|current|ongoing|now|today|score|scores|scorecard|latest)\b/;
const SCHEDULE_QUERY_REGEX = /\b(schedule|scheduled match(?:es)?|fixture|fixtures|upcoming|next match|next game|tomorrow|when is|who is playing today)\b/;
const SERIES_QUERY_REGEX = /\b(series|tournament|world cup|champions trophy|asia cup|ipl|bbl|psl|wpl)\b/;
const PLAYER_PROFILE_REGEX = /\b(profile|who is|player info|player information|country|bio)\b/;
const FACT_LOOKUP_REGEX =
  /\b(average|averages|economy|form|head to head|match|matches|most|rank|ranking|recent|record|records|run|runs|score|scores|scorecard|season|seasons|series|stat|stats|strike rate|summary|table|top|venue|venues|wicket|wickets|win|wins)\b/;
const COMMON_CAPITALIZED_WORDS = new Set([
  'What',
  'When',
  'Where',
  'Which',
  'Who',
  'Why',
  'How',
  'Show',
  'Tell',
  'Give',
  'Latest',
  'Live',
  'Recent',
  'Current',
  'Form',
  'Score'
]);

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

function uniqueNonEmpty(values = []) {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
}

function formatStatValue(value) {
  if (value === null || value === undefined || value === '') return '-';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return '-';
    if (Number.isInteger(value)) return value.toLocaleString('en-US');
    return Number(value.toFixed(2)).toLocaleString('en-US', {
      minimumFractionDigits: 0,
      maximumFractionDigits: 2
    });
  }
  return String(value).trim() || '-';
}

function extractTaggedLine(text = '', labels = []) {
  const lines = String(text || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return '';
  const pattern = labels.length
    ? new RegExp(`^(?:${labels.map((label) => escapeRegex(label)).join('|')}):\\s*(.+)$`, 'i')
    : /^([A-Za-z ]+):\s*(.+)$/;
  for (const line of lines) {
    const match = line.match(pattern);
    if (match?.[1]) {
      return String(match[1]).trim();
    }
  }
  return '';
}

function deriveSummaryText(answer = '', details = {}) {
  const fullText = String(answer || '').trim();
  const taggedSummary =
    extractTaggedLine(fullText, ['Summary', 'Conclusion', 'Insight', 'Status']) ||
    extractTaggedLine(String(details.summary || ''), ['Summary', 'Conclusion', 'Insight', 'Status']);
  if (taggedSummary) return taggedSummary;

  const lines = fullText
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !/^[A-Za-z ]+:$/.test(line));

  const title = String(details.title || '').trim();
  const subtitle = String(details.subtitle || '').trim();
  const contentLine = lines.find((line) => line !== title && line !== subtitle);
  if (!contentLine) return NOT_AVAILABLE_MESSAGE;
  return contentLine.length > 220 ? `${contentLine.slice(0, 217).trim()}...` : contentLine;
}

function buildProviderStatus(errors = []) {
  const messages = uniqueNonEmpty(errors);
  if (!messages.length) return null;

  const joined = normalizeText(messages.join(' '));
  if (joined.includes('hits today exceeded hits limit') || joined.includes('hits limit')) {
    return {
      state: 'rate_limited',
      title: 'Live feed limit reached',
      message:
        'Live match and schedule updates are temporarily unavailable because the CricAPI daily request limit has been reached.'
    };
  }

  if (joined.includes('timed out')) {
    return {
      state: 'timeout',
      title: 'Live feed timed out',
      message: 'Live match and schedule updates are temporarily unavailable because the live data provider timed out.'
    };
  }

  if (joined.includes('not configured')) {
    return {
      state: 'not_configured',
      title: 'Live feed not configured',
      message: 'Live match and schedule updates are unavailable because the CricAPI key is not configured.'
    };
  }

  return {
    state: 'unavailable',
    title: 'Live feed unavailable',
    message: 'Live match and schedule updates are temporarily unavailable from the external data provider.'
  };
}

function buildKeyStats(details = {}) {
  const type = String(details.type || '').trim();

  if (type === 'player_stats') {
    return [
      { label: 'Matches', value: Number(details.stats?.matches || 0) },
      { label: 'Runs', value: Number(details.stats?.runs || 0) },
      { label: 'Average', value: Number(details.stats?.average || 0) },
      { label: 'Strike Rate', value: Number(details.stats?.strike_rate || 0) }
    ];
  }

  if (type === 'team_stats') {
    return [
      { label: 'Matches', value: Number(details.stats?.matches || 0) },
      { label: 'Wins', value: Number(details.stats?.wins || 0) },
      { label: 'Win Rate', value: Number(details.stats?.win_rate || 0) },
      { label: 'Average Score', value: Number(details.stats?.average_score || 0) }
    ];
  }

  if (type === 'compare_players') {
    return [
      {
        label: 'Runs',
        left: Number(details.left?.stats?.runs || 0),
        right: Number(details.right?.stats?.runs || 0)
      },
      {
        label: 'Average',
        left: Number(details.left?.stats?.average || 0),
        right: Number(details.right?.stats?.average || 0)
      },
      {
        label: 'Strike Rate',
        left: Number(details.left?.stats?.strike_rate || 0),
        right: Number(details.right?.stats?.strike_rate || 0)
      }
    ];
  }

  if (type === 'head_to_head') {
    return [
      { label: 'Matches', value: Number(details.stats?.matches || 0) },
      { label: `${details.team1 || 'Team 1'} Wins`, value: Number(details.stats?.wins_team_a || 0) },
      { label: `${details.team2 || 'Team 2'} Wins`, value: Number(details.stats?.wins_team_b || 0) },
      { label: 'No Result', value: Number(details.stats?.no_result || 0) }
    ];
  }

  if (type === 'top_players') {
    return (Array.isArray(details.rows) ? details.rows : []).slice(0, 3).map((row) => ({
      label: `#${row.rank || ''} ${row.player || 'Player'}`,
      value: formatStatValue(row.value)
    }));
  }

  if (type === 'live_update') {
    const stats = [
      { label: 'Upcoming Matches', value: Array.isArray(details.upcoming_matches) ? details.upcoming_matches.length : 0 },
      { label: 'Recent Matches', value: Array.isArray(details.recent_matches) ? details.recent_matches.length : 0 }
    ];
    if (details.provider_status?.title) {
      stats.unshift({
        label: 'Feed Status',
        value: details.provider_status.title
      });
    }
    return stats;
  }

  return [];
}

function buildInsights(details = {}, summary = '', answer = '') {
  const type = String(details.type || '').trim();

  if (type === 'player_stats' && details.player?.name) {
    return uniqueNonEmpty([
      `${details.player.name} has ${Number(details.stats?.runs || 0).toLocaleString('en-US')} runs in the verified archive.`,
      Number(details.stats?.average || 0) > 40 ? `${details.player.name} shows high batting consistency by average.` : '',
      Number(details.stats?.strike_rate || 0) > 100 ? `${details.player.name} scores at an aggressive strike rate.` : ''
    ]).slice(0, 3);
  }

  if (type === 'compare_players' && details.left?.name && details.right?.name) {
    const leftAverage = Number(details.left?.stats?.average || 0);
    const rightAverage = Number(details.right?.stats?.average || 0);
    const leftStrikeRate = Number(details.left?.stats?.strike_rate || 0);
    const rightStrikeRate = Number(details.right?.stats?.strike_rate || 0);
    return uniqueNonEmpty([
      leftAverage === rightAverage ? '' : `${leftAverage > rightAverage ? details.left.name : details.right.name} leads on batting average.`,
      leftStrikeRate === rightStrikeRate ? '' : `${leftStrikeRate > rightStrikeRate ? details.left.name : details.right.name} scores faster by strike rate.`
    ]).slice(0, 3);
  }

  if (type === 'top_players' && Array.isArray(details.rows) && details.rows[0]?.player) {
    return [`${details.rows[0].player} leads this leaderboard in the current query scope.`];
  }

  if (type === 'live_update') {
    if (details.provider_status?.message) {
      return [details.provider_status.message];
    }
    if (Array.isArray(details.upcoming_matches) && details.upcoming_matches.length) {
      return [`${details.upcoming_matches.length} upcoming matches are available from the live feed.`];
    }
  }

  const note = extractAnalystNote(answer) || extractAnalystNote(summary);
  if (note) return [note];
  return summary ? [summary] : [];
}

function extractAnalystNote(text = '') {
  const lines = String(text || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.find((line) => /^(summary|conclusion|insight|status):/i.test(line)) || '';
}

function isUsableEntityHint(value = '', question = '') {
  const clean = String(value || '').trim();
  if (!clean) return false;
  if (clean.length > 60) return false;
  if (normalizeText(clean) === normalizeText(question)) return false;
  if (clean.split(/\s+/).length > 6) return false;
  return true;
}

function extractCapitalizedPhrases(question = '') {
  const matches = String(question || '').match(/\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b/g) || [];
  return uniqueNonEmpty(
    matches.filter((phrase) => {
      const clean = String(phrase || '').trim();
      if (!clean) return false;
      if (COMMON_CAPITALIZED_WORDS.has(clean)) return false;
      return true;
    })
  );
}

function deriveEntityHints(question = '', route = {}) {
  const capitalizedPhrases = extractCapitalizedPhrases(question);
  return {
    playerHints: uniqueNonEmpty(
      [route.player, route.player1, route.player2]
        .filter((value) => isUsableEntityHint(value, question))
        .concat(capitalizedPhrases.filter((phrase) => phrase.split(/\s+/).length >= 2))
    ).slice(0, 2),
    teamHints: uniqueNonEmpty(
      [route.team, route.team1, route.team2]
        .filter((value) => isUsableEntityHint(value, question))
        .concat(capitalizedPhrases.filter((phrase) => phrase.split(/\s+/).length === 1))
    ).slice(0, 2)
  };
}

function toSeason(value = '') {
  const text = String(value || '').trim();
  if (!text) return '';
  const match = text.match(YEAR_REGEX);
  return match ? match[1] : text;
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
  if (/\bt20\b|\bit20\b|\bipl\b/.test(query)) return 'T20';
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

function buildEntityCandidates(...values) {
  return uniqueNonEmpty(
    values.flatMap((value) => {
      const raw = String(value || '').trim();
      if (!raw) return [];
      const cleaned = cleanEntitySegment(raw);
      const reduced = removeGenericWords(raw);
      return [cleaned, reduced, raw];
    })
  );
}

function resolutionWeight(status = '') {
  if (status === 'resolved') return 3;
  if (status === 'clarify') return 2;
  if (status === 'not_found') return 1;
  return 0;
}

function resolveEntityWithFallback(entityType, candidates = []) {
  const queries = buildEntityCandidates(...candidates);
  let best = {
    query: queries[0] || '',
    resolution: { status: 'missing' }
  };

  for (const query of queries) {
    const resolution = resolveEntityStrict(entityType, query);
    if (resolution.status === 'resolved') {
      return { query, resolution };
    }
    if (resolutionWeight(resolution.status) > resolutionWeight(best.resolution.status)) {
      best = { query, resolution };
    }
  }

  return best;
}

function buildResponse(result = {}) {
  const answer = String(result.answer || result.summary || NOT_AVAILABLE_MESSAGE).trim() || NOT_AVAILABLE_MESSAGE;
  const details =
    result.details && typeof result.details === 'object'
      ? result.details
      : result.data && typeof result.data === 'object'
        ? result.data
        : {};
  const suggestions = uniqueNonEmpty(
    Array.isArray(result.suggestions)
      ? result.suggestions
      : Array.isArray(result.followups)
        ? result.followups
        : []
  ).slice(0, 3);
  const type = String(details.type || 'analysis').trim() || 'analysis';
  const title = String(details.title || 'Cricket Intelligence').trim() || 'Cricket Intelligence';
  const summary = deriveSummaryText(answer, details);
  const keyStats = buildKeyStats(details);
  const insights = buildInsights(details, summary, answer);
  return {
    type,
    title,
    answer,
    summary,
    key_stats: keyStats,
    insights,
    details,
    suggestions,
    data: details,
    followups: suggestions
  };
}

function emitStatus(onStatus, payload) {
  if (typeof onStatus !== 'function') return;
  onStatus(payload);
}

function actionStatusMessage(action = '') {
  if (action === 'player_stats' || action === 'player_season_stats') {
    return 'Searching player stats.';
  }
  if (action === 'team_stats') return 'Searching team stats.';
  if (action === 'match_summary') return 'Searching match details.';
  if (action === 'compare_players') return 'Comparing players.';
  if (action === 'head_to_head') return 'Checking head-to-head results.';
  if (action === 'top_players') return 'Searching top performers.';
  if (action === 'glossary') return 'Preparing a short explanation.';
  return 'Searching cricket stats.';
}

function normalizeRoute(route = {}) {
  const entities = route?.entities && typeof route.entities === 'object' ? route.entities : {};
  const merged = { ...entities, ...route };
  const action = SUPPORTED_ACTIONS.includes(merged.action) ? merged.action : 'not_supported';
  return {
    action,
    player: cleanEntitySegment(pickFirst(merged.player, merged.query)),
    player1: cleanEntitySegment(pickFirst(merged.player1)),
    player2: cleanEntitySegment(pickFirst(merged.player2)),
    team: cleanEntitySegment(pickFirst(merged.team, merged.query)),
    team1: cleanEntitySegment(pickFirst(merged.team1)),
    team2: cleanEntitySegment(pickFirst(merged.team2)),
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

function unresolvedEntityResult(entityType, query, resolution = {}) {
  const label = entityType === 'team' ? 'team' : 'player';
  const cleanQuery = String(query || '').trim();
  const choices = Array.isArray(resolution.choices) ? resolution.choices.slice(0, 5) : [];

  if (resolution.status === 'clarify' && choices.length) {
    return {
      answer: `I could not confidently verify the ${label} name "${cleanQuery}" from the dataset. Try one of these exact names: ${choices.join(', ')}.`,
      data: {
        type: 'name_resolution',
        entity: label,
        query: cleanQuery,
        match_status: 'clarify',
        choices
      },
      followups: choices.map((choice) => `Use exact ${label} name: ${choice}`)
    };
  }

  if (resolution.status === 'missing') {
    return {
      answer: `Please provide the exact ${label} name you want me to check.`,
      data: {
        type: 'name_resolution',
        entity: label,
        query: cleanQuery,
        match_status: 'missing',
        choices: []
      },
      followups: []
    };
  }

  return {
    answer: `I could not find that ${label} in the verified dataset.`,
    data: {
      type: 'name_resolution',
      entity: label,
      query: cleanQuery,
      match_status: 'not_found',
      choices
    },
    followups: []
  };
}

function resolveEntityStrict(entityType, query = '') {
  return entityType === 'team' ? resolveTeam(query) : resolvePlayer(query);
}

function runPlayerAction(action, route, question) {
  const { query: playerQuery, resolution } = resolveEntityWithFallback('player', [
    route.player,
    removeGenericWords(question),
    question
  ]);
  if (resolution.status !== 'resolved') {
    return unresolvedEntityResult('player', playerQuery, resolution);
  }
  const player = resolution.item;

  const filters = {
    season: toSeason(pickFirst(route.season, guessSeason(question))),
    format: pickFirst(route.format, guessFormat(question))
  };
  return executeAction(action, { playerId: player.id, filters });
}

function runTeamStats(route, question) {
  const { query: teamQuery, resolution } = resolveEntityWithFallback('team', [
    route.team,
    removeGenericWords(question),
    question
  ]);
  if (resolution.status !== 'resolved') {
    return unresolvedEntityResult('team', teamQuery, resolution);
  }
  const team = resolution.item;

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
  const leftLookup = resolveEntityWithFallback('team', [route.team1, vs.left]);
  const rightLookup = resolveEntityWithFallback('team', [route.team2, vs.right]);
  const leftQuery = leftLookup.query;
  const rightQuery = rightLookup.query;
  const leftResolution = leftQuery ? leftLookup.resolution : null;
  const rightResolution = rightQuery ? rightLookup.resolution : null;

  if (leftResolution && leftResolution.status !== 'resolved') {
    return unresolvedEntityResult('team', leftQuery, leftResolution);
  }
  if (rightResolution && rightResolution.status !== 'resolved') {
    return unresolvedEntityResult('team', rightQuery, rightResolution);
  }

  const left = leftResolution?.item || null;
  const right = rightResolution?.item || null;
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
  const leftLookup = resolveEntityWithFallback('player', [route.player1, vs.left]);
  const rightLookup = resolveEntityWithFallback('player', [route.player2, vs.right]);
  const leftQuery = leftLookup.query;
  const rightQuery = rightLookup.query;
  const leftResolution = leftLookup.resolution;
  if (leftResolution.status !== 'resolved') {
    return unresolvedEntityResult('player', leftQuery, leftResolution);
  }
  const rightResolution = rightLookup.resolution;
  if (rightResolution.status !== 'resolved') {
    return unresolvedEntityResult('player', rightQuery, rightResolution);
  }
  const left = leftResolution.item;
  const right = rightResolution.item;

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
  const leftLookup = resolveEntityWithFallback('team', [route.team1, vs.left]);
  const rightLookup = resolveEntityWithFallback('team', [route.team2, vs.right]);
  const leftQuery = leftLookup.query;
  const rightQuery = rightLookup.query;
  const leftResolution = leftLookup.resolution;
  if (leftResolution.status !== 'resolved') {
    return unresolvedEntityResult('team', leftQuery, leftResolution);
  }
  const rightResolution = rightLookup.resolution;
  if (rightResolution.status !== 'resolved') {
    return unresolvedEntityResult('team', rightQuery, rightResolution);
  }
  const left = leftResolution.item;
  const right = rightResolution.item;

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

function isResolved(resolution = {}) {
  return String(resolution.status || '') === 'resolved';
}

function refineRouteForQuestion(route = {}, question = '') {
  const normalizedQuestion = normalizeText(question);
  const vs = parseVsSides(question);
  const season = toSeason(pickFirst(route.season, guessSeason(question)));
  const format = pickFirst(route.format, guessFormat(question));

  if (vs?.left && vs?.right) {
    const leftPlayer = resolveEntityWithFallback('player', [route.player1, route.player, vs.left]);
    const rightPlayer = resolveEntityWithFallback('player', [route.player2, route.player, vs.right]);
    const leftTeam = resolveEntityWithFallback('team', [route.team1, route.team, vs.left]);
    const rightTeam = resolveEntityWithFallback('team', [route.team2, route.team, vs.right]);

    const playersResolved = isResolved(leftPlayer.resolution) && isResolved(rightPlayer.resolution);
    const teamsResolved = isResolved(leftTeam.resolution) && isResolved(rightTeam.resolution);

    if (teamsResolved && !playersResolved) {
      return {
        ...route,
        action: /\bmatch\b|\bscorecard\b|\bsummary\b/.test(normalizedQuestion) ? 'match_summary' : 'head_to_head',
        player: '',
        player1: '',
        player2: '',
        team: '',
        team1: leftTeam.resolution.item.name,
        team2: rightTeam.resolution.item.name,
        season,
        format
      };
    }

    if (playersResolved && !teamsResolved) {
      return {
        ...route,
        action: 'compare_players',
        team: '',
        team1: '',
        team2: '',
        player1: leftPlayer.resolution.item.canonical_name || leftPlayer.resolution.item.name,
        player2: rightPlayer.resolution.item.canonical_name || rightPlayer.resolution.item.name,
        season,
        format
      };
    }
  }

  if ((route.action === 'player_stats' || route.action === 'player_season_stats') && !vs) {
    const playerLookup = resolveEntityWithFallback('player', [route.player, removeGenericWords(question), question]);
    const teamLookup = resolveEntityWithFallback('team', [route.team, removeGenericWords(question), question]);
    if (isResolved(teamLookup.resolution) && !isResolved(playerLookup.resolution)) {
      return {
        ...route,
        action: 'team_stats',
        player: '',
        team: teamLookup.resolution.item.name,
        season,
        format
      };
    }
  }

  return {
    ...route,
    season,
    format
  };
}

function compactVectorContext(vectorContext = {}) {
  return {
    available: Boolean(vectorContext.available),
    db_dir: String(vectorContext.db_dir || ''),
    warning: String(vectorContext.warning || ''),
    results: Array.isArray(vectorContext.results)
      ? vectorContext.results.slice(0, 5).map((row) => ({
          id: String(row.id || ''),
          distance: Number.isFinite(Number(row.distance)) ? Number(row.distance) : null,
          metadata: row.metadata && typeof row.metadata === 'object' ? row.metadata : {},
          document_preview: String(row.document_preview || '').slice(0, 500)
        }))
      : []
  };
}

function compactMatchForData(match = {}) {
  return {
    id: String(match.id || ''),
    name: String(match.name || ''),
    match_type: String(match.match_type || ''),
    status: String(match.status || ''),
    venue: String(match.venue || ''),
    date: String(match.date || ''),
    teams: Array.isArray(match.teams) ? match.teams.slice(0, 2) : [],
    score: Array.isArray(match.score)
      ? match.score.slice(0, 2).map((item) => ({
          inning: String(item.inning || ''),
          runs: Number(item.runs || 0),
          wickets: item.wickets === null || item.wickets === undefined ? null : Number(item.wickets),
          overs: item.overs === null || item.overs === undefined ? null : Number(item.overs)
        }))
      : [],
    live: Boolean(match.live)
  };
}

function compactPlayerForData(player = {}) {
  return {
    id: String(player.id || ''),
    name: String(player.name || ''),
    country: String(player.country || '')
  };
}

function compactSeriesForData(series = {}) {
  return {
    id: String(series.id || ''),
    name: String(series.name || ''),
    start_date: String(series.start_date || ''),
    end_date: String(series.end_date || ''),
    matches: Number(series.matches || 0)
  };
}

function compactCricApiContext(context = {}) {
  return {
    provider: 'cricapi',
    available: Boolean(context.available),
    errors: Array.isArray(context.errors) ? context.errors.slice(0, 3) : [],
    player_searches: Array.isArray(context.player_searches)
      ? context.player_searches.slice(0, 2).map((item) => ({
          query: item.query,
          items: Array.isArray(item.items) ? item.items.slice(0, 3).map(compactPlayerForData) : []
        }))
      : [],
    player_profiles: Array.isArray(context.player_profiles)
      ? context.player_profiles.slice(0, 2).map(compactPlayerForData)
      : [],
    live_scores: Array.isArray(context.live_scores)
      ? context.live_scores.slice(0, 4).map(compactMatchForData)
      : [],
    schedule: Array.isArray(context.schedule)
      ? context.schedule.slice(0, 4).map(compactMatchForData)
      : [],
    series: Array.isArray(context.series)
      ? context.series.slice(0, 4).map(compactSeriesForData)
      : [],
    series_info: context.series_info
      ? {
          series: compactSeriesForData(context.series_info.series || {}),
          matches: Array.isArray(context.series_info.matches)
            ? context.series_info.matches.slice(0, 4).map(compactMatchForData)
            : []
        }
      : null
  };
}

function compactStructuredResult(structuredContext = {}) {
  const result = structuredContext.result || null;
  return {
    available: Boolean(structuredContext.available),
    cache_ready: Boolean(structuredContext.cache_ready),
    answer: String(result?.answer || ''),
    type: String(result?.data?.type || ''),
    data: result?.data && typeof result.data === 'object' ? result.data : {}
  };
}

function teamHintScore(teamName = '', teamHint = '') {
  const normalizedTeam = normalizeText(teamName);
  const normalizedHint = normalizeText(teamHint);
  if (!normalizedTeam || !normalizedHint) return 0;
  if (normalizedTeam === normalizedHint) return 300;
  if (normalizedTeam.startsWith(`${normalizedHint} `) || normalizedTeam.endsWith(` ${normalizedHint}`)) {
    return 180;
  }
  if (normalizedTeam.includes(normalizedHint)) return 90;
  return 0;
}

function matchQueryScore(match = {}, teamHint = '', question = '') {
  let score = 0;
  const normalizedQuestion = normalizeText(question);
  const teams = Array.isArray(match.teams) ? match.teams : [];

  for (const team of teams) {
    score = Math.max(score, teamHintScore(team, teamHint));
  }

  const normalizedName = normalizeText(match.name || '');
  if (teamHint && normalizedName.includes(normalizeText(teamHint))) {
    score += 25;
  }
  if (normalizedQuestion && normalizedName.includes(normalizedQuestion)) {
    score += 15;
  }
  if (match.live) {
    score += 40;
  }
  return score;
}

function toDateValue(value = '') {
  const parsed = Date.parse(String(value || ''));
  return Number.isFinite(parsed) ? parsed : 0;
}

function sortMatchesForQuestion(items = [], teamHint = '', question = '', { ascending = false } = {}) {
  return [...items].sort((left, right) => {
    const scoreDiff = matchQueryScore(right, teamHint, question) - matchQueryScore(left, teamHint, question);
    if (scoreDiff !== 0) return scoreDiff;
    const dateDiff = toDateValue(right.date_time_gmt || right.date) - toDateValue(left.date_time_gmt || left.date);
    if (dateDiff !== 0) {
      return ascending ? -dateDiff : dateDiff;
    }
    return String(left.name || '').localeCompare(String(right.name || ''));
  });
}

function slimMatch(match = {}) {
  if (!match) return null;
  return {
    id: String(match.id || ''),
    name: String(match.name || ''),
    teams: Array.isArray(match.teams) ? match.teams.slice(0, 2) : [],
    date: String(match.date || ''),
    venue: String(match.venue || ''),
    status: String(match.status || match.result || ''),
    winner: String(match.winner || ''),
    match_type: String(match.match_type || match.format || ''),
    summary: String(match.summary || match.result || ''),
    top_batters: Array.isArray(match.top_batters) ? match.top_batters.slice(0, 3) : [],
    top_bowlers: Array.isArray(match.top_bowlers) ? match.top_bowlers.slice(0, 3) : [],
    score: Array.isArray(match.score)
      ? match.score.slice(0, 2).map((row) => ({
          inning: String(row.inning || ''),
          runs: Number(row.runs || 0),
          wickets: row.wickets === null || row.wickets === undefined ? null : Number(row.wickets),
          overs: row.overs === null || row.overs === undefined ? null : Number(row.overs)
        }))
      : []
  };
}

function slimPlayerProfile(player = {}) {
  if (!player) return null;
  return {
    id: String(player.id || ''),
    name: String(player.name || ''),
    country: String(player.country || '')
  };
}

function titleCaseMetric(metric = '') {
  const value = String(metric || '').trim();
  if (!value) return 'Metric';
  return value
    .split('_')
    .join(' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function scopeSubtitle(route = {}) {
  const bits = [];
  if (route?.season) bits.push(String(route.season));
  if (route?.format) bits.push(String(route.format));
  return bits.join(' | ');
}

function rankedRows(rows = []) {
  return rows.map((row, index) => ({
    rank: index + 1,
    ...row
  }));
}

function buildPublicDetails(question = '', route = {}, structuredContext = {}, vectorContext = {}, cricApiContext = {}, synthesized = {}) {
  const data = structuredContext?.result?.data && typeof structuredContext.result.data === 'object'
    ? structuredContext.result.data
    : {};
  const type = String(data.type || '').trim();
  const fallbackSummary = String(synthesized?.answer || structuredContext?.result?.answer || '').trim();
  const normalizedQuestion = normalizeText(question);
  const providerStatus = buildProviderStatus(cricApiContext?.errors || []);

  if (type === 'player_stats') {
    const player = data.player || {};
    return {
      type,
      title: String(player.name || 'Player Snapshot'),
      subtitle: String(player.team || player.country || ''),
      summary: fallbackSummary,
      player,
      stats: data.stats || {},
      recent_matches: Array.isArray(data.recent_matches) ? data.recent_matches.slice(0, 5).map(slimMatch) : []
    };
  }
  if (type === 'team_stats') {
    const team = data.team || {};
    return {
      type,
      title: String(team.name || 'Team Snapshot'),
      subtitle: scopeSubtitle(route),
      summary: fallbackSummary,
      team,
      stats: data.stats || {},
      recent_matches: Array.isArray(data.stats?.recent_matches) ? data.stats.recent_matches.slice(0, 5).map(slimMatch) : []
    };
  }
  if (type === 'match_summary') {
    const match = slimMatch(data.match || {});
    return {
      type,
      title: String(match.name || 'Match Summary'),
      subtitle: [match.date, match.venue].filter(Boolean).join(' | '),
      summary: String(match.summary || fallbackSummary),
      match
    };
  }
  if (type === 'compare_players') {
    const left = data.left || {};
    const right = data.right || {};
    return {
      type,
      title: `${left.name || 'Player 1'} vs ${right.name || 'Player 2'}`,
      subtitle: scopeSubtitle(route),
      summary: fallbackSummary,
      left,
      right
    };
  }
  if (type === 'head_to_head') {
    return {
      type,
      title: `${String(data.team1 || 'Team 1')} vs ${String(data.team2 || 'Team 2')}`,
      subtitle: scopeSubtitle(route),
      summary: fallbackSummary,
      team1: String(data.team1 || ''),
      team2: String(data.team2 || ''),
      stats: data.stats
        ? {
            matches: Number(data.stats.matches || 0),
            wins_team_a: Number(data.stats.wins_team_a || 0),
            wins_team_b: Number(data.stats.wins_team_b || 0),
            no_result: Number(data.stats.no_result || 0)
          }
        : {},
      recent_matches: Array.isArray(data.stats?.recent_matches) ? data.stats.recent_matches.slice(0, 5).map(slimMatch) : []
    };
  }
  if (type === 'top_players') {
    const metric = String(data.metric || '');
    return {
      type,
      title: `Top ${titleCaseMetric(metric)}`,
      subtitle: scopeSubtitle(route),
      summary: fallbackSummary,
      metric,
      rows: rankedRows(Array.isArray(data.rows) ? data.rows.slice(0, 10) : [])
    };
  }
  if (type === 'glossary') {
    return {
      type,
      title: titleCaseMetric(String(data.term || 'Glossary')),
      subtitle: 'Cricket term',
      summary: String(structuredContext?.result?.answer || fallbackSummary),
      term: String(data.term || ''),
      explanation: String(structuredContext?.result?.answer || '')
    };
  }

  const liveMatch = Array.isArray(cricApiContext.live_scores) ? cricApiContext.live_scores[0] : null;
  const nextMatch = Array.isArray(cricApiContext.schedule) ? cricApiContext.schedule[0] : null;
  const playerProfile = Array.isArray(cricApiContext.player_profiles) ? cricApiContext.player_profiles[0] : null;
  const shouldRenderLiveSurface =
    LIVE_QUERY_REGEX.test(normalizedQuestion) ||
    SCHEDULE_QUERY_REGEX.test(normalizedQuestion) ||
    Boolean(liveMatch || nextMatch || playerProfile);
  if (shouldRenderLiveSurface) {
    const upcomingMatches = Array.isArray(cricApiContext.schedule)
      ? cricApiContext.schedule.slice(0, 4).map(slimMatch)
      : [];
    const recentMatches = Array.isArray(cricApiContext.live_scores)
      ? cricApiContext.live_scores.slice(0, 4).map(slimMatch)
      : [];
    return {
      type: 'live_update',
      title: upcomingMatches.length ? 'Upcoming Matches' : recentMatches.length ? 'Live Match Center' : 'Match Center',
      subtitle: scopeSubtitle(route),
      summary: fallbackSummary,
      live_match: slimMatch(liveMatch || {}),
      next_match: slimMatch(nextMatch || {}),
      upcoming_matches: upcomingMatches,
      recent_matches: recentMatches,
      player: slimPlayerProfile(playerProfile || {}),
      provider_status: providerStatus
    };
  }

  return {
    type: 'summary',
    title: 'Cricket Intelligence',
    subtitle: '',
    summary: fallbackSummary
  };
}

async function safeCricApiCall(loader) {
  try {
    return { ok: true, value: await loader() };
  } catch (error) {
    return {
      ok: false,
      error: error?.message || 'CricAPI request failed.',
      config_error: error instanceof CricApiConfigError || error?.name === 'CricApiConfigError'
    };
  }
}

async function buildCricApiContext(route, question) {
  const normalizedQuestion = normalizeText(question);
  const { playerHints, teamHints } = deriveEntityHints(question, route);
  const formatHint = pickFirst(route.format, guessFormat(question));

  const context = {
    provider: 'cricapi',
    available: false,
    errors: [],
    player_searches: [],
    player_profiles: [],
    live_scores: [],
    schedule: [],
    series: [],
    series_info: null
  };

  if (playerHints.length) {
    const searchResults = await Promise.all(
      playerHints.map(async (hint) => ({
        query: hint,
        result: await safeCricApiCall(() => searchCricApiPlayers({ q: hint, limit: 3 }))
      }))
    );

    for (const item of searchResults) {
      if (!item.result.ok) {
        context.errors.push(item.result.error);
        continue;
      }
      const players = Array.isArray(item.result.value?.items) ? item.result.value.items : [];
      context.player_searches.push({
        query: item.query,
        items: players
      });
    }

    const profileIds = uniqueNonEmpty(
      context.player_searches.map((row) => row.items?.[0]?.id).slice(0, 2)
    );
    const profileResults = await Promise.all(
      profileIds.map(async (id) => safeCricApiCall(() => getPlayerInfo(id)))
    );
    for (const profile of profileResults) {
      if (!profile.ok) {
        context.errors.push(profile.error);
        continue;
      }
      if (profile.value?.player) {
        context.player_profiles.push(profile.value.player);
      }
    }
  }

  if (SERIES_QUERY_REGEX.test(normalizedQuestion)) {
    const seriesSearch = await safeCricApiCall(() => getSeriesList({ q: question, limit: 4 }));
    if (seriesSearch.ok) {
      context.series = Array.isArray(seriesSearch.value?.items) ? seriesSearch.value.items : [];
      const topSeriesId = context.series[0]?.id || '';
      if (topSeriesId) {
        const seriesInfo = await safeCricApiCall(() => getSeriesInfo(topSeriesId));
        if (seriesInfo.ok) {
          context.series_info = seriesInfo.value;
        } else {
          context.errors.push(seriesInfo.error);
        }
      }
    } else {
      context.errors.push(seriesSearch.error);
    }
  }

  const wantsLive = LIVE_QUERY_REGEX.test(normalizedQuestion);
  const wantsSchedule = SCHEDULE_QUERY_REGEX.test(normalizedQuestion);
  const teamHint = teamHints[0] || '';

  if (wantsLive || wantsSchedule || teamHint || formatHint) {
    const [liveResult, scheduleResult] = await Promise.all([
      safeCricApiCall(() =>
        getLiveScores({
          team: teamHint,
          matchType: formatHint,
          includeRecent: true,
          limit: wantsLive ? 5 : 3
        })
      ),
      safeCricApiCall(() =>
        getMatchSchedule({
          team: teamHint,
          matchType: formatHint,
          limit: wantsSchedule ? 5 : 3,
          upcomingOnly: true
        })
      )
    ]);

    if (liveResult.ok) {
      const liveItems = Array.isArray(liveResult.value?.items) ? liveResult.value.items : [];
      context.live_scores = sortMatchesForQuestion(liveItems, teamHint, question);
    } else {
      context.errors.push(liveResult.error);
    }

    if (scheduleResult.ok) {
      const scheduleItems = Array.isArray(scheduleResult.value?.items) ? scheduleResult.value.items : [];
      context.schedule = sortMatchesForQuestion(scheduleItems, teamHint, question, { ascending: true });
    } else {
      context.errors.push(scheduleResult.error);
    }
  }

  if (
    !context.player_searches.length &&
    !context.player_profiles.length &&
    !context.live_scores.length &&
    !context.schedule.length &&
    !context.series.length &&
    !context.series_info
  ) {
    const fallbackLive = await safeCricApiCall(() =>
      getLiveScores({
        includeRecent: true,
        limit: 3
      })
    );
    if (fallbackLive.ok) {
      const liveItems = Array.isArray(fallbackLive.value?.items) ? fallbackLive.value.items : [];
      context.live_scores = sortMatchesForQuestion(liveItems, teamHint, question);
    } else {
      context.errors.push(fallbackLive.error);
    }
  }

  context.errors = uniqueNonEmpty(context.errors);
  context.available = Boolean(
    context.player_searches.length ||
      context.player_profiles.length ||
      context.live_scores.length ||
      context.schedule.length ||
      context.series.length ||
      context.series_info
  );
  return context;
}

function buildStructuredContext(route, question) {
  const cache = datasetStore.getCache();
  if (!cache) {
    datasetStore.start().catch(() => {
      // Leave the request on live/vector fallback while the local cache warms in the background.
    });
    return {
      cache_ready: false,
      available: false,
      result: null,
      route
    };
  }

  const effectiveRoute = refineRouteForQuestion(route, question);
  let result = unavailableResult();
  if (effectiveRoute.action === 'player_stats' || effectiveRoute.action === 'player_season_stats') {
    result = runPlayerAction(effectiveRoute.action, effectiveRoute, question);
  } else if (effectiveRoute.action === 'team_stats') {
    result = runTeamStats(effectiveRoute, question);
  } else if (effectiveRoute.action === 'match_summary') {
    result = runMatchSummary(effectiveRoute, question);
  } else if (effectiveRoute.action === 'compare_players') {
    result = runComparePlayers(effectiveRoute, question);
  } else if (effectiveRoute.action === 'head_to_head') {
    result = runHeadToHead(effectiveRoute, question);
  } else if (effectiveRoute.action === 'top_players') {
    result = runTopPlayers(effectiveRoute, question);
  } else if (effectiveRoute.action === 'glossary') {
    result = runGlossary(effectiveRoute, question);
  }

  const available = Boolean(
    result &&
      result.answer &&
      result.answer !== NOT_AVAILABLE_MESSAGE &&
      result.answer !== unavailableResult().answer
  );

  return {
    cache_ready: true,
    available,
    result,
    route: effectiveRoute
  };
}

function replaceNameInAnswer(answer = '', original = '', replacement = '') {
  const source = String(answer || '');
  const oldText = String(original || '').trim();
  const newText = String(replacement || '').trim();
  if (!source || !oldText || !newText || oldText === newText) return source;
  return source.split(oldText).join(newText);
}

function mergeResolvedPlayerMeta(player = {}, profile = {}, query = '') {
  const datasetName = String(player.dataset_name || player.name || '').trim();
  const canonicalName =
    String(profile.canonical_name || '').trim() ||
    String(player.canonical_name || '').trim() ||
    String(query || '').trim() ||
    datasetName;

  return {
    ...player,
    dataset_name: datasetName,
    canonical_name: canonicalName,
    name: canonicalName,
    image_url: String(profile.image_url || player.image_url || ''),
    wikipedia_url: String(profile.wikipedia_url || player.wikipedia_url || ''),
    country: String(profile.country || player.country || '')
  };
}

async function enrichStructuredResult(structuredContext = {}, route = {}, question = '') {
  if (!structuredContext?.cache_ready || !structuredContext?.result?.data) return structuredContext;

  const data = structuredContext.result.data;
  const type = String(data.type || '').trim();

  if (type === 'player_stats' && data.player?.name) {
    const playerQuery = cleanEntitySegment(pickFirst(route.player, deriveEntityHints(question, route).playerHints[0]));
    const originalName = String(data.player.name || '').trim();
    const profile = await getPlayerProfile({
      query: playerQuery,
      datasetName: originalName
    });
    data.player = mergeResolvedPlayerMeta(data.player, profile, playerQuery);
    structuredContext.result.answer = replaceNameInAnswer(
      structuredContext.result.answer,
      originalName,
      data.player.name
    );
  }

  if (type === 'compare_players' && data.left?.name && data.right?.name) {
    const parsedSides = parseVsSides(question) || {};
    const leftQuery = cleanEntitySegment(pickFirst(route.player1, parsedSides.left));
    const rightQuery = cleanEntitySegment(pickFirst(route.player2, parsedSides.right));
    const leftOriginal = String(data.left.name || '').trim();
    const rightOriginal = String(data.right.name || '').trim();

    const [leftProfile, rightProfile] = await Promise.all([
      getPlayerProfile({
        query: leftQuery,
        datasetName: leftOriginal
      }),
      getPlayerProfile({
        query: rightQuery,
        datasetName: rightOriginal
      })
    ]);

    data.left = mergeResolvedPlayerMeta(data.left, leftProfile, leftQuery);
    data.right = mergeResolvedPlayerMeta(data.right, rightProfile, rightQuery);
    structuredContext.result.answer = replaceNameInAnswer(
      replaceNameInAnswer(structuredContext.result.answer, leftOriginal, data.left.name),
      rightOriginal,
      data.right.name
    );
  }

  return structuredContext;
}

function buildSourceList(structuredContext, vectorContext, cricApiContext, modelSources = []) {
  const sources = [];
  if (structuredContext?.available) sources.push('dataset');
  if (vectorContext?.available && Array.isArray(vectorContext.results) && vectorContext.results.length) {
    sources.push('vector_db');
  }
  if (cricApiContext?.available) sources.push('cricapi');
  for (const source of modelSources) {
    const clean = String(source || '').trim();
    if (clean && !sources.includes(clean)) {
      sources.push(clean);
    }
  }
  return sources;
}

function defaultFollowups(route = {}, structuredContext = {}) {
  const structuredFollowups = structuredContext?.result?.followups || [];
  if (structuredFollowups.length) return structuredFollowups.slice(0, 3);

  if (route.action === 'match_summary') {
    return [
      'Show the detailed scorecard for this match',
      'Show recent matches for this team',
      'Show the head to head record'
    ];
  }

  return [
    'Show recent live scores',
    'Compare two cricket players',
    'Show the next scheduled matches'
  ];
}

function shouldUseLooseFallback(question = '', route = {}) {
  if (route.action && route.action !== 'not_supported') return true;

  const normalizedQuestion = normalizeText(question);
  if (!normalizedQuestion) return false;

  if (LIVE_QUERY_REGEX.test(normalizedQuestion) || SCHEDULE_QUERY_REGEX.test(normalizedQuestion)) {
    return true;
  }

  if (SERIES_QUERY_REGEX.test(normalizedQuestion) || PLAYER_PROFILE_REGEX.test(normalizedQuestion)) {
    return true;
  }

  if (FACT_LOOKUP_REGEX.test(normalizedQuestion)) {
    const hints = deriveEntityHints(question, route);
    return Boolean(hints.playerHints.length || hints.teamHints.length || parseVsSides(question) || guessMatchId(question));
  }

  return false;
}

function fallbackAnswer(question, route, structuredContext, vectorContext, cricApiContext) {
  const structuredType = String(structuredContext?.result?.data?.type || '').trim();
  const normalizedQuestion = normalizeText(question);
  const isLiveOrScheduleQuery =
    LIVE_QUERY_REGEX.test(normalizedQuestion) || SCHEDULE_QUERY_REGEX.test(normalizedQuestion);

  if (structuredContext?.result?.answer && !(isLiveOrScheduleQuery && structuredType === 'name_resolution')) {
    return {
      answer: structuredContext.result.answer,
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
    };
  }

  if (!shouldUseLooseFallback(question, route)) {
    return {
      answer: NOT_AVAILABLE_MESSAGE,
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
    };
  }

  const liveMatch = cricApiContext?.live_scores?.[0];
  if (liveMatch) {
    return {
      answer: buildLiveAnswer(question, route, cricApiContext),
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
    };
  }

  const providerStatus = buildProviderStatus(cricApiContext?.errors || []);
  if (providerStatus && (isLiveOrScheduleQuery || route.action === 'not_supported')) {
    return {
      answer: providerStatus.message,
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
    };
  }

  const vectorHit = vectorContext?.results?.[0];
  if (vectorHit?.document_preview) {
    return {
      answer: 'I found relevant archive context, but not enough verified structured evidence to produce a professional cricket summary for that query.',
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
    };
  }

  return {
    answer: NOT_AVAILABLE_MESSAGE,
    followups: defaultFollowups(route, structuredContext),
    sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
  };
}

function escapeRegex(value = '') {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function extractPlayerRunsFromPreview(playerName = '', preview = '') {
  const parts = String(playerName || '').trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return null;
  const first = parts[0];
  const last = parts[parts.length - 1];
  const initial = first[0] || '';
  const patterns = [
    new RegExp(`\\b${escapeRegex(first)}\\s+${escapeRegex(last)}\\b\\s+(\\d+)`, 'i'),
    new RegExp(`\\b${escapeRegex(initial)}\\s+${escapeRegex(last)}\\b\\s+(\\d+)`, 'i'),
    new RegExp(`\\b${escapeRegex(last)}\\b\\s+(\\d+)`, 'i')
  ];
  for (const pattern of patterns) {
    const match = String(preview || '').match(pattern);
    if (match) {
      return Number(match[1]);
    }
  }
  return null;
}

function buildLiveAnswer(question = '', route = {}, cricApiContext = {}) {
  const { teamHints } = deriveEntityHints(question, route);
  const teamHint = teamHints[0] || '';
  const normalizedQuestion = normalizeText(question);
  const liveItems = Array.isArray(cricApiContext.live_scores) ? cricApiContext.live_scores : [];
  const scheduleItems = Array.isArray(cricApiContext.schedule) ? cricApiContext.schedule : [];
  const activeMatch = liveItems.find((item) => item.live) || null;
  const recentMatch = liveItems[0] || null;
  const nextMatch = scheduleItems[0] || null;
  const providerStatus = buildProviderStatus(cricApiContext?.errors || []);

  if (!LIVE_QUERY_REGEX.test(normalizeText(question)) && !SCHEDULE_QUERY_REGEX.test(normalizeText(question))) {
    return '';
  }

  function scoreLine(row = {}) {
    if (!row || row.runs === null || row.runs === undefined) return '';
    const wickets =
      row.wickets === null || row.wickets === undefined || row.wickets === '' ? '' : `/${row.wickets}`;
    const overs =
      row.overs === null || row.overs === undefined || row.overs === '' ? '' : ` (${row.overs} overs)`;
    return `${row.inning || 'Innings'}: ${row.runs}${wickets}${overs}`;
  }

  function topPerformer(match = {}) {
    const batter = Array.isArray(match.top_batters) ? match.top_batters[0] : null;
    if (batter?.name) {
      return `${batter.name} - ${batter.runs}${batter.balls ? ` (${batter.balls})` : ''}`;
    }
    const bowler = Array.isArray(match.top_bowlers) ? match.top_bowlers[0] : null;
    if (bowler?.name) {
      return `${bowler.name} - ${bowler.wickets}/${bowler.runs_conceded || 0}`;
    }
    return '';
  }

  function matchBlock(match = {}, { label = '', includeStatus = false } = {}) {
    if (!match?.name) return '';
    return [
      match.name,
      `Date: ${match.date || 'Date unavailable'}`,
      match.venue ? `Venue: ${match.venue}` : '',
      match.match_type ? `Format: ${match.match_type}` : '',
      includeStatus && match.status ? `Status: ${match.status}` : ''
    ]
      .filter(Boolean)
      .join('\n');
  }

  if ((/\bnext scheduled matches?\b|\bupcoming matches?\b|\bwho is playing today\b/.test(normalizedQuestion) || SCHEDULE_QUERY_REGEX.test(normalizedQuestion)) && scheduleItems.length) {
    return [
      'Upcoming Matches',
      '',
      ...scheduleItems.slice(0, 3).flatMap((match, index) => {
        const block = matchBlock(match);
        return index === 0 ? [block] : ['', block];
      })
    ]
      .filter(Boolean)
      .join('\n');
  }

  if (activeMatch) {
    const scoreLines = (Array.isArray(activeMatch.score) ? activeMatch.score : []).map(scoreLine).filter(Boolean);
    const performer = topPerformer(activeMatch);
    return [
      `${activeMatch.name || `${teamHint || 'Cricket'} Match`} - Live`,
      '',
      ...scoreLines,
      `Status: ${activeMatch.status || 'Status unavailable'}`,
      performer ? '' : null,
      performer ? `Top Performer: ${performer}` : null
    ]
      .filter(Boolean)
      .join('\n');
  }

  const bits = [];
  if (!liveItems.length && !scheduleItems.length && providerStatus) {
    return providerStatus.message;
  }
  if (teamHint) {
    bits.push(`No current live match was found for ${teamHint}.`);
  }
  if (recentMatch) {
    bits.push(
      [
        `${recentMatch.name || 'Recent Match'} - Recent`,
        `Status: ${recentMatch.status || 'Status unavailable'}`,
        recentMatch.venue ? `Venue: ${recentMatch.venue}` : ''
      ]
        .filter(Boolean)
        .join('\n')
    );
  }
  if (nextMatch) {
    bits.push(
      [
        `${nextMatch.name || 'Next Match'} - Upcoming`,
        `Date: ${nextMatch.date || 'Date unavailable'}`,
        nextMatch.venue ? `Venue: ${nextMatch.venue}` : ''
      ]
        .filter(Boolean)
        .join('\n')
    );
  }
  return bits.join('\n\n').trim();
}

function buildPlayerFormAnswer(question = '', route = {}, vectorContext = {}) {
  const { playerHints } = deriveEntityHints(question, route);
  const playerHint = playerHints[0] || '';
  if (!playerHint) return '';

  const vectorResults = Array.isArray(vectorContext.results) ? vectorContext.results : [];
  const rows = vectorResults
    .map((row) => {
      const preview = String(row.document_preview || '');
      const runs = extractPlayerRunsFromPreview(playerHint, preview);
      if (runs === null || !String(preview).toLowerCase().includes(playerHint.split(/\s+/).slice(-1)[0].toLowerCase())) {
        return null;
      }
      return {
        date: String(row.metadata?.date || ''),
        venue: String(row.metadata?.venue || ''),
        matchType: String(row.metadata?.match_type || ''),
        runs
      };
    })
    .filter(Boolean)
    .sort((left, right) => String(right.date || '').localeCompare(String(left.date || '')))
    .slice(0, 3);

  if (!rows.length) return '';

  return `Archived form for ${playerHint}: ${rows
    .map((row) => `${row.date || 'date n/a'} ${row.matchType || 'match'} at ${row.venue || 'venue n/a'} - ${row.runs} runs`)
    .join('; ')}.`;
}

function buildGroundedAnswer(question, route, structuredContext, vectorContext, cricApiContext) {
  const parts = [];
  const liveAnswer = buildLiveAnswer(question, route, cricApiContext);
  const formAnswer = buildPlayerFormAnswer(question, route, vectorContext);

  if (LIVE_QUERY_REGEX.test(normalizeText(question)) || SCHEDULE_QUERY_REGEX.test(normalizeText(question))) {
    if (liveAnswer) parts.push(liveAnswer);
  }

  if (PLAYER_PROFILE_REGEX.test(normalizeText(question)) && cricApiContext?.player_profiles?.[0]) {
    const player = cricApiContext.player_profiles[0];
    parts.push([
      player.name,
      '',
      `Country: ${player.country || 'Not available'}`,
      '',
      `Summary: ${player.name} is available in the live player directory.`
    ].join('\n'));
  }

  if (/\bform\b|\brecent\b|\bhow is\b|\blook like\b/.test(normalizeText(question))) {
    if (formAnswer) parts.push(formAnswer);
  }

  if (structuredContext?.available && structuredContext?.result?.answer && parts.length === 0) {
    parts.push(structuredContext.result.answer);
  }

  if (!parts.length) return null;
  return {
    answer: parts.join('\n\n'),
    followups: defaultFollowups(route, structuredContext),
    sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
  };
}

async function synthesizeAnswer(question, route, structuredContext, vectorContext, cricApiContext) {
  const grounded = buildGroundedAnswer(
    question,
    route,
    structuredContext,
    vectorContext,
    cricApiContext
  );
  if (grounded) {
    return grounded;
  }

  const vectorSummary = compactVectorContext(vectorContext);
  const cricApiSummary = compactCricApiContext(cricApiContext);
  const structuredSummary = compactStructuredResult(structuredContext);

  const messages = [
    {
      role: 'system',
      content: [
        'You are Cricket Intelligence AI, a professional cricket analytics assistant.',
        'Use the supplied sources with strict priority based on question type.',
        'For live, today, current, ongoing, upcoming, and recent match questions, prefer CricAPI.',
        'For historical player, team, match, season, and record questions, prefer the structured dataset and vector archive.',
        'For player comparisons, rely on the structured dataset and computed statistics.',
        'If reasoning is required, synthesize only from the supplied evidence and do not invent missing statistics.',
        'Always resolve player names to the full official name when the supplied evidence supports that resolution.',
        'Never answer with shortened player names when a canonical name is available in the supplied evidence.',
        'Use a professional, structured format with short sections and exact values when available.',
        'For player answers, prefer blocks like Name, Matches, Runs, Average, Strike Rate, then Summary.',
        'For comparison answers, prefer blocks like "Player A vs Player B", metric sections, then Conclusion.',
        'For live answers, prefer blocks like "Match - Live", score lines, status, and top performer when available.',
        'If no live match is available for the requested team, say that clearly and use the closest recent or upcoming CricAPI item instead.',
        'Do not mention models, APIs, vector databases, routing, prompts, or internal system details.',
        'If the supplied evidence is insufficient, say so plainly instead of guessing.',
        'If you use general knowledge beyond the supplied sources, include "general_knowledge" in sources.',
        'Return ONLY valid JSON with keys: summary, suggestions, sources.'
      ].join('\n')
    },
    {
      role: 'user',
      content: JSON.stringify(
        {
          question,
          route,
          structured_dataset: structuredSummary,
          vector_archive: vectorSummary,
          cricapi: cricApiSummary
        },
        null,
        2
      )
    }
  ];

  try {
    const content = await callLlama(messages, {
      temperature: 0.2,
      timeoutMs: 45000,
      purpose: 'reasoning'
    });
    const parsed = extractJsonFromText(content);
    if (parsed && typeof parsed === 'object' && String(parsed.summary || parsed.answer || '').trim()) {
      return {
        answer: String(parsed.summary || parsed.answer || '').trim(),
        followups: uniqueNonEmpty(
          Array.isArray(parsed.suggestions) ? parsed.suggestions : Array.isArray(parsed.followups) ? parsed.followups : []
        ).slice(0, 3),
        sources: uniqueNonEmpty(Array.isArray(parsed.sources) ? parsed.sources : [])
      };
    }
  } catch (_) {
    // Fall back to the grounded answer builders below.
  }

  return fallbackAnswer(question, route, structuredContext, vectorContext, cricApiContext);
}

async function handleQuery({ question = '', query = '' } = {}) {
  return processQuery({ question, query });
}

async function processQuery({ question = '', query = '' } = {}, { onStatus } = {}) {
  const text = pickFirst(question, query);
  if (!text) {
    return {
      statusCode: 400,
      response: buildResponse({
        summary: 'Please type your question.',
        details: {},
        suggestions: []
      })
    };
  }

  emitStatus(onStatus, {
    stage: 'search',
    message: 'Searching stats.'
  });
  const route = normalizeRoute(await routeQuestion(text, {}));
  let structuredContext = buildStructuredContext(route, text);
  const effectiveRoute = structuredContext.route || route;
  structuredContext = await enrichStructuredResult(structuredContext, effectiveRoute, text);
  emitStatus(onStatus, {
    stage: 'search',
    action: effectiveRoute.action,
    message: structuredContext.cache_ready ? actionStatusMessage(effectiveRoute.action) : 'Searching saved records.'
  });
  const vectorPromise = queryVectorDb(text, { k: 5 });

  emitStatus(onStatus, {
    stage: 'live',
    message: 'Checking latest match data.'
  });
  const cricApiPromise = buildCricApiContext(effectiveRoute, text);

  const [vectorContext, cricApiContext] = await Promise.all([vectorPromise, cricApiPromise]);

  emitStatus(onStatus, {
    stage: 'synthesizing',
    message: 'Preparing answer.'
  });
  const synthesized = await synthesizeAnswer(
    text,
    effectiveRoute,
    structuredContext,
    vectorContext,
    cricApiContext
  );

  return {
    statusCode: 200,
    response: buildResponse({
      answer: synthesized.answer,
      data: buildPublicDetails(text, effectiveRoute, structuredContext, vectorContext, cricApiContext, synthesized),
      followups:
        (Array.isArray(synthesized.followups) && synthesized.followups.length
          ? synthesized.followups
          : defaultFollowups(effectiveRoute, structuredContext)
        ).slice(0, 3)
    })
  };
}

module.exports = {
  handleQuery,
  processQuery
};
