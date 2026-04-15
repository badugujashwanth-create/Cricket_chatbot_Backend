const {
  routeQuestion,
  extractJsonFromText,
  normalizeDataSources,
  inferDataSources
} = require('./llamaRouter');
const { callLlama, openAiEndpointUrl } = require('./llamaClient');
const { queryVectorDb } = require('./chromaService');
const { getPlayerProfile } = require('./playerProfileService');
const { getSession, setPendingClarification, clearPendingClarification, updateContext } = require('./sessionStore');
const { NOT_AVAILABLE_MESSAGE, SUPPORTED_ACTIONS, GLOSSARY, DATA_SOURCES } = require('./constants');
const {
  CricApiConfigError,
  CricbuzzApiConfigError,
  getLiveScores,
  searchPlayers: searchCricApiPlayers,
  getPlayerInfo,
  getMatchSchedule,
  getSeriesList,
  getSeriesInfo,
  searchCricbuzzPlayers,
  getCricbuzzPlayerCardByName
} = require('./cricApiService');
const { EspnServiceError, getPlayerCareerByQuery } = require('./espnService');
const { normalizeText } = require('./textUtils');
const {
  cleanEntitySegment,
  parseVsSides,
  isLiveLeaningQuestion,
  rankEntityCandidates
} = require('./queryParser');
const {
  loadPlayerProfiles,
  loadMatchSummaries,
  resolvePlayer: resolveVectorPlayer,
  resolveTeam: resolveVectorTeam,
  getTopPlayersByMetric,
  getTopPlayersForTeam,
  getMatchById: getVectorMatchById,
  findMatchesByTeams,
  findMatchesForTeam
} = require('./vectorIndexService');
const { lookupKnowledge } = require('./knowledgeService');

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
const PLAYER_CONTEXT_PRONOUN_REGEX = /\b(he|him|his|she|her)\b/i;
const TEAM_CONTEXT_PRONOUN_REGEX = /\b(they|them|their)\b/i;
const wikipediaSummaryCache = new Map();
const wikipediaWikitextCache = new Map();
const NUMBER_WORDS = new Map([
  ['one', 1],
  ['two', 2],
  ['three', 3],
  ['four', 4],
  ['five', 5],
  ['six', 6],
  ['seven', 7],
  ['eight', 8],
  ['nine', 9],
  ['ten', 10]
]);
const GENERAL_CHAT_SYSTEM_PROMPT =
  'You are an intelligent Cricket AI Assistant. The user has asked a general question or greeted you. Answer them conversationally and politely in a short sentence, and gently guide them back to asking about cricket stats or live scores.';
const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || '').trim();
const OPENAI_MODEL = String(process.env.OPENAI_MODEL || 'gpt-4o').trim();
const DATA_SOURCE = DATA_SOURCES;

function pickFirst(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const clean = String(value).trim();
    if (clean) return clean;
  }
  return '';
}

function createEmptyVectorContext(warning = '') {
  return {
    available: false,
    warning: String(warning || '').trim(),
    results: []
  };
}

function createEmptyCricApiContext(errors = []) {
  return {
    provider: 'cricapi',
    available: false,
    errors: uniqueNonEmpty(errors),
    player_searches: [],
    player_profiles: [],
    live_scores: [],
    archive_recent_matches: [],
    schedule: [],
    series: [],
    series_info: null
  };
}

function createEmptyCricbuzzContext(errors = []) {
  return {
    provider: 'cricbuzz',
    available: false,
    errors: uniqueNonEmpty(errors),
    player_searches: [],
    players: []
  };
}

function createEmptyEspnContext(errors = []) {
  return {
    provider: 'espn',
    available: false,
    errors: uniqueNonEmpty(errors),
    player_searches: [],
    player: null
  };
}

function resolveRequestedDataSources(route = {}, question = '') {
  const action = String(route?.action || '').trim();
  if (action === 'live_update') {
    return [DATA_SOURCE.CRICAPI_LIVE];
  }
  if (action === 'general_knowledge') {
    return [DATA_SOURCE.LOCAL_KNOWLEDGE];
  }
  if (isGeneralConversationAction(action)) {
    return [DATA_SOURCE.OPENAI_FALLBACK];
  }
  const explicitSources = normalizeDataSources(route.data_sources || []);
  const inferredSources = explicitSources.length ? explicitSources : inferDataSources(question, route);
  return normalizeDataSources(inferredSources);
}

function uniqueNonEmpty(values = []) {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
}

function isGeneralConversationAction(action = '') {
  return ['chit_chat'].includes(String(action || '').trim());
}

async function callOpenAiChatCompletions(messages = [], { temperature = 0.2, timeoutMs = 60000 } = {}) {
  if (!OPENAI_API_KEY) {
    throw new Error('OpenAI API key is not configured.');
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Number(timeoutMs || 60000));

  try {
    const response = await fetch(openAiEndpointUrl(), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: OPENAI_MODEL,
        temperature: Number.isFinite(Number(temperature)) ? Number(temperature) : 0.2,
        messages
      }),
      signal: controller.signal
    });

    if (!response.ok) {
      const payload = await response.text().catch(() => '');
      throw new Error(`OpenAI chat completions failed (${response.status})${payload ? `: ${payload}` : ''}`);
    }

    const payload = await response.json();
    return String(payload?.choices?.[0]?.message?.content || '').trim();
  } finally {
    clearTimeout(timeout);
  }
}

function normalizeDetectedEntities(values = []) {
  return uniqueNonEmpty(Array.isArray(values) ? values : []).slice(0, 12);
}

function extractProperNamePhrases(text = '') {
  const matches =
    String(text || '').match(/\b(?:[A-Z][a-z]+|[A-Z]{2,4})(?:\s+(?:[A-Z][a-z]+|[A-Z]{2,4})){1,4}\b/g) || [];
  const blocked = new Set([
    'Cricket Intelligence',
    'Match Summary',
    'Recent Match',
    'Next Match',
    'Upcoming Matches'
  ]);
  return uniqueNonEmpty(matches.filter((value) => !blocked.has(String(value || '').trim())));
}

function extractDetectedEntitiesFallback(route = {}, data = {}, answer = '', cricApiContext = {}) {
  const baseCandidates = [
    route.player,
    route.player1,
    route.player2,
    route.team,
    route.team1,
    route.team2,
    data?.player?.name,
    data?.team?.name,
    data?.left?.name,
    data?.right?.name,
    data?.match?.name,
    data?.match?.venue
  ];

  const contextualCandidates = [];
  const matchTeams = Array.isArray(data?.match?.teams) ? data.match.teams : [];
  const recentMatches = Array.isArray(data?.recent_matches) ? data.recent_matches : [];
  const liveMatches = Array.isArray(cricApiContext?.live_scores) ? cricApiContext.live_scores : [];

  for (const team of matchTeams) contextualCandidates.push(team);
  for (const match of recentMatches.slice(0, 5)) {
    contextualCandidates.push(match?.name, match?.venue);
    if (Array.isArray(match?.teams)) contextualCandidates.push(...match.teams);
  }
  for (const match of liveMatches.slice(0, 3)) {
    contextualCandidates.push(match?.name, match?.venue);
    if (Array.isArray(match?.teams)) contextualCandidates.push(...match.teams);
  }

  const normalizedAnswer = String(answer || '').trim();
  const matchedAnswerEntities = [];
  if (normalizedAnswer) {
    for (const value of [...baseCandidates, ...contextualCandidates]) {
      const clean = String(value || '').trim();
      if (clean && normalizedAnswer.includes(clean)) {
        matchedAnswerEntities.push(clean);
      }
    }
  }

  return normalizeDetectedEntities([
    ...baseCandidates,
    ...matchedAnswerEntities,
    ...extractProperNamePhrases(normalizedAnswer)
  ]);
}

function wordToNumber(value = '') {
  const clean = normalizeText(value);
  if (!clean) return null;
  if (/^\d+$/.test(clean)) return Number(clean);
  return NUMBER_WORDS.get(clean) ?? null;
}

function unavailableResult(message = NOT_AVAILABLE_MESSAGE) {
  return {
    answer: String(message || NOT_AVAILABLE_MESSAGE).trim() || NOT_AVAILABLE_MESSAGE,
    data: {
      type: 'summary'
    },
    followups: []
  };
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

  if (type === 'team_info') {
    const rows = [];
    if (Number(details.stats?.ipl_titles || 0)) {
      rows.push({ label: 'IPL Titles', value: Number(details.stats.ipl_titles || 0) });
    }
    if (Number(details.stats?.major_titles || 0)) {
      rows.push({ label: 'Major Titles', value: Number(details.stats.major_titles || 0) });
    }
    rows.push(
      { label: 'Matches', value: Number(details.stats?.matches || 0) },
      { label: 'Wins', value: Number(details.stats?.wins || 0) },
      { label: 'Win Rate', value: Number(details.stats?.win_rate || 0) }
    );
    return rows.slice(0, 4);
  }

  if (type === 'team_squad') {
    return [
      { label: 'Captain', value: String(details.team?.captain || details.captain || '-').trim() || '-' },
      { label: 'Coach', value: String(details.team?.coach || details.coach || '-').trim() || '-' },
      { label: 'Total Players', value: Number(details.total_players || 0) },
      { label: 'Matches', value: Number(details.stats?.matches || 0) }
    ];
  }

  if (type === 'playing_xi') {
    return [
      { label: 'Captain', value: String(details.team?.captain || details.captain || '-').trim() || '-' },
      { label: 'Coach', value: String(details.team?.coach || details.coach || '-').trim() || '-' },
      { label: 'Selected XI', value: Array.isArray(details.players) ? details.players.length : 0 },
      { label: 'Matches', value: Number(details.stats?.matches || 0) }
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

  if (type === 'record_lookup') {
    if (details.stats && typeof details.stats === 'object' && !Array.isArray(details.stats)) {
      return Object.entries(details.stats)
        .slice(0, 4)
        .map(([label, value]) => ({
          label: titleCaseMetric(label),
          value
        }));
    }
    return (Array.isArray(details.rows) ? details.rows : []).slice(0, 3).map((row) => ({
      label: `#${row.rank || ''} ${row.player || row.team || 'Record'}`,
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

  if (type === 'team_info' && details.team?.name) {
    return uniqueNonEmpty([
      details.team?.captain ? `${details.team.name} are captained by ${details.team.captain}.` : '',
      Number(details.stats?.ipl_titles || 0) ? `${details.team.name} have ${formatStatValue(details.stats.ipl_titles)} IPL titles in the current team profile.` : '',
      Number(details.stats?.win_rate || 0) ? `${details.team.name} has an archived win rate of ${formatStatValue(details.stats.win_rate)}%.` : '',
      Number(details.stats?.matches || 0) ? `${details.team.name} appears in ${formatStatValue(details.stats.matches)} archived matches.` : '',
      summary
    ]).slice(0, 3);
  }

  if ((type === 'team_squad' || type === 'playing_xi') && details.team?.name) {
    return uniqueNonEmpty([
      details.team?.captain ? `${details.team.name} are captained by ${details.team.captain}.` : '',
      details.team?.coach ? `${details.team.name} are coached by ${details.team.coach}.` : '',
      Number(details.total_players || 0) ? `${details.team.name} have ${formatStatValue(details.total_players)} listed players in the archived squad view.` : '',
      summary
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

function toUnifiedResponseType(details = {}) {
  const rawType = String(details.type || '').trim();

  if (rawType === 'team_squad' || rawType === 'playing_xi') return rawType;
  if (rawType === 'player_stats' || rawType === 'player_season_stats') return 'player';
  if (rawType === 'team_stats' || rawType === 'team_info') return 'team';
  if (rawType === 'match_summary' || rawType === 'live_update') return 'match';
  if (rawType === 'compare_players' || rawType === 'head_to_head') return 'comparison';
  if (rawType === 'general_knowledge' || rawType === 'glossary') return 'record';
  if (rawType === 'record_lookup') return 'record';
  if (rawType === 'subjective_analysis') {
    return details.left || details.right || details.team1 || details.team2 ? 'comparison' : 'record';
  }
  return 'record';
}

function toStatKey(label = '') {
  return String(label || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'value';
}

function normalizeStatEntryValue(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const clean = String(value ?? '').trim();
  return clean || null;
}

function buildUnifiedStats(details = {}) {
  const keyStats = buildKeyStats(details);
  if (!Array.isArray(keyStats) || !keyStats.length) return {};

  return keyStats.reduce((accumulator, item, index) => {
    const labelKey = toStatKey(item.label || `stat_${index + 1}`);
    const value = normalizeStatEntryValue(item.value);
    const left = normalizeStatEntryValue(item.left);
    const right = normalizeStatEntryValue(item.right);

    if (left !== null || right !== null) {
      if (left !== null) accumulator[`${labelKey}_left`] = left;
      if (right !== null) accumulator[`${labelKey}_right`] = right;
      return accumulator;
    }

    if (value !== null) {
      accumulator[labelKey] = value;
    }

    return accumulator;
  }, {});
}

function pruneEmptyFields(value) {
  if (Array.isArray(value)) {
    const next = value
      .map((item) => pruneEmptyFields(item))
      .filter((item) => {
        if (item === null || item === undefined) return false;
        if (item === '') return false;
        if (Array.isArray(item)) return item.length > 0;
        if (typeof item === 'object') return Object.keys(item).length > 0;
        return true;
      });
    return next;
  }

  if (value && typeof value === 'object') {
    return Object.entries(value).reduce((accumulator, [key, nestedValue]) => {
      const cleaned = pruneEmptyFields(nestedValue);
      if (cleaned === null || cleaned === undefined) return accumulator;
      if (cleaned === '') return accumulator;
      if (Array.isArray(cleaned) && !cleaned.length) return accumulator;
      if (cleaned && typeof cleaned === 'object' && !Array.isArray(cleaned) && !Object.keys(cleaned).length) {
        return accumulator;
      }
      accumulator[key] = cleaned;
      return accumulator;
    }, {});
  }

  return value;
}

function buildUnifiedImage(details = {}) {
  if (details.player?.image_url) return String(details.player.image_url || '').trim();
  if (details.team?.image_url) return String(details.team.image_url || '').trim();
  if (details.image_url) return String(details.image_url || '').trim();
  if (details.left?.image_url && !details.right?.image_url) return String(details.left.image_url || '').trim();
  return '';
}

function buildUnifiedExtra(details = {}, summary = '', answer = '', suggestions = []) {
  const insights = buildInsights(details, summary, answer);
  const rawType = String(details.type || '').trim();
  const extra = {
    action: rawType || 'summary',
    intent: String(details.intent || rawType || '').trim(),
    sub_intent: String(details.sub_intent || '').trim(),
    answer_mode: String(details.answer_mode || '').trim(),
    time_context: String(details.time_context || '').trim(),
    confidence: Number.isFinite(Number(details.confidence)) ? Number(details.confidence) : undefined,
    subtitle: String(details.subtitle || '').trim(),
    suggestions,
    insights,
    detected_entities: normalizeDetectedEntities(details.detected_entities),
    sources: Array.isArray(details.sources) ? details.sources : []
  };

  if (rawType === 'chat') {
    extra.mode = 'chat';
    extra.message = String(details.message || summary || answer || '').trim();
    return pruneEmptyFields(extra);
  }

  if (rawType === 'subjective_analysis') {
    extra.question = String(details.question || '').trim();
    return pruneEmptyFields(extra);
  }

  if (rawType === 'player_stats' || rawType === 'player_season_stats') {
    extra.entities = {
      player: details.player || {}
    };
    extra.player_description = String(details.player?.description || '').trim();
    extra.recent_matches = Array.isArray(details.recent_matches) ? details.recent_matches : [];
    extra.chartData = details.chartData && typeof details.chartData === 'object' ? details.chartData : null;
    return pruneEmptyFields(extra);
  }

  if (rawType === 'team_stats') {
    extra.entities = {
      team: details.team || {}
    };
    extra.team_description = String(details.team?.description || '').trim();
    extra.recent_matches = Array.isArray(details.recent_matches) ? details.recent_matches : [];
    return pruneEmptyFields(extra);
  }

  if (rawType === 'team_info') {
    extra.entities = {
      team: details.team || {}
    };
    extra.question = String(details.question || '').trim();
    extra.team_description = String(details.team?.description || '').trim();
    extra.recent_matches = Array.isArray(details.recent_matches) ? details.recent_matches : [];
    return pruneEmptyFields(extra);
  }

  if (rawType === 'team_squad' || rawType === 'playing_xi') {
    extra.entities = {
      team: details.team || {}
    };
    extra.team_description = String(details.team?.description || '').trim();
    extra.players = Array.isArray(details.players) ? details.players : [];
    extra.total_players = Number(details.total_players || 0);
    return pruneEmptyFields(extra);
  }

  if (rawType === 'compare_players') {
    extra.entities = {
      left: details.left || {},
      right: details.right || {}
    };
    extra.chartData = details.chartData && typeof details.chartData === 'object' ? details.chartData : null;
    return pruneEmptyFields(extra);
  }

  if (rawType === 'head_to_head') {
    extra.entities = {
      team1: details.team1 || '',
      team2: details.team2 || ''
    };
    extra.recent_matches = Array.isArray(details.recent_matches) ? details.recent_matches : [];
    return pruneEmptyFields(extra);
  }

  if (rawType === 'match_summary') {
    extra.match = details.match || {};
    return pruneEmptyFields(extra);
  }

  if (rawType === 'live_update') {
    extra.live_match = details.live_match || {};
    extra.next_match = details.next_match || {};
    extra.upcoming_matches = Array.isArray(details.upcoming_matches) ? details.upcoming_matches : [];
    extra.recent_matches = Array.isArray(details.recent_matches) ? details.recent_matches : [];
    extra.entities = details.player ? { player: details.player } : {};
    extra.provider_status = details.provider_status || {};
    return pruneEmptyFields(extra);
  }

  if (rawType === 'top_players') {
    extra.metric = String(details.metric || '').trim();
    extra.resolved_metric = String(details.resolved_metric || '').trim();
    extra.rows = Array.isArray(details.rows) ? details.rows : [];
    extra.chartData = details.chartData && typeof details.chartData === 'object' ? details.chartData : null;
    return pruneEmptyFields(extra);
  }

  if (rawType === 'record_lookup') {
    extra.question = String(details.question || '').trim();
    extra.metric = String(details.metric || '').trim();
    extra.resolved_metric = String(details.resolved_metric || '').trim();
    extra.rows = Array.isArray(details.rows) ? details.rows : [];
    return pruneEmptyFields(extra);
  }

  if (rawType === 'glossary') {
    extra.term = String(details.term || '').trim();
    extra.explanation = String(details.explanation || '').trim();
    return pruneEmptyFields(extra);
  }

  if (rawType === 'general_knowledge') {
    extra.mode = 'knowledge';
    extra.question = String(details.question || '').trim();
    extra.category = String(details.category || '').trim();
    extra.related_topics = Array.isArray(details.related_topics) ? details.related_topics : [];
    extra.examples = Array.isArray(details.examples) ? details.examples : [];
    extra.fallback_used = Boolean(details.fallback_used);
    return pruneEmptyFields(extra);
  }

  return pruneEmptyFields(extra);
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

function buildPhraseCandidates(text = '', { minWords = 1, maxWords = 4 } = {}) {
  const tokens = normalizeText(text).split(' ').filter(Boolean);
  const phrases = [];
  for (let size = Math.min(maxWords, tokens.length); size >= 1; size -= 1) {
    if (size < minWords) continue;
    for (let index = 0; index <= tokens.length - size; index += 1) {
      phrases.push(tokens.slice(index, index + size).join(' '));
    }
  }
  return uniqueNonEmpty(phrases);
}

function stripWikiMarkup(value = '') {
  let text = String(value || '');
  if (!text) return '';

  text = text
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<ref[\s\S]*?<\/ref>/gi, ' ')
    .replace(/<ref[^>]*\/>/gi, ' ')
    .replace(/&nbsp;/gi, ' ');

  let previous = '';
  while (text !== previous) {
    previous = text;
    text = text.replace(/{{[^{}]*}}/g, ' ');
  }

  text = text
    .replace(/\[\[(?:[^|\]]+\|)?([^\]]+)\]\]/g, '$1')
    .replace(/\[https?:\/\/[^\s\]]+\s*([^\]]*)\]/g, '$1')
    .replace(/'''|''/g, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  return text;
}

async function fetchWikipediaSummary(topic = '') {
  const cleanTopic = String(topic || '').trim();
  if (!cleanTopic) return null;
  const cacheKey = cleanTopic.toLowerCase();
  if (wikipediaSummaryCache.has(cacheKey)) {
    return wikipediaSummaryCache.get(cacheKey);
  }

  try {
    const response = await fetch(
      `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(cleanTopic.replace(/\s+/g, '_'))}`,
      {
        headers: {
          Accept: 'application/json',
          'User-Agent': 'Cricket-Intelligence-Console/1.0'
        }
      }
    );
    if (!response.ok) {
      wikipediaSummaryCache.set(cacheKey, null);
      return null;
    }
    const payload = await response.json();
    const summary = {
      title: String(payload?.title || cleanTopic).trim(),
      description: String(payload?.description || '').trim(),
      extract: String(payload?.extract || '').trim(),
      image: String(payload?.thumbnail?.source || '').trim(),
      wikipedia_url: String(payload?.content_urls?.desktop?.page || '').trim()
    };
    wikipediaSummaryCache.set(cacheKey, summary);
    return summary;
  } catch (_) {
    wikipediaSummaryCache.set(cacheKey, null);
    return null;
  }
}

async function fetchWikipediaWikitext(topic = '') {
  const cleanTopic = String(topic || '').trim();
  if (!cleanTopic) return '';
  const cacheKey = cleanTopic.toLowerCase();
  if (wikipediaWikitextCache.has(cacheKey)) {
    return wikipediaWikitextCache.get(cacheKey);
  }

  try {
    const response = await fetch(
      `https://en.wikipedia.org/w/api.php?action=parse&page=${encodeURIComponent(cleanTopic.replace(/\s+/g, '_'))}&prop=wikitext&formatversion=2&format=json`,
      {
        headers: {
          Accept: 'application/json',
          'User-Agent': 'Cricket-Intelligence-Console/1.0'
        }
      }
    );
    if (!response.ok) {
      wikipediaWikitextCache.set(cacheKey, '');
      return '';
    }
    const payload = await response.json();
    const wikitext = String(payload?.parse?.wikitext || '').trim();
    wikipediaWikitextCache.set(cacheKey, wikitext);
    return wikitext;
  } catch (_) {
    wikipediaWikitextCache.set(cacheKey, '');
    return '';
  }
}

function parseWikipediaInfoboxFields(wikitext = '') {
  const source = String(wikitext || '');
  const infoboxStart = source.indexOf('{{Infobox cricket team');
  if (infoboxStart < 0) return {};

  const infoboxEnd = source.indexOf('\n}}', infoboxStart);
  const infoboxBlock = source.slice(
    infoboxStart,
    infoboxEnd > infoboxStart ? infoboxEnd : Math.min(source.length, infoboxStart + 4000)
  );
  const fields = {};

  for (const line of infoboxBlock.split('\n')) {
    const match = line.match(/^\|\s*([^=]+?)\s*=\s*(.*)$/);
    if (!match) continue;
    const key = normalizeText(match[1]).replace(/\s+/g, '_');
    const value = stripWikiMarkup(match[2]);
    if (!key || !value) continue;
    fields[key] = value;
  }

  return fields;
}

function extractCountFromText(value = '') {
  const clean = stripWikiMarkup(value);
  const numericMatch = clean.match(/\b(\d+)\b/);
  if (numericMatch) return Number(numericMatch[1]);
  const wordMatch = clean.match(/\b(one|two|three|four|five|six|seven|eight|nine|ten)\b/i);
  return wordToNumber(wordMatch?.[1] || '');
}

function buildTeamTitleStats(fields = {}) {
  const titles = [];
  for (let index = 1; index <= 4; index += 1) {
    const title = String(fields[`title${index}`] || '').trim();
    const wins = extractCountFromText(fields[`title${index}wins`]);
    if (!title || wins === null) continue;
    titles.push({
      title,
      wins
    });
  }
  return titles;
}

function formatTitleWinLine(entry = {}) {
  const wins = Number(entry.wins || 0);
  const title = String(entry.title || '').trim();
  if (!title || !wins) return '';
  const titleLabel = title.includes('title') ? title : `${title} title${wins === 1 ? '' : 's'}`;
  return `${wins} ${titleLabel}`;
}

function buildTeamInfoAnswerFromWiki(question = '', teamName = '', wiki = null, fields = {}) {
  const extract = String(wiki?.extract || '').trim();
  const description = String(wiki?.description || '').trim();
  const text = `${description}. ${extract}`.trim();
  const normalizedQuestion = normalizeText(question);

  if (/\btroph(?:y|ies)|titles?\b/.test(normalizedQuestion)) {
    const titleLines = buildTeamTitleStats(fields).map(formatTitleWinLine).filter(Boolean);
    if (titleLines.length) {
      return `${teamName} have won ${titleLines.join(' and ')}.`;
    }
    const match =
      text.match(/(\d+|one|two|three|four|five|six|seven|eight|nine|ten)[-\s]*(?:time\s+)?(?:champions?|titles?|trophies)/i) ||
      text.match(/won\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+(?:titles?|trophies)/i);
    const count = wordToNumber(match?.[1] || '');
    if (count !== null) {
      return `${teamName} have won ${count} major titles based on the current Wikipedia summary.`;
    }
  }

  if (/\bcaptain\b/.test(normalizedQuestion)) {
    if (fields.captain) {
      return `${teamName} are captained by ${fields.captain}.`;
    }
    const match =
      text.match(/captained by\s+([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){0,3})/i) ||
      text.match(/captain(?:ed)?\s+by\s+([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){0,3})/i);
    if (match?.[1]) {
      return `${teamName} are captained by ${String(match[1]).trim()} based on the current Wikipedia summary.`;
    }
  }

  if (/\bcoach\b/.test(normalizedQuestion) && fields.coach) {
    return `${teamName} are coached by ${fields.coach}.`;
  }

  if (/\bowner\b/.test(normalizedQuestion) && fields.owner) {
    return `${teamName} are owned by ${fields.owner}.`;
  }

  if (/\b(home ground|stadium|ground|venue)\b/.test(normalizedQuestion) && fields.ground) {
    return `${teamName} play their home matches at ${fields.ground}.`;
  }

  if (/\bfounded\b|\bwhen started\b|\bestablished\b/.test(normalizedQuestion) && fields.founded) {
    return `${teamName} were founded in ${fields.founded}.`;
  }

  if (!text) return '';
  return extract || text;
}

function lookupKnownRecord(question = '') {
  const normalizedQuestion = normalizeText(question);

  if (
    ((/\bhighest score\b/.test(normalizedQuestion) || /\bhighest individual score\b/.test(normalizedQuestion)) &&
      /\bodi\b|\bone day international\b/.test(normalizedQuestion)) ||
    /\bhighest score in odi\b/.test(normalizedQuestion) ||
    /\bhighest individual score in odi\b/.test(normalizedQuestion)
  ) {
    return {
      title: 'Highest ODI Score',
      metric: 'highest_score',
      resolved_metric: 'exact_record',
      answer:
        'Rohit Sharma holds the men\'s ODI highest-score record with 264 against Sri Lanka at Eden Gardens on 13 November 2014.',
      stats: {
        player: 'Rohit Sharma',
        record: 264,
        opposition: 'Sri Lanka',
        venue: 'Eden Gardens',
        date: '2014-11-13'
      },
      player_name: 'Rohit Sharma'
    };
  }

  if (
    (/\bfastest century\b|\bfastest 100\b/.test(normalizedQuestion) &&
      /\bodi\b|\bone day international\b/.test(normalizedQuestion)) ||
    /\bfastest century in odi\b/.test(normalizedQuestion)
  ) {
    return {
      title: 'Fastest ODI Century',
      metric: 'fastest_century',
      resolved_metric: 'exact_record',
      answer:
        'AB de Villiers holds the men\'s ODI fastest-century record with a hundred in 31 balls against West Indies in Johannesburg on 18 January 2015.',
      stats: {
        player: 'AB de Villiers',
        balls: 31,
        opposition: 'West Indies',
        date: '2015-01-18'
      },
      player_name: 'AB de Villiers'
    };
  }

  if (
    /\bbest bowling figures\b/.test(normalizedQuestion) &&
    /\bodi\b|\bone day international\b/.test(normalizedQuestion)
  ) {
    return {
      title: 'Best ODI Bowling Figures',
      metric: 'best_bowling_figures',
      resolved_metric: 'exact_record',
      answer:
        'Chaminda Vaas holds the men\'s ODI best-bowling record with 8 for 19 against Zimbabwe in Colombo in December 2001.',
      stats: {
        player: 'Chaminda Vaas',
        wickets: 8,
        runs_conceded: 19,
        opposition: 'Zimbabwe',
        year: 2001
      },
      player_name: 'Chaminda Vaas'
    };
  }

  if (
    (/\bfastest century\b|\bfastest 100\b/.test(normalizedQuestion) &&
      /\bt20\b|\bt20i\b|\btwenty20\b/.test(normalizedQuestion))
  ) {
    return {
      title: 'Fastest T20I Century',
      metric: 'fastest_century',
      resolved_metric: 'exact_record',
      answer:
        'Sahil Chauhan holds the men\'s T20I fastest-century record with a hundred in 27 balls against Cyprus on 17 June 2024.',
      stats: {
        player: 'Sahil Chauhan',
        balls: 27,
        opposition: 'Cyprus',
        date: '2024-06-17'
      },
      player_name: 'Sahil Chauhan'
    };
  }

  if (
    /\bmost wickets\b/.test(normalizedQuestion) &&
    /\b(world cup|wc)\b/.test(normalizedQuestion)
  ) {
    return {
      title: 'Most World Cup Wickets',
      metric: 'most_wickets_world_cup',
      resolved_metric: 'exact_record',
      answer:
        'Glenn McGrath holds the men\'s Cricket World Cup wickets record with 71 wickets.',
      stats: {
        player: 'Glenn McGrath',
        wickets: 71,
        tournament: 'Cricket World Cup'
      },
      player_name: 'Glenn McGrath'
    };
  }

  if (
    /\blowest total\b/.test(normalizedQuestion) &&
    /\btest\b/.test(normalizedQuestion)
  ) {
    return {
      title: 'Lowest Test Total',
      metric: 'lowest_total',
      resolved_metric: 'exact_record',
      answer:
        'New Zealand hold the men\'s Test lowest-total record after being bowled out for 26 against England in Auckland in 1955.',
      stats: {
        team: 'New Zealand',
        total: 26,
        opposition: 'England',
        venue: 'Auckland',
        year: 1955
      }
    };
  }

  return null;
}

function resolutionWeight(status = '') {
  if (status === 'resolved') return 3;
  if (status === 'clarify') return 2;
  if (status === 'not_found') return 1;
  return 0;
}

async function resolveEntityWithFallback(entityType, candidates = [], { question = '' } = {}) {
  const queries = buildEntityCandidates(...candidates);
  let best = {
    query: queries[0] || '',
    resolution: { status: 'missing', score: 0 }
  };

  for (const query of queries) {
    const resolution = await resolveEntityStrict(entityType, query, { question });
    if (resolution.status === 'resolved') {
      const currentScore = Number(resolution.score || 0);
      const bestScore = Number(best.resolution?.score || 0);
      if (best.resolution.status !== 'resolved' || currentScore > bestScore) {
        best = { query, resolution };
      }
      continue;
    }
    if (best.resolution.status !== 'resolved' && resolutionWeight(resolution.status) > resolutionWeight(best.resolution.status)) {
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
  const type = toUnifiedResponseType(details);
  const title = String(details.title || 'Cricket Intelligence').trim() || 'Cricket Intelligence';
  const summarySource = String(details.summary || answer || NOT_AVAILABLE_MESSAGE).trim() || NOT_AVAILABLE_MESSAGE;
  const summary = summarySource.length > 1200 ? `${summarySource.slice(0, 1197).trim()}...` : summarySource;
  const image = buildUnifiedImage(details);
  const stats = buildUnifiedStats(details);
  const extra = buildUnifiedExtra(details, summary, answer, suggestions);
  const detectedEntities = normalizeDetectedEntities(extra.detected_entities);
  return {
    type,
    title,
    image,
    summary,
    stats,
    extra,
    detected_entities: detectedEntities
  };
}

function applySessionContext(question = '', session = null) {
  const text = String(question || '').trim();
  const playerName = String(session?.context?.player_name || '').trim();
  const teamName = String(session?.context?.team_name || '').trim();
  let rewritten = text;
  let usedContext = false;

  if (!text) return text;
  if (PLAYER_CONTEXT_PRONOUN_REGEX.test(text) && playerName) {
    rewritten = rewritten
      .replace(/\bhis\b/gi, `${playerName}'s`)
      .replace(/\bhe\b/gi, playerName)
      .replace(/\bhim\b/gi, playerName)
      .replace(/\bshe\b/gi, playerName)
      .replace(/\bher\b/gi, `${playerName}'s`);
    usedContext = true;
  }
  if (TEAM_CONTEXT_PRONOUN_REGEX.test(text) && teamName) {
    rewritten = rewritten
      .replace(/\btheir\b/gi, `${teamName}'s`)
      .replace(/\bthey\b/gi, teamName)
      .replace(/\bthem\b/gi, teamName);
    usedContext = true;
  }
  if (!usedContext) return text;

  return [
    rewritten,
    '',
    `Resolved conversation context: player=${playerName || 'n/a'}; team=${teamName || 'n/a'}.`
  ].join('\n');
}

function buildSessionContextPatch(route = {}, publicDetails = {}) {
  const patch = {};
  const type = String(publicDetails?.type || '').trim();

  if (route?.season) patch.season = String(route.season);
  if (route?.format) patch.format = String(route.format);
  if (route?.action) patch.action = String(route.action);

  if (type === 'player_stats' && publicDetails.player?.name) {
    patch.player_name = String(publicDetails.player.name || '').trim();
    if (publicDetails.player.id) {
      patch.player_id = String(publicDetails.player.id || '').trim();
    }
    if (publicDetails.player.team) {
      patch.team_name = String(publicDetails.player.team || '').trim();
    }
    return patch;
  }

  if (type === 'team_stats' && publicDetails.team?.name) {
    patch.team_name = String(publicDetails.team.name || '').trim();
    if (publicDetails.team.id) {
      patch.team_id = String(publicDetails.team.id || '').trim();
    }
    return patch;
  }

  if (type === 'live_update') {
    if (publicDetails.player?.name) {
      patch.player_name = String(publicDetails.player.name || '').trim();
      if (publicDetails.player.id) {
        patch.player_id = String(publicDetails.player.id || '').trim();
      }
    }
    if (route?.team) {
      patch.team_name = String(route.team || '').trim();
    }
    return patch;
  }

  if (route?.action === 'player_stats' || route?.action === 'player_season_stats') {
    if (route.player) {
      patch.player_name = String(route.player || '').trim();
    }
  }

  if (route?.action === 'team_stats' && route.team) {
    patch.team_name = String(route.team || '').trim();
  }

  return patch;
}

function syncSessionState(session, route = {}, structuredContext = {}, publicDetails = {}) {
  if (!session) return;

  const resultData =
    structuredContext?.result?.data && typeof structuredContext.result.data === 'object'
      ? structuredContext.result.data
      : {};

  if (String(resultData.type || '').trim() === 'name_resolution') {
    setPendingClarification(session, {
      entity: String(resultData.entity || '').trim(),
      query: String(resultData.query || '').trim(),
      choices: Array.isArray(resultData.choices) ? resultData.choices.slice(0, 5) : []
    });
    return;
  }

  clearPendingClarification(session);
  const patch = buildSessionContextPatch(route, publicDetails);
  if (Object.keys(patch).length) {
    updateContext(session, patch);
  }
}

function applySessionRouteFallback(route = {}, question = '', session = null) {
  const normalizedQuestion = normalizeText(question);
  const playerName = String(session?.context?.player_name || '').trim();
  const teamName = String(session?.context?.team_name || '').trim();
  const playerPronoun = PLAYER_CONTEXT_PRONOUN_REGEX.test(question);
  const teamPronoun = TEAM_CONTEXT_PRONOUN_REGEX.test(question);
  const action = String(route?.action || '').trim();
  const isStatsLikeQuestion = FACT_LOOKUP_REGEX.test(normalizedQuestion);

  if (playerPronoun && playerName && isStatsLikeQuestion && ['glossary', 'not_supported', ''].includes(action)) {
    return {
      ...route,
      action: 'player_stats',
      intent: 'player_stats',
      sub_intent: 'player_stats',
      answer_mode: 'fact',
      player: playerName,
      team: '',
      term: '',
      query: playerName
    };
  }

  if (teamPronoun && teamName && isStatsLikeQuestion && ['glossary', 'not_supported', ''].includes(action)) {
    return {
      ...route,
      action: 'team_stats',
      intent: 'team_stats',
      sub_intent: 'team_stats',
      answer_mode: 'fact',
      team: teamName,
      player: '',
      term: '',
      query: teamName
    };
  }

  return route;
}

function emitStatus(onStatus, payload) {
  if (typeof onStatus !== 'function') return;
  onStatus(payload);
}

function actionStatusMessage(action = '') {
  if (isGeneralConversationAction(action)) return 'Responding conversationally.';
  if (action === 'general_knowledge') return 'Checking the local cricket knowledge base.';
  if (action === 'live_update') return 'Checking live scores.';
  if (action === 'subjective_analysis') return 'Analyzing the debate.';
  if (action === 'player_stats' || action === 'player_season_stats') {
    return 'Searching player stats.';
  }
  if (action === 'team_stats') return 'Searching team stats.';
  if (action === 'team_squad') return 'Loading team squad.';
  if (action === 'playing_xi') return 'Building playing XI.';
  if (action === 'team_info') return 'Checking team information.';
  if (action === 'match_summary') return 'Searching match details.';
  if (action === 'compare_players') return 'Comparing players.';
  if (action === 'head_to_head') return 'Checking head-to-head results.';
  if (action === 'record_lookup') return 'Checking cricket records.';
  if (action === 'top_players') return 'Searching top performers.';
  if (action === 'glossary') return 'Preparing a short explanation.';
  return 'Searching cricket stats.';
}

function normalizeRoute(route = {}) {
  const entities = route?.entities && typeof route.entities === 'object' ? route.entities : {};
  const merged = { ...entities, ...route };
  const action = SUPPORTED_ACTIONS.includes(merged.action) ? merged.action : 'not_supported';
  const dataSources = normalizeDataSources(
    merged.data_sources ||
      merged.dataSources ||
      merged.required_data_sources ||
      merged.requiredDataSources ||
      []
  );
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
    min_overs: pickFirst(merged.min_overs),
    intent: pickFirst(merged.intent, merged.action),
    sub_intent: pickFirst(merged.sub_intent, merged.subIntent),
    time_context: pickFirst(merged.time_context, merged.timeContext),
    answer_mode: pickFirst(merged.answer_mode, merged.answerMode),
    confidence: Number.isFinite(Number(merged.confidence)) ? Number(merged.confidence) : null,
    data_sources: dataSources
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

function normalizeResolvedEntity(entityType, item = {}) {
  if (!item || typeof item !== 'object') return item;
  if (entityType === 'player') {
    const canonicalName = String(item.canonical_name || item.name || '').trim() || String(item.name || '').trim();
    return {
      ...item,
      dataset_name: String(item.name || '').trim(),
      canonical_name: canonicalName,
      name: canonicalName
    };
  }
  return item;
}

function buildResolutionChoices(entityType, matches = []) {
  return uniqueNonEmpty(
    matches.slice(0, 5).map((item) => {
      if (entityType === 'player') {
        return String(item.canonical_name || item.name || '').trim();
      }
      return String(item.name || '').trim();
    })
  );
}

async function fetchLivePlayerCandidates(query = '') {
  try {
    const result = await searchCricApiPlayers({ q: query, limit: 5 });
    return Array.isArray(result?.items)
      ? result.items
          .map((item) => ({
            id: String(item.id || '').trim(),
            name: String(item.name || '').trim(),
            canonical_name: String(item.name || '').trim(),
            team: String(item.country || item.team || '').trim(),
            role: '',
            source: 'cricapi',
            is_active: true
          }))
          .filter((item) => item.id && item.name)
      : [];
  } catch (_) {
    return [];
  }
}

async function resolveEntityStrict(entityType, query = '', { question = '' } = {}) {
  const cleanQuery = String(query || '').trim();
  if (!cleanQuery) return { status: 'missing' };

  const rawResolution =
    entityType === 'team'
      ? await resolveVectorTeam(cleanQuery)
      : await resolveVectorPlayer(cleanQuery);

  const matches = Array.isArray(rawResolution?.matches) ? rawResolution.matches : [];
  const liveLeaning = entityType === 'player' && isLiveLeaningQuestion(question);

  if (entityType === 'player' && liveLeaning) {
    const liveCandidates = await fetchLivePlayerCandidates(cleanQuery);
    const rankedArchiveMatches = rankEntityCandidates(
      cleanQuery,
      matches.map((item) => ({
        ...normalizeResolvedEntity(entityType, item),
        source: 'vector'
      })),
      {
        liveLeaning: true,
        liveAliases: liveCandidates.map((item) => item.name)
      }
    );

    if (rankedArchiveMatches.length) {
      const topScore = Number(
        rankedArchiveMatches[0]?.weighted_score || rankedArchiveMatches[0]?.score || 0
      );
      const secondScore = Number(
        rankedArchiveMatches[1]?.weighted_score || rankedArchiveMatches[1]?.score || 0
      );
      const choices = buildResolutionChoices(entityType, rankedArchiveMatches);

      if (topScore < 0.72 || secondScore >= topScore - 0.08) {
        return {
          status: 'clarify',
          query: cleanQuery,
          choices,
          score: topScore
        };
      }

      return {
        status: 'resolved',
        item: normalizeResolvedEntity(entityType, rankedArchiveMatches[0]),
        choices,
        score: topScore
      };
    }

    const rankedLiveMatches = rankEntityCandidates(cleanQuery, liveCandidates, {
      liveLeaning: true,
      liveAliases: liveCandidates.map((item) => item.name)
    });
    if (rankedLiveMatches.length && Number(rankedLiveMatches[0]?.weighted_score || 0) >= 0.7) {
      return {
        status: 'resolved',
        item: normalizeResolvedEntity(entityType, rankedLiveMatches[0]),
        choices: buildResolutionChoices(entityType, rankedLiveMatches),
        score: Number(rankedLiveMatches[0].weighted_score || 0)
      };
    }
  }

  if (rawResolution?.found && rawResolution?.item) {
    return {
      status: 'resolved',
      item: normalizeResolvedEntity(entityType, rawResolution.item),
      choices: buildResolutionChoices(entityType, matches),
      score: Number(rawResolution?.score || matches[0]?.score || 0)
    };
  }
  if (matches.length) {
    const topScore = Number(matches[0]?.score || 0);
    const secondScore = Number(matches[1]?.score || 0);
    const choices = buildResolutionChoices(entityType, matches);

    // Avoid silently collapsing vague queries onto a famous player or team.
    // If the top fuzzy match is weak or close to the runner-up, force clarification instead.
    if (topScore < 0.72 || secondScore >= topScore - 0.08) {
      return {
        status: 'clarify',
        query: cleanQuery,
        choices,
        score: topScore
      };
    }

    return {
      status: 'resolved',
      item: normalizeResolvedEntity(entityType, matches[0]),
      choices,
      score: topScore
    };
  }

  return {
    status: 'not_found',
    query: cleanQuery,
    choices: []
  };
}

function toPlayerStats(player = {}) {
  return {
    matches: Number(player.matches || 0),
    runs: Number(player.runs || 0),
    average: Number(player.average || 0),
    strike_rate: Number(player.strike_rate || 0),
    wickets: Number(player.wickets || 0),
    economy: Number(player.economy || 0),
    fours: Number(player.fours || 0),
    sixes: Number(player.sixes || 0)
  };
}

function toTeamStats(team = {}) {
  const matches = Number(team.matches || 0);
  const runs = Number(team.runs || 0);
  return {
    matches,
    wins: Number(team.wins || 0),
    losses: Number(team.losses || 0),
    no_result: Number(team.no_result || 0),
    win_rate: Number(team.win_rate || 0),
    average_score: matches > 0 ? Number((runs / matches).toFixed(2)) : 0,
    runs
  };
}

function hasMeaningfulStats(stats = {}) {
  if (!stats || typeof stats !== 'object') return false;
  return ['matches', 'runs', 'average', 'strike_rate', 'wickets', 'economy', 'fours', 'sixes'].some(
    (key) => Number(stats[key] || 0) > 0
  );
}

function mergeEspnCareerIntoPlayer(player = {}, espnPlayer = null) {
  if (!espnPlayer || typeof espnPlayer !== 'object') return player;
  const careerSummary =
    espnPlayer.career_summary && typeof espnPlayer.career_summary === 'object'
      ? espnPlayer.career_summary
      : {};
  const fallbackStats = hasMeaningfulStats(player.stats || {}) ? player.stats || {} : careerSummary;

  return {
    ...player,
    id: String(player.id || espnPlayer.id || '').trim(),
    player_key: String(player.player_key || player.id || espnPlayer.id || '').trim(),
    name: String(player.name || espnPlayer.name || '').trim(),
    full_name: String(player.full_name || espnPlayer.full_name || player.name || espnPlayer.name || '').trim(),
    team: String(player.team || espnPlayer.team || '').trim(),
    team_name: String(player.team_name || player.team || espnPlayer.team || '').trim(),
    role: String(player.role || espnPlayer.role || '').trim(),
    batting_style: String(player.batting_style || espnPlayer.batting_style || '').trim(),
    bowling_style: String(player.bowling_style || espnPlayer.bowling_style || '').trim(),
    major_teams: Array.isArray(espnPlayer.major_teams) ? espnPlayer.major_teams : [],
    source: String(player.source || espnPlayer.source || '').trim(),
    stats: fallbackStats,
    career: {
      summary: careerSummary,
      by_format:
        espnPlayer.stats_by_format && typeof espnPlayer.stats_by_format === 'object'
          ? espnPlayer.stats_by_format
          : {}
    }
  };
}

function buildComparisonChartData(left = {}, right = {}) {
  return {
    type: 'radar',
    title: 'Player Skill Comparison',
    labels: ['Matches', 'Runs', 'Average', 'Strike Rate', 'Wickets', 'Sixes'],
    datasets: [
      {
        label: String(left.name || 'Player A'),
        color: '#22c55e',
        data: [
          Number(left.stats?.matches || 0),
          Number(left.stats?.runs || 0),
          Number(left.stats?.average || 0),
          Number(left.stats?.strike_rate || 0),
          Number(left.stats?.wickets || 0),
          Number(left.stats?.sixes || 0)
        ]
      },
      {
        label: String(right.name || 'Player B'),
        color: '#38bdf8',
        data: [
          Number(right.stats?.matches || 0),
          Number(right.stats?.runs || 0),
          Number(right.stats?.average || 0),
          Number(right.stats?.strike_rate || 0),
          Number(right.stats?.wickets || 0),
          Number(right.stats?.sixes || 0)
        ]
      }
    ]
  };
}

function buildLeaderboardChartData(metric = '', rows = []) {
  const chartRows = Array.isArray(rows) ? rows.slice(0, 8) : [];
  return {
    type: 'bar',
    title: `Top ${titleCaseMetric(metric || 'performers')}`,
    labels: chartRows.map((row) => String(row.player || row.team || 'Record')),
    datasets: [
      {
        label: titleCaseMetric(metric || 'value'),
        color: '#f59e0b',
        data: chartRows.map((row) => Number(row.value || 0))
      }
    ]
  };
}

function normalizeSquadRole(role = '') {
  const normalizedRole = normalizeText(role);
  if (!normalizedRole) return 'Player';
  if (/\bkeeper\b|\bwicket/.test(normalizedRole)) return 'Wicketkeeper';
  if (/\ball\b/.test(normalizedRole)) return 'All-rounder';
  if (/\bbowl/.test(normalizedRole)) return 'Bowler';
  if (/\bbat/.test(normalizedRole)) return 'Batsman';
  return String(role || 'Player').trim() || 'Player';
}

async function buildSquadPlayersForTeam(teamName = '', { limit = 0 } = {}) {
  const players = await loadPlayerProfiles();
  const cleanTeam = normalizeText(teamName);
  const squad = players
    .filter((player) => normalizeText(player.team) === cleanTeam)
    .sort(
      (left, right) =>
        Number(right.matches || 0) - Number(left.matches || 0) ||
        Number(right.runs || 0) - Number(left.runs || 0) ||
        Number(right.wickets || 0) - Number(left.wickets || 0) ||
        String(left.canonical_name || left.name || '').localeCompare(String(right.canonical_name || right.name || ''))
    );

  const selected = limit > 0 ? squad.slice(0, limit) : squad;
  const profiles = await Promise.all(
    selected.map(async (player) => {
      const canonicalName = String(player.canonical_name || player.name || '').trim();
      const wikiSummary = await fetchWikipediaSummary(canonicalName).catch(() => null);
      return {
        name: canonicalName,
        role: normalizeSquadRole(player.role),
        image: String(wikiSummary?.image || '').trim(),
        matches: Number(player.matches || 0),
        runs: Number(player.runs || 0),
        wickets: Number(player.wickets || 0)
      };
    })
  );

  return {
    totalPlayers: squad.length,
    players: profiles
  };
}

function toPublicMatch(match = {}) {
  const team1 = String(match.team1 || '').trim();
  const team2 = String(match.team2 || '').trim();
  const inningsSummary = String(match.innings_summary || '').trim();
  const winner = String(match.winner || '').trim();
  return {
    id: String(match.id || '').trim(),
    name: team1 && team2 ? `${team1} vs ${team2}` : 'Match Summary',
    teams: [team1, team2].filter(Boolean),
    date: String(match.date || '').trim(),
    venue: String(match.venue || '').trim(),
    status: winner ? `${winner} won` : 'Result unavailable',
    winner,
    match_type: String(match.format || '').trim(),
    summary: uniqueNonEmpty([
      winner ? `${winner} won.` : '',
      inningsSummary
    ]).join(' '),
    top_batters: [],
    top_bowlers: [],
    score: []
  };
}

function compareMetricLine(label = '', leftName = '', leftValue = 0, rightName = '', rightValue = 0) {
  if (leftValue === rightValue) {
    return `${label}: ${leftName} and ${rightName} are level at ${formatStatValue(leftValue)}.`;
  }
  const leader = leftValue > rightValue ? leftName : rightName;
  const trailingValue = leftValue > rightValue ? rightValue : leftValue;
  const winningValue = Math.max(leftValue, rightValue);
  return `${label}: ${leader} leads ${formatStatValue(winningValue)} to ${formatStatValue(trailingValue)}.`;
}

function resolveLeaderboardMetric(routeMetric = '', question = '') {
  const explicit = String(routeMetric || '').trim();
  const normalizedQuestion = normalizeText(question);
  if (explicit === 'sixes') {
    return { requested_metric: 'sixes', resolved_metric: 'sixes', note: '' };
  }
  if (explicit === 'fours') {
    return { requested_metric: 'fours', resolved_metric: 'fours', note: '' };
  }
  if (/\bfastest\s+(?:50|fifty|100|century)\b|\bquickest\s+(?:50|fifty|100|century)\b|\bmost aggressive batting\b/.test(normalizedQuestion)) {
    return {
      requested_metric: explicit || 'fastest_50',
      resolved_metric: 'strike_rate',
      note: 'Using strike rate as the nearest archived proxy for fastest scoring.'
    };
  }
  if (/\bmost six(?:es)?\b|\bbig hitters?\b|\bpower hitters?\b/.test(normalizedQuestion)) {
    return { requested_metric: 'sixes', resolved_metric: 'sixes', note: '' };
  }
  if (/\bmost four(?:s)?\b/.test(normalizedQuestion)) {
    return { requested_metric: 'fours', resolved_metric: 'fours', note: '' };
  }
  const metric = explicit || guessMetric(question);
  return { requested_metric: metric, resolved_metric: metric, note: '' };
}

async function runPlayerAction(action, route, question) {
  const { query: playerQuery, resolution } = await resolveEntityWithFallback(
    'player',
    [route.player, removeGenericWords(question), question],
    { question }
  );
  if (resolution.status !== 'resolved') {
    return unresolvedEntityResult('player', playerQuery, resolution);
  }
  const player = resolution.item;
  const stats = toPlayerStats(player);
  const season = toSeason(pickFirst(route.season, guessSeason(question)));
  const format = pickFirst(route.format, guessFormat(question));
  const scopeNote = season
    ? `Season-specific splits are not indexed in the current vector archive, so this is the latest verified overall profile for ${player.name}.`
    : '';

  return {
    answer: uniqueNonEmpty([
      hasMeaningfulStats(stats)
        ? `${player.name} has ${formatStatValue(stats.runs)} runs from ${formatStatValue(stats.matches)} archived matches, with an average of ${formatStatValue(stats.average)} and strike rate ${formatStatValue(stats.strike_rate)}.`
        : `${player.name} was resolved as the active player match, but detailed archive totals are currently limited. Live and fallback profile sources will be used to enrich the response.`,
      stats.wickets ? `${player.name} has also taken ${formatStatValue(stats.wickets)} wickets.` : '',
      scopeNote
    ]).join(' '),
    data: {
      type: 'player_stats',
      title: player.name,
      subtitle: [player.team, format, season].filter(Boolean).join(' | '),
      player: {
        id: player.id,
        player_key: player.id,
        name: player.name,
        full_name: player.canonical_name || player.name,
        canonical_name: player.canonical_name || player.name,
        dataset_name: player.dataset_name || player.name,
        team: player.team,
        team_name: player.team,
        role: player.role,
        source: String(player.source || 'vector').trim() || 'vector'
      },
      stats,
      recent_matches: []
    },
    followups: ['Compare two players', 'Show recent live scores', 'Show team head to head']
  };
}

async function runTeamStats(route, question) {
  const { query: teamQuery, resolution } = await resolveEntityWithFallback('team', [
    route.team,
    ...buildPhraseCandidates(question),
    removeGenericWords(question),
    question
  ]);
  if (resolution.status !== 'resolved') {
    return unresolvedEntityResult('team', teamQuery, resolution);
  }
  const team = resolution.item;
  const season = toSeason(pickFirst(route.season, guessSeason(question)));
  const format = pickFirst(route.format, guessFormat(question));
  const recentMatches = (await findMatchesForTeam(team.name, { limit: 5, year: season, format })).map(toPublicMatch);
  const stats = toTeamStats(team);

  return {
    answer: `${team.name} has ${formatStatValue(stats.wins)} wins from ${formatStatValue(stats.matches)} archived matches, a win rate of ${formatStatValue(stats.win_rate)}%, and an average team score of ${formatStatValue(stats.average_score)}.`,
    data: {
      type: 'team_stats',
      title: team.name,
      subtitle: [format, season].filter(Boolean).join(' | '),
      team: {
        id: team.id,
        name: team.name
      },
      stats: {
        ...stats,
        recent_matches: recentMatches
      }
    },
    followups: ['Show team head to head', 'Show recent live scores', 'Show upcoming matches']
  };
}

async function runMatchSummary(route, question) {
  const season = toSeason(pickFirst(route.season, guessSeason(question)));
  const format = pickFirst(route.format, guessFormat(question));
  const matchId = pickFirst(route.match_id, guessMatchId(question));
  if (matchId) {
    const directMatch = await getVectorMatchById(matchId);
    if (!directMatch) return unavailableResult('I could not find that archived match.');
    const publicMatch = toPublicMatch(directMatch);
    return {
      answer: publicMatch.summary || `${publicMatch.name} is available in the archive.`,
      data: {
        type: 'match_summary',
        match: publicMatch
      },
      followups: ['Show team head to head', 'Show live scores', 'Show upcoming matches']
    };
  }

  const vs = parseVsSides(question) || {};
  const leftLookup = vs.left
    ? await resolveEntityWithFallback('team', [route.team1, vs.left])
    : { query: '', resolution: null };
  const rightLookup = vs.right
    ? await resolveEntityWithFallback('team', [route.team2, vs.right])
    : { query: '', resolution: null };

  if (leftLookup.resolution && leftLookup.resolution.status !== 'resolved') {
    return unresolvedEntityResult('team', leftLookup.query, leftLookup.resolution);
  }
  if (rightLookup.resolution && rightLookup.resolution.status !== 'resolved') {
    return unresolvedEntityResult('team', rightLookup.query, rightLookup.resolution);
  }

  let match = null;
  if (leftLookup.resolution?.item && rightLookup.resolution?.item) {
    match = (await findMatchesByTeams(
      [leftLookup.resolution.item.name, rightLookup.resolution.item.name],
      { limit: 1, year: season, format }
    ))[0] || null;
  } else {
    const teamHint = pickFirst(route.team, leftLookup.resolution?.item?.name, rightLookup.resolution?.item?.name);
    if (teamHint) {
      match = (await findMatchesForTeam(teamHint, { limit: 1, year: season, format }))[0] || null;
    }
  }

  if (!match) {
    return unavailableResult('I could not find a matching archived match summary.');
  }

  const publicMatch = toPublicMatch(match);
  return {
    answer: publicMatch.summary || `${publicMatch.name} is available in the archive.`,
    data: {
      type: 'match_summary',
      match: publicMatch
    },
    followups: ['Show team head to head', 'Show live scores', 'Show upcoming matches']
  };
}

async function runComparePlayers(route, question) {
  const vs = parseVsSides(question) || {};
  const leftLookup = await resolveEntityWithFallback('player', [route.player1, vs.left], { question });
  const rightLookup = await resolveEntityWithFallback('player', [route.player2, vs.right], { question });
  if (leftLookup.resolution.status !== 'resolved') {
    return unresolvedEntityResult('player', leftLookup.query, leftLookup.resolution);
  }
  if (rightLookup.resolution.status !== 'resolved') {
    return unresolvedEntityResult('player', rightLookup.query, rightLookup.resolution);
  }

  const left = leftLookup.resolution.item;
  const right = rightLookup.resolution.item;
  const leftStats = toPlayerStats(left);
  const rightStats = toPlayerStats(right);

  return {
    answer: [
      `${left.name} vs ${right.name}.`,
      compareMetricLine('Runs', left.name, leftStats.runs, right.name, rightStats.runs),
      compareMetricLine('Average', left.name, leftStats.average, right.name, rightStats.average),
      compareMetricLine('Strike Rate', left.name, leftStats.strike_rate, right.name, rightStats.strike_rate),
      compareMetricLine('Wickets', left.name, leftStats.wickets, right.name, rightStats.wickets)
    ].join(' '),
    data: {
      type: 'compare_players',
      left: {
        id: left.id,
        player_key: left.id,
        name: left.name,
        full_name: left.canonical_name || left.name,
        canonical_name: left.canonical_name || left.name,
        team: left.team,
        role: left.role,
        stats: leftStats
      },
      right: {
        id: right.id,
        player_key: right.id,
        name: right.name,
        full_name: right.canonical_name || right.name,
        canonical_name: right.canonical_name || right.name,
        team: right.team,
        role: right.role,
        stats: rightStats
      },
      chartData: buildComparisonChartData(
        { name: left.name, stats: leftStats },
        { name: right.name, stats: rightStats }
      )
    },
    followups: ['Show recent live scores', 'Show team head to head', 'Show top batters']
  };
}

async function runHeadToHead(route, question) {
  const vs = parseVsSides(question) || {};
  const leftLookup = await resolveEntityWithFallback('team', [route.team1, vs.left]);
  const rightLookup = await resolveEntityWithFallback('team', [route.team2, vs.right]);
  if (leftLookup.resolution.status !== 'resolved') {
    return unresolvedEntityResult('team', leftLookup.query, leftLookup.resolution);
  }
  if (rightLookup.resolution.status !== 'resolved') {
    return unresolvedEntityResult('team', rightLookup.query, rightLookup.resolution);
  }

  const left = leftLookup.resolution.item;
  const right = rightLookup.resolution.item;
  const season = toSeason(pickFirst(route.season, guessSeason(question)));
  const format = pickFirst(route.format, guessFormat(question));
  const matches = await findMatchesByTeams([left.name, right.name], { limit: 5000, year: season, format });

  const stats = matches.reduce(
    (accumulator, match) => {
      accumulator.matches += 1;
      if (!match.winner) {
        accumulator.no_result += 1;
      } else if (normalizeText(match.winner) === normalizeText(left.name)) {
        accumulator.wins_team_a += 1;
      } else if (normalizeText(match.winner) === normalizeText(right.name)) {
        accumulator.wins_team_b += 1;
      } else {
        accumulator.no_result += 1;
      }
      return accumulator;
    },
    {
      matches: 0,
      wins_team_a: 0,
      wins_team_b: 0,
      no_result: 0
    }
  );

  return {
    answer:
      stats.matches > 0
        ? `${left.name} and ${right.name} have ${formatStatValue(stats.matches)} archived meetings. ${left.name} won ${formatStatValue(stats.wins_team_a)}, ${right.name} won ${formatStatValue(stats.wins_team_b)}, and ${formatStatValue(stats.no_result)} ended without a result.`
        : `I could not find an archived head-to-head record for ${left.name} vs ${right.name} in the current filter scope.`,
    data: {
      type: 'head_to_head',
      team1: left.name,
      team2: right.name,
      stats: {
        ...stats,
        recent_matches: matches.slice(0, 5).map(toPublicMatch)
      }
    },
    followups: ['Show live scores', 'Show upcoming matches', 'Compare two players']
  };
}

async function runTopPlayers(route, question) {
  const metricInfo = resolveLeaderboardMetric(pickFirst(route.metric), question);
  const requestedLimit = Number(route.limit || 10);
  const allRows = await getTopPlayersByMetric(metricInfo.resolved_metric, {
    limit: Math.max(50, requestedLimit * 10)
  });
  let rows = [...allRows];
  if (metricInfo.resolved_metric === 'strike_rate') {
    rows = rows.filter((row) => Number(row.matches || 0) >= 20 && Number(row.runs || 0) >= 500);
  } else if (metricInfo.resolved_metric === 'economy') {
    rows = rows.filter((row) => Number(row.matches || 0) >= 20 && Number(row.wickets || 0) >= 25);
  } else if (metricInfo.resolved_metric === 'sixes' || metricInfo.resolved_metric === 'fours') {
    rows = rows.filter((row) => Number(row.matches || 0) >= 20 && Number(row.runs || 0) >= 1000);
  }
  if (!rows.length) {
    rows = allRows;
  }
  rows = rows.slice(0, Math.max(1, requestedLimit)).map((row, index) => ({
    ...row,
    rank: index + 1
  }));
  const answerRows = rows.slice(0, 3).map((row) => `#${row.rank} ${row.player} - ${formatStatValue(row.value)}`);
  return {
    answer: uniqueNonEmpty([
      answerRows.length
        ? `Top ${titleCaseMetric(metricInfo.requested_metric)} from the archived vector index: ${answerRows.join('; ')}.`
        : '',
      metricInfo.note
    ]).join(' '),
    data: {
      type: 'top_players',
      metric: metricInfo.requested_metric,
      resolved_metric: metricInfo.resolved_metric,
      rows,
      chartData: buildLeaderboardChartData(metricInfo.requested_metric, rows)
    },
    followups: ['Compare two players', 'Show player stats', 'Show live scores']
  };
}

async function runGlossary(route, question) {
  const rawTerm = pickFirst(route.term, question);
  const normalizedTerm = normalizeText(rawTerm);
  const glossaryEntry = Object.entries(GLOSSARY).find(([key]) => {
    const cleanKey = normalizeText(key.replace(/_/g, ' '));
    return normalizedTerm.includes(cleanKey);
  });
  const term = glossaryEntry?.[0] || rawTerm;
  const explanation = glossaryEntry?.[1] || 'I can explain common cricket terms like strike rate, economy, average, and head to head.';

  return {
    answer: explanation,
    data: {
      type: 'glossary',
      term,
      explanation
    },
    followups: ['Show top batters', 'Show player stats', 'Show live scores']
  };
}

async function runGeneralKnowledge(route, question) {
  const knowledge = lookupKnowledge(question, {
    subIntent: route.sub_intent || ''
  });
  const fallbackSubIntent = String(route.sub_intent || '').trim();
  const fallbackMessages = {
    points_table:
      'The current setup does not expose a live points table feed yet. I can still help with live scores, schedules, and recent results.',
    team_ranking:
      'Live ICC rankings are not available from the configured providers right now. I can still explain how rankings work or help with team stats.',
    pitch_report:
      'Live venue-specific pitch reports are not available from the configured providers right now. I can still explain common pitch behaviors and matchups.',
    injury_update:
      'Verified live injury updates are not available from the configured providers right now. I can still help with squad, playing XI, and recent match context.',
    general_cricket_fallback:
      'I can explain cricket rules, formats, history, records, equipment, and training basics.'
  };

  if (!knowledge.found) {
    const fallbackAnswer =
      fallbackMessages[fallbackSubIntent] ||
      'I do not have a verified local knowledge entry for that exact cricket topic yet, but I can help with rules, history, records, and training basics.';

    return {
      answer: fallbackAnswer,
      data: {
        type: 'general_knowledge',
        title: 'Cricket Knowledge',
        category: fallbackSubIntent || 'general_cricket_fallback',
        question: String(question || '').trim(),
        source_label: 'Local Knowledge',
        fallback_used: true
      },
      followups: ['What is LBW?', 'Who won WC 2011?', 'Difference between ODI and T20']
    };
  }

  const entry = knowledge.entry;
  return {
    answer: entry.answer,
    data: {
      type: 'general_knowledge',
      title: entry.title,
      category: entry.sub_intent || entry.category || 'general_cricket_fallback',
      question: String(question || '').trim(),
      source_label: 'Local Knowledge',
      related_topics: Array.isArray(entry.related_topics) ? entry.related_topics : [],
      examples: Array.isArray(entry.examples) ? entry.examples : [],
      confidence: Number(knowledge.score || 0)
    },
    followups: Array.isArray(entry.related_topics) && entry.related_topics.length
      ? entry.related_topics.slice(0, 3)
      : ['Show live scores', 'Show player stats', 'Show cricket records']
  };
}

async function runTeamInfo(route, question) {
  const vs = parseVsSides(question) || {};
  const multiWordPhrases = buildPhraseCandidates(question, { minWords: 2, maxWords: 4 });
  const { query: teamQuery, resolution } = await resolveEntityWithFallback('team', [
    route.team,
    route.team1,
    route.team2,
    vs.left,
    vs.right,
    ...multiWordPhrases,
    removeGenericWords(question)
  ]);
  if (resolution.status !== 'resolved') {
    return unresolvedEntityResult('team', teamQuery, resolution);
  }

  const team = resolution.item;
  const recentMatches = (await findMatchesForTeam(team.name, { limit: 5 })).map(toPublicMatch);
  const stats = toTeamStats(team);
  const [wikiSummary, wikiWikitext] = await Promise.all([
    fetchWikipediaSummary(team.name),
    fetchWikipediaWikitext(team.name)
  ]);
  const wikiFields = parseWikipediaInfoboxFields(wikiWikitext);
  const titleStats = buildTeamTitleStats(wikiFields);
  const titleTotal = titleStats.reduce((sum, entry) => sum + Number(entry.wins || 0), 0);
  const iplTitles = titleStats.find((entry) => /\bipl\b|indian premier league/i.test(entry.title));
  const answer =
    buildTeamInfoAnswerFromWiki(question, team.name, wikiSummary, wikiFields) ||
    `${team.name} have ${formatStatValue(stats.wins)} wins from ${formatStatValue(stats.matches)} archived matches and a win rate of ${formatStatValue(stats.win_rate)}%.`;

  return {
    answer,
    data: {
      type: 'team_info',
      title: team.name,
      question: String(question || '').trim(),
      team: {
        id: team.id,
        name: team.name,
        image_url: String(wikiSummary?.image || '').trim(),
        wikipedia_url: String(wikiSummary?.wikipedia_url || '').trim(),
        short_description: String(wikiSummary?.description || '').trim(),
        description: String(wikiSummary?.extract || wikiSummary?.description || '').trim(),
        captain: String(wikiFields.captain || '').trim(),
        coach: String(wikiFields.coach || '').trim(),
        owner: String(wikiFields.owner || '').trim(),
        home_ground: String(wikiFields.ground || '').trim(),
        founded: String(wikiFields.founded || '').trim()
      },
      stats: {
        ...stats,
        major_titles: titleTotal || 0,
        ipl_titles: Number(iplTitles?.wins || 0) || 0
      },
      recent_matches: recentMatches
    },
    followups: ['Show team head to head', 'Show recent live scores', 'Show upcoming matches']
  };
}

async function runTeamSquad(route, question, { playingXi = false } = {}) {
  const vs = parseVsSides(question) || {};
  const multiWordPhrases = buildPhraseCandidates(question, { minWords: 2, maxWords: 4 });
  const { query: teamQuery, resolution } = await resolveEntityWithFallback('team', [
    route.team,
    route.team1,
    route.team2,
    vs.left,
    vs.right,
    ...multiWordPhrases,
    removeGenericWords(question)
  ]);
  if (resolution.status !== 'resolved') {
    return unresolvedEntityResult('team', teamQuery, resolution);
  }

  const team = resolution.item;
  const stats = toTeamStats(team);
  const [wikiSummary, wikiWikitext, squadResult] = await Promise.all([
    fetchWikipediaSummary(team.name),
    fetchWikipediaWikitext(team.name),
    buildSquadPlayersForTeam(team.name, { limit: playingXi ? 11 : 0 })
  ]);
  const wikiFields = parseWikipediaInfoboxFields(wikiWikitext);
  const squadPlayers = Array.isArray(squadResult.players) ? squadResult.players : [];
  const totalPlayers = Number(squadResult.totalPlayers || squadPlayers.length || 0);

  if (!squadPlayers.length) {
    return unavailableResult(`I could not find a verified squad list for ${team.name} in the current vector archive.`);
  }

  return {
    answer: playingXi
      ? `${team.name} playing XI proxy from the archived roster: ${squadPlayers.map((player) => player.name).join(', ')}.`
      : `${team.name} squad from the archived vector index includes ${formatStatValue(totalPlayers)} listed players.`,
    data: {
      type: playingXi ? 'playing_xi' : 'team_squad',
      title: team.name,
      subtitle: playingXi ? 'Playing XI' : 'Squad',
      question: String(question || '').trim(),
      team: {
        id: team.id,
        name: team.name,
        image_url: String(wikiSummary?.image || '').trim(),
        wikipedia_url: String(wikiSummary?.wikipedia_url || '').trim(),
        short_description: String(wikiSummary?.description || '').trim(),
        description: String(wikiSummary?.extract || wikiSummary?.description || '').trim(),
        captain: String(wikiFields.captain || '').trim(),
        coach: String(wikiFields.coach || '').trim(),
        home_ground: String(wikiFields.ground || '').trim()
      },
      captain: String(wikiFields.captain || '').trim(),
      coach: String(wikiFields.coach || '').trim(),
      stats,
      players: playingXi ? squadPlayers.map((player) => player.name) : squadPlayers,
      total_players: playingXi ? squadPlayers.length : totalPlayers
    },
    followups: ['Show team info', 'Show recent matches', 'Compare two teams']
  };
}

async function runRecordLookup(route, question) {
  const knownRecord = lookupKnownRecord(question);
  if (knownRecord) {
    let imageUrl = '';
    if (knownRecord.player_name) {
      const profile = await getPlayerProfile({
        query: knownRecord.player_name,
        datasetName: knownRecord.player_name
      });
      imageUrl = String(profile?.image_url || '').trim();
    }

    return {
      answer: knownRecord.answer,
      data: {
        type: 'record_lookup',
        title: knownRecord.title,
        question: String(question || '').trim(),
        metric: knownRecord.metric,
        resolved_metric: knownRecord.resolved_metric,
        image_url: imageUrl,
        stats: knownRecord.stats || {},
        rows: []
      },
      followups: ['Show player stats', 'Compare two players', 'Show live scores']
    };
  }

  const metricInfo = resolveLeaderboardMetric(pickFirst(route.metric), question);
  let rows = [];
  if (['runs', 'wickets', 'strike_rate', 'economy', 'sixes', 'fours'].includes(metricInfo.resolved_metric)) {
    rows = await getTopPlayersByMetric(metricInfo.resolved_metric, { limit: 10 });
  }

  return {
    answer: '',
    data: {
      type: 'record_lookup',
      title: 'Cricket Record',
      question: String(question || '').trim(),
      metric: metricInfo.requested_metric,
      resolved_metric: metricInfo.resolved_metric,
      rows
    },
    followups: ['Show player stats', 'Compare two players', 'Show live scores']
  };
}

function isResolved(resolution = {}) {
  return String(resolution.status || '') === 'resolved';
}

async function refineRouteForQuestion(route = {}, question = '') {
  const normalizedQuestion = normalizeText(question);
  const vs = parseVsSides(question);
  const season = toSeason(pickFirst(route.season, guessSeason(question)));
  const format = pickFirst(route.format, guessFormat(question));

  if (vs?.left && vs?.right) {
    const [leftPlayer, rightPlayer, leftTeam, rightTeam] = await Promise.all([
      resolveEntityWithFallback('player', [route.player1, route.player, vs.left], { question }),
      resolveEntityWithFallback('player', [route.player2, route.player, vs.right], { question }),
      resolveEntityWithFallback('team', [route.team1, route.team, vs.left]),
      resolveEntityWithFallback('team', [route.team2, route.team, vs.right])
    ]);

    const playersResolved = isResolved(leftPlayer.resolution) && isResolved(rightPlayer.resolution);
    const teamsResolved = isResolved(leftTeam.resolution) && isResolved(rightTeam.resolution);
    const playerScore =
      Number(leftPlayer.resolution?.score || 0) + Number(rightPlayer.resolution?.score || 0);
    const teamScore =
      Number(leftTeam.resolution?.score || 0) + Number(rightTeam.resolution?.score || 0);
    const rawLeft = normalizeText(vs.left || '');
    const rawRight = normalizeText(vs.right || '');
    const exactTeamSurface =
      teamsResolved &&
      normalizeText(leftTeam.resolution.item?.name || '') === rawLeft &&
      normalizeText(rightTeam.resolution.item?.name || '') === rawRight;

    if (teamsResolved && (!playersResolved || exactTeamSurface || teamScore >= playerScore + 0.15)) {
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

    if (playersResolved && (!teamsResolved || playerScore > teamScore)) {
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
    const [playerLookup, teamLookup] = await Promise.all([
      resolveEntityWithFallback('player', [route.player, removeGenericWords(question), question], { question }),
      resolveEntityWithFallback('team', [route.team, removeGenericWords(question), question])
    ]);
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
    archive_recent_matches: Array.isArray(context.archive_recent_matches)
      ? context.archive_recent_matches.slice(0, 4).map(slimMatch)
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

function compactCricbuzzPlayerForData(player = {}) {
  return {
    id: String(player.id || '').trim(),
    name: String(player.name || '').trim(),
    team: String(player.team || '').trim(),
    country: String(player.country || '').trim(),
    role: String(player.role || '').trim(),
    batting_style: String(player.batting_style || '').trim(),
    bowling_style: String(player.bowling_style || '').trim(),
    bio: String(player.bio || '').trim(),
    stats: player.stats && typeof player.stats === 'object' ? player.stats : {}
  };
}

function compactCricbuzzContext(context = {}) {
  return {
    provider: 'cricbuzz',
    available: Boolean(context.available),
    errors: Array.isArray(context.errors) ? context.errors.slice(0, 3) : [],
    player_searches: Array.isArray(context.player_searches)
      ? context.player_searches.slice(0, 2).map((item) => ({
          query: item.query,
          items: Array.isArray(item.items) ? item.items.slice(0, 3).map(compactCricbuzzPlayerForData) : []
        }))
      : [],
    players: Array.isArray(context.players)
      ? context.players.slice(0, 2).map(compactCricbuzzPlayerForData)
      : []
  };
}

function compactEspnContext(context = {}) {
  return {
    provider: 'espn',
    available: Boolean(context.available),
    errors: Array.isArray(context.errors) ? context.errors.slice(0, 3) : [],
    player_searches: Array.isArray(context.player_searches)
      ? context.player_searches.slice(0, 2).map((item) => ({
          query: item.query,
          items: Array.isArray(item.items) ? item.items.slice(0, 3) : []
        }))
      : [],
    player:
      context.player && typeof context.player === 'object'
        ? {
            id: String(context.player.id || '').trim(),
            name: String(context.player.name || '').trim(),
            full_name: String(context.player.full_name || '').trim(),
            team: String(context.player.team || '').trim(),
            role: String(context.player.role || '').trim(),
            batting_style: String(context.player.batting_style || '').trim(),
            bowling_style: String(context.player.bowling_style || '').trim(),
            career_summary:
              context.player.career_summary && typeof context.player.career_summary === 'object'
                ? context.player.career_summary
                : {},
            stats_by_format:
              context.player.stats_by_format && typeof context.player.stats_by_format === 'object'
                ? context.player.stats_by_format
                : {}
          }
        : null
  };
}

function buildMergedApiContext({
  question = '',
  route = {},
  structuredContext = null,
  vectorContext = {},
  cricApiContext = {},
  cricbuzzContext = {},
  espnContext = {}
} = {}) {
  const requestedSources = resolveRequestedDataSources(route, question);
  const merged = {
    user_question: String(question || '').trim(),
    route: {
      action: String(route.action || 'not_supported').trim() || 'not_supported',
      data_sources: requestedSources
    },
    source_status: {
      vector_db: {
        requested: requestedSources.includes(DATA_SOURCE.VECTOR_DB),
        available: Boolean(vectorContext?.available)
      },
      local_knowledge: {
        requested: requestedSources.includes(DATA_SOURCE.LOCAL_KNOWLEDGE),
        available: String(structuredContext?.result?.data?.type || '').trim() === 'general_knowledge'
      },
      cricapi: {
        requested: requestedSources.includes(DATA_SOURCE.CRICAPI_LIVE),
        available: Boolean(cricApiContext?.available)
      },
      cricbuzz: {
        requested: requestedSources.includes(DATA_SOURCE.CRICBUZZ_STATS),
        available: Boolean(cricbuzzContext?.available)
      },
      espn: {
        requested: ['player_stats', 'player_season_stats', 'compare_players'].includes(
          String(route.action || '').trim()
        ),
        available: Boolean(espnContext?.available)
      }
    },
    vector_db:
      requestedSources.includes(DATA_SOURCE.VECTOR_DB) ||
      Boolean(vectorContext?.available) ||
      Boolean(Array.isArray(vectorContext?.results) && vectorContext.results.length)
        ? compactVectorContext(vectorContext)
        : null,
    local_knowledge:
      requestedSources.includes(DATA_SOURCE.LOCAL_KNOWLEDGE) &&
      String(structuredContext?.result?.data?.type || '').trim() === 'general_knowledge'
        ? {
            available: true,
            category: String(structuredContext?.result?.data?.category || '').trim(),
            title: String(structuredContext?.result?.data?.title || '').trim(),
            answer: String(structuredContext?.result?.answer || '').trim()
          }
        : null,
    cricapi:
      requestedSources.includes(DATA_SOURCE.CRICAPI_LIVE) || Boolean(cricApiContext?.available)
        ? compactCricApiContext(cricApiContext)
        : null,
    cricbuzz:
      requestedSources.includes(DATA_SOURCE.CRICBUZZ_STATS) || Boolean(cricbuzzContext?.available)
        ? compactCricbuzzContext(cricbuzzContext)
        : null,
    espn:
      ['player_stats', 'player_season_stats', 'compare_players'].includes(
        String(route.action || '').trim()
      ) || Boolean(espnContext?.available)
        ? compactEspnContext(espnContext)
        : null
  };

  if (structuredContext) {
    const structuredSummary = compactStructuredResult(structuredContext);
    if (structuredSummary.available || structuredSummary.data || structuredSummary.answer) {
      merged.structured_context = structuredSummary;
    }
  }

  return merged;
}

function buildMergedEvidenceContext({
  question = '',
  route = {},
  vectorContext = {},
  livePayload = null,
  cricbuzzContext = {},
  espnContext = {}
} = {}) {
  const requestedSources = resolveRequestedDataSources(route, question);
  const sections = [
    `Question: ${String(question || '').trim()}`,
    `Primary Action: ${String(route.action || 'not_supported').trim() || 'not_supported'}`,
    `Requested Data Sources: ${requestedSources.join(', ') || 'NONE'}`
  ];

  if (requestedSources.includes(DATA_SOURCE.OPENAI_FALLBACK)) {
    sections.push(
      [
        'OPENAI_FALLBACK_STATUS:',
        'This query needs general cricket reasoning or conversational synthesis. Answer directly from the supplied evidence, and if no specialist evidence is present, respond from broad cricket knowledge without inventing unavailable live statistics.'
      ].join('\n')
    );
  }

  if (requestedSources.includes(DATA_SOURCE.LOCAL_KNOWLEDGE)) {
    sections.push(
      [
        'LOCAL_KNOWLEDGE_STATUS:',
        'Use the deterministic local cricket knowledge layer for rules, terminology, history, world cup winners, equipment, and training topics. Do not invent live statistics when only local knowledge is available.'
      ].join('\n')
    );
  }

  if (requestedSources.includes(DATA_SOURCE.CRICAPI_LIVE)) {
    if (livePayload) {
      sections.push(`LIVE_API_JSON:\n${JSON.stringify(compactCricApiContext(livePayload), null, 2)}`);
      if (!livePayload.available) {
        sections.push(
          [
            'LIVE_API_STATUS:',
            'Live data is currently unavailable. Answer any historical parts from VECTOR_DB only, and explicitly say the live portion could not be fetched right now.'
          ].join('\n')
        );
      }
    } else {
      sections.push(
        [
          'LIVE_API_JSON:',
          'null',
          'LIVE_API_STATUS:',
          'Live data is currently unavailable. Answer any historical parts from VECTOR_DB only, and explicitly say the live portion could not be fetched right now.'
        ].join('\n')
      );
    }
  }

  if (requestedSources.includes(DATA_SOURCE.CRICBUZZ_STATS)) {
    sections.push(
      `CRICBUZZ_STATS_JSON:\n${JSON.stringify(compactCricbuzzContext(cricbuzzContext), null, 2)}`
    );
    if (!cricbuzzContext?.available) {
      sections.push(
        [
          'CRICBUZZ_STATUS:',
          'Granular Cricbuzz player statistics are currently unavailable. Use the remaining sources and state clearly when deep player stats could not be fetched.'
        ].join('\n')
      );
    }
  }

  if (espnContext?.available || ['player_stats', 'player_season_stats', 'compare_players'].includes(String(route.action || '').trim())) {
    sections.push(`ESPN_STATS_JSON:\n${JSON.stringify(compactEspnContext(espnContext), null, 2)}`);
    if (!espnContext?.available) {
      sections.push(
        [
          'ESPN_STATUS:',
          'ESPN fallback career data is unavailable. Use the remaining evidence and say clearly when career enrichments could not be fetched.'
        ].join('\n')
      );
    }
  }

  if (requestedSources.includes(DATA_SOURCE.VECTOR_DB)) {
    const vectorChunks = Array.isArray(vectorContext.results)
      ? vectorContext.results.slice(0, 5).map((row, index) =>
          [
            `Chunk ${index + 1}:`,
            `Metadata: ${JSON.stringify(row.metadata && typeof row.metadata === 'object' ? row.metadata : {}, null, 2)}`,
            `Text: ${String(row.document_preview || row.document || '').trim() || 'No preview available.'}`
          ].join('\n')
        )
      : [];

    sections.push(
      [
        'VECTOR_DB_CHUNKS:',
        vectorChunks.length ? vectorChunks.join('\n\n') : 'No relevant archive chunks found.'
      ].join('\n')
    );
  }

  return sections.join('\n\n').trim();
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

function buildPublicDetails(
  question = '',
  route = {},
  structuredContext = {},
  vectorContext = {},
  cricApiContext = {},
  synthesized = {},
  espnContext = {}
) {
  const data = structuredContext?.result?.data && typeof structuredContext.result.data === 'object'
    ? structuredContext.result.data
    : {};
  const type = String(data.type || '').trim();
  const fallbackSummary = String(synthesized?.answer || structuredContext?.result?.answer || '').trim();
  const responseSources = Array.isArray(synthesized?.sources) ? synthesized.sources : [];
  const detectedEntities = normalizeDetectedEntities(synthesized?.detected_entities).length
    ? normalizeDetectedEntities(synthesized?.detected_entities)
    : extractDetectedEntitiesFallback(route, data, fallbackSummary, cricApiContext);
  const normalizedQuestion = normalizeText(question);
  const providerStatus = buildProviderStatus(cricApiContext?.errors || []);
  const responseMeta = {
    intent: String(route.action || '').trim(),
    sub_intent: String(route.sub_intent || '').trim(),
    answer_mode: String(route.answer_mode || '').trim(),
    time_context: String(route.time_context || '').trim(),
    confidence: Number.isFinite(Number(route.confidence)) ? Number(route.confidence) : undefined
  };
  const withDetectedEntities = (payload = {}) =>
    pruneEmptyFields({
      ...payload,
      ...responseMeta,
      ...(detectedEntities.length ? { detected_entities: detectedEntities } : {})
    });

  if (type === 'chat') {
    return withDetectedEntities({
      type,
      title: 'Cricket Intelligence',
      subtitle: 'Ready when you are',
      summary: fallbackSummary,
      message: fallbackSummary
    });
  }
  if (type === 'subjective_analysis') {
    return withDetectedEntities({
      type,
      title: 'Analyst View',
      subtitle: 'Data-driven perspective',
      summary: fallbackSummary,
      question: String(data.question || question || '').trim()
    });
  }
  if (type === 'player_stats') {
    const player = mergeEspnCareerIntoPlayer(data.player || {}, espnContext?.player || null);
    return withDetectedEntities({
      type,
      title: String(player.name || 'Player Snapshot'),
      subtitle: String(player.team || player.country || ''),
      summary: fallbackSummary,
      player,
      stats: hasMeaningfulStats(data.stats || {}) ? data.stats || {} : player.stats || {},
      chartData: data.chartData && typeof data.chartData === 'object' ? data.chartData : null,
      recent_matches: Array.isArray(data.recent_matches) ? data.recent_matches.slice(0, 5).map(slimMatch) : [],
      sources: responseSources
    });
  }
  if (type === 'team_stats') {
    const team = data.team || {};
    return withDetectedEntities({
      type,
      title: String(team.name || 'Team Snapshot'),
      subtitle: scopeSubtitle(route),
      summary: fallbackSummary,
      team,
      stats: data.stats || {},
      recent_matches: Array.isArray(data.stats?.recent_matches) ? data.stats.recent_matches.slice(0, 5).map(slimMatch) : [],
      sources: responseSources
    });
  }
  if (type === 'team_info') {
    const team = data.team || {};
    return withDetectedEntities({
      type,
      title: String(team.name || 'Team Information'),
      subtitle: 'Team information',
      summary: fallbackSummary,
      question: String(data.question || question || '').trim(),
      team,
      stats: data.stats || {},
      recent_matches: Array.isArray(data.recent_matches) ? data.recent_matches.slice(0, 5).map(slimMatch) : [],
      sources: responseSources
    });
  }
  if (type === 'team_squad' || type === 'playing_xi') {
    const team = data.team || {};
    return withDetectedEntities({
      type,
      title: String(team.name || 'Team Squad'),
      subtitle: String(data.subtitle || (type === 'playing_xi' ? 'Playing XI' : 'Squad')).trim(),
      summary: fallbackSummary,
      team,
      captain: String(data.captain || team.captain || '').trim(),
      coach: String(data.coach || team.coach || '').trim(),
      stats: data.stats || {},
      players: Array.isArray(data.players) ? data.players : [],
      total_players: Number(data.total_players || 0),
      sources: responseSources
    });
  }
  if (type === 'match_summary') {
    const match = slimMatch(data.match || {});
    return withDetectedEntities({
      type,
      title: String(match.name || 'Match Summary'),
      subtitle: [match.date, match.venue].filter(Boolean).join(' | '),
      summary: String(match.summary || fallbackSummary),
      match,
      sources: responseSources
    });
  }
  if (type === 'compare_players') {
    const left = data.left || {};
    const right = data.right || {};
    return withDetectedEntities({
      type,
      title: `${left.name || 'Player 1'} vs ${right.name || 'Player 2'}`,
      subtitle: scopeSubtitle(route),
      summary: fallbackSummary,
      left,
      right,
      chartData: data.chartData && typeof data.chartData === 'object' ? data.chartData : null,
      sources: responseSources
    });
  }
  if (type === 'head_to_head') {
    return withDetectedEntities({
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
      recent_matches: Array.isArray(data.stats?.recent_matches) ? data.stats.recent_matches.slice(0, 5).map(slimMatch) : [],
      sources: responseSources
    });
  }
  if (type === 'top_players') {
    const metric = String(data.metric || '');
    return withDetectedEntities({
      type,
      title: `Top ${titleCaseMetric(metric)}`,
      subtitle: scopeSubtitle(route),
      summary: fallbackSummary,
      metric,
      rows: rankedRows(Array.isArray(data.rows) ? data.rows.slice(0, 10) : []),
      chartData: data.chartData && typeof data.chartData === 'object' ? data.chartData : null,
      sources: responseSources
    });
  }
  if (type === 'record_lookup') {
    const metric = String(data.metric || data.resolved_metric || '');
    return withDetectedEntities({
      type,
      title: String(data.title || (metric ? `Cricket Record: ${titleCaseMetric(metric)}` : 'Cricket Record')),
      subtitle: 'Record lookup',
      summary: fallbackSummary,
      image_url: String(data.image_url || '').trim(),
      question: String(data.question || question || '').trim(),
      metric,
      resolved_metric: String(data.resolved_metric || '').trim(),
      stats: data.stats && typeof data.stats === 'object' ? data.stats : {},
      rows: rankedRows(Array.isArray(data.rows) ? data.rows.slice(0, 10) : []),
      sources: responseSources
    });
  }
  if (type === 'glossary') {
    return withDetectedEntities({
      type,
      title: titleCaseMetric(String(data.term || 'Glossary')),
      subtitle: 'Cricket term',
      summary: String(structuredContext?.result?.answer || fallbackSummary),
      term: String(data.term || ''),
      explanation: String(structuredContext?.result?.answer || ''),
      sources: responseSources
    });
  }
  if (type === 'general_knowledge') {
    return withDetectedEntities({
      type,
      title: String(data.title || 'Cricket Knowledge').trim() || 'Cricket Knowledge',
      subtitle: String(data.category || 'Cricket knowledge').trim(),
      summary: String(structuredContext?.result?.answer || fallbackSummary),
      question: String(data.question || question || '').trim(),
      category: String(data.category || '').trim(),
      related_topics: Array.isArray(data.related_topics) ? data.related_topics.slice(0, 5) : [],
      examples: Array.isArray(data.examples) ? data.examples.slice(0, 3) : [],
      fallback_used: Boolean(data.fallback_used),
      sources: responseSources
    });
  }

  const liveMatch = Array.isArray(cricApiContext.live_scores) ? cricApiContext.live_scores[0] : null;
  const nextMatch = Array.isArray(cricApiContext.schedule) ? cricApiContext.schedule[0] : null;
  const archiveRecentMatches = Array.isArray(cricApiContext.archive_recent_matches)
    ? cricApiContext.archive_recent_matches
    : [];
  const playerProfile = Array.isArray(cricApiContext.player_profiles) ? cricApiContext.player_profiles[0] : null;
  const shouldRenderLiveSurface =
    LIVE_QUERY_REGEX.test(normalizedQuestion) ||
    SCHEDULE_QUERY_REGEX.test(normalizedQuestion) ||
    Boolean(liveMatch || nextMatch || playerProfile || archiveRecentMatches.length);
  if (shouldRenderLiveSurface) {
    const upcomingMatches = Array.isArray(cricApiContext.schedule)
      ? cricApiContext.schedule.slice(0, 4).map(slimMatch)
      : [];
    const recentMatches = Array.isArray(cricApiContext.live_scores) && cricApiContext.live_scores.length
      ? cricApiContext.live_scores.slice(0, 4).map(slimMatch)
      : archiveRecentMatches.slice(0, 4).map(slimMatch);
    return withDetectedEntities({
      type: 'live_update',
      title: upcomingMatches.length ? 'Upcoming Matches' : recentMatches.length ? 'Live Match Center' : 'Match Center',
      subtitle: scopeSubtitle(route),
      summary: fallbackSummary,
      live_match: slimMatch(liveMatch || {}),
      next_match: slimMatch(nextMatch || {}),
      upcoming_matches: upcomingMatches,
      recent_matches: recentMatches,
      player: slimPlayerProfile(playerProfile || {}),
      provider_status: providerStatus,
      sources: responseSources
    });
  }

  return withDetectedEntities({
    type: 'summary',
    title: 'Cricket Intelligence',
    subtitle: '',
    summary: fallbackSummary,
    sources: responseSources
  });
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

async function safeCricbuzzCall(loader) {
  try {
    return { ok: true, value: await loader() };
  } catch (error) {
    const friendlyError = error?.details?.subscription_required
      ? 'Cricbuzz stats are unavailable because the current RapidAPI subscription does not include this endpoint.'
      : error?.message || 'Cricbuzz request failed.';
    return {
      ok: false,
      error: friendlyError,
      config_error: error instanceof CricbuzzApiConfigError || error?.name === 'CricbuzzApiConfigError'
    };
  }
}

async function safeEspnCall(loader) {
  try {
    return { ok: true, value: await loader() };
  } catch (error) {
    return {
      ok: false,
      error: error?.message || 'ESPN request failed.',
      config_error: error instanceof EspnServiceError || error?.name === 'EspnServiceError'
    };
  }
}

function shouldUseEspnFallback(route = {}, structuredContext = {}, cricApiContext = {}) {
  const action = String(route.action || '').trim();
  if (!['player_stats', 'player_season_stats', 'compare_players'].includes(action)) {
    return false;
  }

  const structuredPlayers = [
    structuredContext?.result?.data?.player,
    structuredContext?.result?.data?.left,
    structuredContext?.result?.data?.right
  ].filter((item) => item && typeof item === 'object');
  if (structuredPlayers.some((player) => !hasMeaningfulStats(toPlayerStats(player)))) {
    return true;
  }

  const liveProfiles = Array.isArray(cricApiContext?.player_profiles) ? cricApiContext.player_profiles : [];
  return !liveProfiles.length;
}

async function buildEspnContext(route = {}, question = '', structuredContext = {}, cricApiContext = {}) {
  const context = createEmptyEspnContext();
  if (!shouldUseEspnFallback(route, structuredContext, cricApiContext)) {
    return context;
  }

  const playerQueries = uniqueNonEmpty([
    route.player,
    route.player1,
    route.player2,
    structuredContext?.result?.data?.player?.name,
    structuredContext?.result?.data?.left?.name,
    structuredContext?.result?.data?.right?.name,
    removeGenericWords(question)
  ]).slice(0, 2);

  if (!playerQueries.length) {
    return context;
  }

  const searchResults = await Promise.all(
    playerQueries.map(async (query) => ({
      query,
      result: await safeEspnCall(() => getPlayerCareerByQuery(query, { limit: 3 }))
    }))
  );

  for (const item of searchResults) {
    if (!item.result.ok) {
      context.errors.push(item.result.error);
      continue;
    }
    context.player_searches.push({
      query: item.query,
      items: [
        {
          id: String(item.result.value?.player?.id || '').trim(),
          name: String(item.result.value?.player?.name || '').trim(),
          team: String(item.result.value?.player?.team || '').trim()
        }
      ].filter((entry) => entry.id && entry.name)
    });
    if (!context.player && item.result.value?.player) {
      context.player = item.result.value.player;
    }
  }

  context.errors = uniqueNonEmpty(context.errors);
  context.available = Boolean(context.player);
  return context;
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
    archive_recent_matches: [],
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

  if (teamHint) {
    context.archive_recent_matches = (await findMatchesForTeam(teamHint, { limit: 4, format: formatHint }))
      .map(toPublicMatch);
  }

  if (
    !context.player_searches.length &&
      !context.player_profiles.length &&
      !context.live_scores.length &&
      !context.archive_recent_matches.length &&
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
      context.archive_recent_matches.length ||
      context.schedule.length ||
      context.series.length ||
      context.series_info
  );
  return context;
}

async function buildCricbuzzContext(route, question) {
  const { playerHints } = deriveEntityHints(question, route);
  const context = createEmptyCricbuzzContext();

  if (!playerHints.length) {
    return context;
  }

  const searchResults = await Promise.all(
    playerHints.slice(0, 2).map(async (hint) => ({
      query: hint,
      result: await safeCricbuzzCall(() => searchCricbuzzPlayers({ q: hint, limit: 3 }))
    }))
  );

  for (const item of searchResults) {
    if (!item.result.ok) {
      context.errors.push(item.result.error);
      continue;
    }
    context.player_searches.push({
      query: item.query,
      items: Array.isArray(item.result.value?.items) ? item.result.value.items : []
    });
  }

  const playerCards = await Promise.all(
    context.player_searches
      .map((row) => row.items?.[0]?.name)
      .filter(Boolean)
      .slice(0, 2)
      .map(async (name) => safeCricbuzzCall(() => getCricbuzzPlayerCardByName(name)))
  );

  for (const card of playerCards) {
    if (!card.ok) {
      context.errors.push(card.error);
      continue;
    }
    if (card.value?.player) {
      context.players.push(card.value.player);
    }
  }

  context.errors = uniqueNonEmpty(context.errors);
  context.available = Boolean(context.player_searches.length || context.players.length);
  return context;
}

async function executeRouteDataSources(route, question, { onStatus, structuredContext = null } = {}) {
  const requestedSources = resolveRequestedDataSources(route, question);
  let vectorContext = createEmptyVectorContext();
  let livePayload = null;
  let cricbuzzContext = createEmptyCricbuzzContext();
  let espnContext = createEmptyEspnContext();
  let liveFailureMessage = '';
  let cricbuzzFailureMessage = '';

  if (!requestedSources.length) {
    const cricApiContext = createEmptyCricApiContext();
    return {
      requestedSources,
      vectorContext,
      cricApiContext,
      cricbuzzContext,
      espnContext,
      livePayload,
      mergedApiContext: buildMergedApiContext({
        question,
        route,
        vectorContext,
        cricApiContext,
        cricbuzzContext,
        espnContext
      }),
      mergedEvidenceContext: ''
    };
  }

  if (requestedSources.includes(DATA_SOURCE.VECTOR_DB)) {
    emitStatus(onStatus, {
      stage: 'search',
      action: route.action,
      message: actionStatusMessage(route.action)
    });
  }
  if (requestedSources.includes(DATA_SOURCE.LOCAL_KNOWLEDGE)) {
    emitStatus(onStatus, {
      stage: 'knowledge',
      action: route.action,
      message: actionStatusMessage(route.action)
    });
  }
  if (requestedSources.includes(DATA_SOURCE.CRICAPI_LIVE)) {
    emitStatus(onStatus, {
      stage: 'live',
      message: 'Checking latest match data.'
    });
  }
  if (requestedSources.includes(DATA_SOURCE.CRICBUZZ_STATS)) {
    emitStatus(onStatus, {
      stage: 'expert_stats',
      message: 'Collecting Cricbuzz player intelligence.'
    });
  }
  if (['player_stats', 'player_season_stats', 'compare_players'].includes(String(route.action || '').trim())) {
    emitStatus(onStatus, {
      stage: 'career_fallback',
      message: 'Checking ESPN fallback career data.'
    });
  }

  const vectorPromise = requestedSources.includes(DATA_SOURCE.VECTOR_DB)
    ? queryVectorDb(question, { k: 5 })
    : Promise.resolve(vectorContext);
  const livePromise = requestedSources.includes(DATA_SOURCE.CRICAPI_LIVE)
    ? buildCricApiContext(route, question)
    : Promise.resolve(null);
  const cricbuzzPromise = requestedSources.includes(DATA_SOURCE.CRICBUZZ_STATS)
    ? buildCricbuzzContext(route, question)
    : Promise.resolve(cricbuzzContext);
  const espnPromise = ['player_stats', 'player_season_stats', 'compare_players'].includes(
    String(route.action || '').trim()
  )
    ? buildEspnContext(route, question, structuredContext, livePayload || {})
    : Promise.resolve(espnContext);

  const [vectorResult, liveResult, cricbuzzResult, espnResult] = await Promise.allSettled([
    vectorPromise,
    livePromise,
    cricbuzzPromise,
    espnPromise
  ]);

  vectorContext =
    vectorResult.status === 'fulfilled'
      ? vectorResult.value
      : createEmptyVectorContext(vectorResult.reason?.message || 'vector_query_failed');

  if (liveResult.status === 'fulfilled') {
    livePayload = liveResult.value;
  } else {
    liveFailureMessage = liveResult.reason?.message || 'Live data is currently unavailable.';
    livePayload = null;
  }

  if (cricbuzzResult.status === 'fulfilled') {
    cricbuzzContext = cricbuzzResult.value;
  } else {
    cricbuzzFailureMessage =
      cricbuzzResult.reason?.message || 'Cricbuzz player statistics are currently unavailable.';
    cricbuzzContext = createEmptyCricbuzzContext([cricbuzzFailureMessage]);
  }

  const cricApiContext = livePayload || createEmptyCricApiContext([liveFailureMessage]);
  if (!cricbuzzContext.available && cricbuzzFailureMessage) {
    cricbuzzContext = createEmptyCricbuzzContext([cricbuzzFailureMessage]);
  }

  if (espnResult.status === 'fulfilled') {
    espnContext = espnResult.value;
  } else {
    espnContext = createEmptyEspnContext([
      espnResult.reason?.message || 'ESPN fallback unavailable.'
    ]);
  }

  const finalMergedApiContext = buildMergedApiContext({
    question,
    route,
    structuredContext,
    vectorContext,
    cricApiContext,
    cricbuzzContext,
    espnContext
  });

  return {
    requestedSources,
    vectorContext,
    cricApiContext,
    cricbuzzContext,
    espnContext,
    livePayload,
    mergedApiContext: finalMergedApiContext,
    // This merged block is fed to the synthesis model for compound questions.
    mergedEvidenceContext: buildMergedEvidenceContext({
      question,
      route: {
        ...route,
        data_sources: requestedSources
      },
      vectorContext,
      livePayload,
      cricbuzzContext,
      espnContext
    })
  };
}

async function buildStructuredContext(route, question) {
  const effectiveRoute = await refineRouteForQuestion(route, question);
  if (isGeneralConversationAction(effectiveRoute.action)) {
    return {
      cache_ready: false,
      available: false,
      result: {
        answer: '',
        data: { type: 'chat' },
        followups: ['Show live scores', 'Show player stats', 'Upcoming matches']
      },
      route: effectiveRoute
    };
  }
  if (effectiveRoute.action === 'general_knowledge') {
    const result = await runGeneralKnowledge(effectiveRoute, question);
    return {
      cache_ready: true,
      available: Boolean(String(result?.answer || '').trim()),
      result,
      route: effectiveRoute
    };
  }
  if (effectiveRoute.action === 'subjective_analysis') {
    const shortEntityHints = uniqueNonEmpty(
      (String(question || '').match(/\b[A-Za-z]{2,5}\b/g) || []).map((token) => token.trim())
    );
    const [playerLookup, teamLookup] = await Promise.all([
      resolveEntityWithFallback('player', [route.player, route.player1, route.player2, removeGenericWords(question)], { question }),
      resolveEntityWithFallback('team', [route.team, route.team1, route.team2, ...shortEntityHints, ...buildPhraseCandidates(question), removeGenericWords(question)])
    ]);
    const subject =
      teamLookup.resolution?.status === 'resolved'
        ? String(teamLookup.resolution.item?.name || '').trim()
        : playerLookup.resolution?.status === 'resolved'
          ? String(playerLookup.resolution.item?.name || '').trim()
          : '';
    return {
      cache_ready: true,
      available: false,
      result: {
        answer: '',
        data: {
          type: 'subjective_analysis',
          question: String(question || '').trim(),
          subject
        },
        followups: ['Compare two players', 'Show team head to head', 'Show recent live scores']
      },
      route: effectiveRoute
    };
  }
  if (effectiveRoute.action === 'team_info') {
    const result = await runTeamInfo(effectiveRoute, question);
    return {
      cache_ready: true,
      available: Boolean(result?.data?.team?.name),
      result,
      route: effectiveRoute
    };
  }
  if (effectiveRoute.action === 'team_squad' || effectiveRoute.action === 'playing_xi') {
    const result = await runTeamSquad(effectiveRoute, question, {
      playingXi: effectiveRoute.action === 'playing_xi'
    });
    return {
      cache_ready: true,
      available: Boolean(Array.isArray(result?.data?.players) && result.data.players.length),
      result,
      route: effectiveRoute
    };
  }
  if (effectiveRoute.action === 'record_lookup') {
    const result = await runRecordLookup(effectiveRoute, question);
    return {
      cache_ready: true,
      available: Boolean(Array.isArray(result?.data?.rows) && result.data.rows.length),
      result,
      route: effectiveRoute
    };
  }
  if (effectiveRoute.action === 'live_update') {
    return {
      cache_ready: true,
      available: true,
      result: {
        answer: '',
        data: { type: 'live_update' },
        followups: []
      },
      route: effectiveRoute
    };
  }

  let result = unavailableResult();
  if (effectiveRoute.action === 'player_stats' || effectiveRoute.action === 'player_season_stats') {
    result = await runPlayerAction(effectiveRoute.action, effectiveRoute, question);
  } else if (effectiveRoute.action === 'team_stats') {
    result = await runTeamStats(effectiveRoute, question);
  } else if (effectiveRoute.action === 'match_summary') {
    result = await runMatchSummary(effectiveRoute, question);
  } else if (effectiveRoute.action === 'compare_players') {
    result = await runComparePlayers(effectiveRoute, question);
  } else if (effectiveRoute.action === 'head_to_head') {
    result = await runHeadToHead(effectiveRoute, question);
  } else if (effectiveRoute.action === 'top_players') {
    result = await runTopPlayers(effectiveRoute, question);
  } else if (effectiveRoute.action === 'glossary') {
    result = await runGlossary(effectiveRoute, question);
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

function buildChitChatAnswer(question = '') {
  const normalizedQuestion = normalizeText(question);

  if (/\b(thanks|thank you)\b/.test(normalizedQuestion)) {
    return "You're welcome. Ask me for live scores, player stats, or match predictions.";
  }
  if (/\bwho are you\b|\bwho are u\b|\bwho r u\b|\bwho made you\b|\bwho built you\b|\bwho created you\b/.test(normalizedQuestion)) {
    return 'I am your Cricket AI assistant. Ask me about live scores, player stats, or match predictions.';
  }
  if (/\bhow are you\b|\bwhat can you do\b|\bwhat can u do\b/.test(normalizedQuestion)) {
    return 'Ready to help. Ask me about live scores, player stats, or match predictions.';
  }
  if (/\b(hi|hello|hey|hii|heya|yo|sup)\b/.test(normalizedQuestion)) {
    return 'Hi. Ask me about live scores, player stats, or match predictions.';
  }
  return 'I can answer that briefly, but I am built for cricket. Ask me about live scores or player stats.';
}

async function buildConversationalFallbackAnswer(question = '') {
  const messages = [
    {
      role: 'system',
      content: GENERAL_CHAT_SYSTEM_PROMPT
    },
    {
      role: 'user',
      content: String(question || '').trim()
    }
  ];
  const followups = ['Show live scores', 'Show player stats', 'Predict a match'];

  try {
    const content = await callOpenAiChatCompletions(messages, {
      temperature: 0.4,
      timeoutMs: 30000
    });
    const answer = String(content || '').replace(/\s+/g, ' ').trim();
    if (answer) {
      return {
        answer,
        followups,
        sources: []
      };
    }
  } catch (_) {
    // Fall through to the generic LLM provider or local fallback.
  }

  try {
    const content = await callLlama(messages, {
      purpose: 'reasoning',
      temperature: 0.4,
      timeoutMs: 30000
    });
    const answer = String(content || '').replace(/\s+/g, ' ').trim();
    if (answer) {
      return {
        answer,
        followups,
        sources: []
      };
    }
  } catch (_) {
    // Fall back to the local short response below.
  }

  return {
    answer: buildChitChatAnswer(question),
    followups,
    sources: []
  };
}

async function canonicalizeLeaderboardRows(rows = []) {
  return Promise.all(
    rows.map(async (row) => {
      if (!row?.player) return row;
      const profile = await getPlayerProfile({
        query: row.player,
        datasetName: row.player
      });
      return {
        ...row,
        player: String(profile?.canonical_name || row.player).trim() || row.player
      };
    })
  );
}

async function buildSubjectiveAnalysisAnswer(question = '', route = {}, structuredContext = {}, vectorContext = {}, cricApiContext = {}) {
  const { playerHints, teamHints } = deriveEntityHints(question, route);
  const subject = pickFirst(structuredContext?.result?.data?.subject, playerHints[0], teamHints[0], 'this debate');
  const evidence = [];
  const structuredAnswer = String(structuredContext?.result?.answer || '').trim();
  const vectorPreview = String(vectorContext?.results?.[0]?.document_preview || '').trim();
  const liveItem = cricApiContext?.live_scores?.[0] || cricApiContext?.schedule?.[0] || null;
  const normalizedQuestion = normalizeText(question);

  if (structuredAnswer && structuredAnswer !== NOT_AVAILABLE_MESSAGE) {
    evidence.push(`Verified archive context: ${structuredAnswer}`);
  }
  if (vectorPreview) {
    evidence.push(`Archive narrative context: ${vectorPreview.slice(0, 220).trim()}${vectorPreview.length > 220 ? '...' : ''}`);
  }
  if (liveItem?.name) {
    evidence.push(
      `Live context: ${liveItem.name}${liveItem.status ? ` (${liveItem.status})` : ''}`
    );
  }

  if (/\b(best|top)\s+(bowler|bolwer)\b/.test(normalizedQuestion) && subject && subject !== 'this debate') {
    const bowlers = await canonicalizeLeaderboardRows(await getTopPlayersForTeam(subject, 'wickets', { limit: 3 }));
    if (bowlers.length) {
      return {
        answer: `${bowlers[0].player} is the strongest archived bowling answer for ${subject}, with ${formatStatValue(bowlers[0].wickets)} wickets across ${formatStatValue(bowlers[0].matches)} matches. Other strong archived options are ${bowlers.slice(1).map((row) => row.player).join(' and ')}.`,
        followups: ['Show player stats', 'Compare two players', 'Show recent live scores'],
        sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
      };
    }
  }

  if (/\b(best|top)\s+(batsman|batter|finisher)\b/.test(normalizedQuestion) && subject && subject !== 'this debate') {
    const batters = await canonicalizeLeaderboardRows(await getTopPlayersForTeam(subject, 'runs', { limit: 3 }));
    if (batters.length) {
      return {
        answer: `${batters[0].player} looks like the strongest archived batting pick for ${subject}, with ${formatStatValue(batters[0].runs)} runs across ${formatStatValue(batters[0].matches)} matches. Other strong archived names are ${batters.slice(1).map((row) => row.player).join(' and ')}.`,
        followups: ['Show player stats', 'Compare two players', 'Show recent live scores'],
        sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
      };
    }
  }

  if (/\bwho will win\b|\bpredict(?:ion)?\b/.test(normalizedQuestion)) {
    const base = `Cricket is unpredictable, so there is no guaranteed winner for ${subject}.`;
    if (evidence.length) {
      return {
        answer: `${base}\n\nThe most useful signals I can ground right now are:\n- ${evidence.join('\n- ')}`,
        followups: ['Compare two teams', 'Show recent live scores', 'Show team head to head'],
        sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
      };
    }
    return {
      answer: `${base} The safest way to judge it is to compare recent form, head-to-head record, and lineup strength for the teams you care about.`,
      followups: ['Compare two teams', 'Show recent live scores', 'Show upcoming matches'],
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
    };
  }

  if (evidence.length) {
    return {
      answer: `That question is more debate than single-number fact for ${subject}.\n\nHere is the strongest context I can ground from the available cricket data:\n- ${evidence.join('\n- ')}`,
      followups: ['Compare two players', 'Show team head to head', 'Show recent live scores'],
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
    };
  }

  return {
    answer: `That is a cricket debate rather than a single definitive stat for ${subject}. I would frame it using recent form, match pressure, head-to-head context, and role balance instead of one absolute claim.`,
    followups: ['Compare two players', 'Show team head to head', 'Show recent live scores'],
    sources: buildSourceList(structuredContext, vectorContext, cricApiContext)
  };
}

async function buildTeamInfoFallbackAnswer(structuredContext = {}) {
  const directAnswer = String(structuredContext?.result?.answer || '').trim();
  if (directAnswer && directAnswer !== NOT_AVAILABLE_MESSAGE) {
    return {
      answer: directAnswer,
      followups: ['Show team head to head', 'Show recent live scores', 'Show upcoming matches'],
      sources: buildSourceList(structuredContext, {}, {})
    };
  }

  const team = structuredContext?.result?.data?.team || {};
  const teamName = String(team.name || 'that team').trim();
  const question = String(structuredContext?.result?.data?.question || '').trim();
  const wiki = await fetchWikipediaSummary(teamName);
  const wikiAnswer = buildTeamInfoAnswerFromWiki(question, teamName, wiki);
  return {
    answer:
      wikiAnswer ||
      `I found archived information for ${teamName}, but I could not fully verify the exact latest non-stat detail from the current archive alone.`,
    followups: ['Show team head to head', 'Show recent live scores', 'Show upcoming matches'],
    sources: buildSourceList(structuredContext, {}, {})
  };
}

function buildRecordFallbackAnswer(question = '', structuredContext = {}) {
  const directAnswer = String(structuredContext?.result?.answer || '').trim();
  if (directAnswer && directAnswer !== NOT_AVAILABLE_MESSAGE) {
    return {
      answer: directAnswer,
      followups: ['Show player stats', 'Compare two players', 'Show live scores'],
      sources: buildSourceList(structuredContext, {}, {})
    };
  }

  const metric = String(structuredContext?.result?.data?.metric || '').trim();
  const rows = Array.isArray(structuredContext?.result?.data?.rows) ? structuredContext.result.data.rows : [];
  if (rows.length) {
    return {
      answer: `Here is the closest archived record view for ${metric || 'that record'}: ${rows
        .slice(0, 3)
        .map((row, index) => `#${index + 1} ${row.player || row.team || 'Record'} - ${formatStatValue(row.value)}`)
        .join('; ')}.`,
      followups: ['Show player stats', 'Compare two players', 'Show live scores'],
      sources: buildSourceList(structuredContext, {}, {})
    };
  }
  return {
    answer: `I could not verify that exact cricket record from the current archived data alone.`,
    followups: ['Show player stats', 'Compare two players', 'Show live scores'],
    sources: buildSourceList(structuredContext, {}, {})
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
    short_description: String(profile.short_description || player.short_description || ''),
    description: String(profile.description || player.description || ''),
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

  if ((type === 'team_stats' || type === 'team_info') && data.team?.name) {
    const wikiSummary = await fetchWikipediaSummary(String(data.team.name || '').trim());
    if (wikiSummary) {
      data.team = {
        ...data.team,
        image_url: String(data.team.image_url || wikiSummary.image || '').trim(),
        wikipedia_url: String(data.team.wikipedia_url || wikiSummary.wikipedia_url || '').trim(),
        short_description: String(data.team.short_description || wikiSummary.description || '').trim(),
        description: String(data.team.description || wikiSummary.extract || wikiSummary.description || '').trim()
      };
    }
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

function buildSourceList(
  structuredContext,
  vectorContext,
  cricApiContext,
  modelSources = [],
  cricbuzzContext = {},
  espnContext = {}
) {
  const sources = [];
  const structuredType = String(structuredContext?.result?.data?.type || '').trim();
  if (structuredContext?.available) {
    if (structuredType === 'general_knowledge') {
      sources.push('Local Knowledge');
    } else if (structuredType === 'glossary') {
      sources.push('Glossary');
    } else {
      sources.push('Vector Archive');
    }
  }
  if (vectorContext?.available && Array.isArray(vectorContext.results) && vectorContext.results.length) {
    sources.push('Vector DB');
  }
  if (cricApiContext?.available) sources.push('CricAPI');
  if (cricbuzzContext?.available) sources.push('Cricbuzz');
  if (espnContext?.available) sources.push('ESPN');
  if (Array.isArray(cricApiContext?.web_scraper_matches) && cricApiContext.web_scraper_matches.length) {
    sources.push('Web Scraper');
  }
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

  if (route.action === 'subjective_analysis') {
    return [
      'Compare two players',
      'Show team head to head',
      'Show recent live scores'
    ];
  }

  if (route.action === 'general_knowledge' || route.action === 'glossary') {
    return [
      'What is LBW?',
      'Who won WC 2011?',
      'Difference between ODI and T20'
    ];
  }

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

function fallbackAnswer(
  question,
  route,
  structuredContext,
  vectorContext,
  cricApiContext,
  cricbuzzContext = {},
  espnContext = {}
) {
  const structuredType = String(structuredContext?.result?.data?.type || '').trim();
  const normalizedQuestion = normalizeText(question);
  const isLiveOrScheduleQuery =
    LIVE_QUERY_REGEX.test(normalizedQuestion) || SCHEDULE_QUERY_REGEX.test(normalizedQuestion);

  if (structuredContext?.result?.answer && !(isLiveOrScheduleQuery && structuredType === 'name_resolution')) {
    return {
      answer: structuredContext.result.answer,
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext, [], cricbuzzContext, espnContext)
    };
  }

  if (!shouldUseLooseFallback(question, route)) {
    return {
      answer: NOT_AVAILABLE_MESSAGE,
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext, [], cricbuzzContext, espnContext)
    };
  }

  const liveMatch = cricApiContext?.live_scores?.[0];
  if (liveMatch) {
    return {
      answer: buildLiveAnswer(question, route, cricApiContext),
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext, [], cricbuzzContext, espnContext)
    };
  }

  const providerStatus = buildProviderStatus(cricApiContext?.errors || []);
  if (providerStatus && (isLiveOrScheduleQuery || route.action === 'not_supported')) {
    return {
      answer: providerStatus.message,
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext, [], cricbuzzContext, espnContext)
    };
  }

  const vectorHit = vectorContext?.results?.[0];
  if (vectorHit?.document_preview) {
    return {
      answer: 'I found relevant archive context, but not enough verified structured evidence to produce a professional cricket summary for that query.',
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext, [], cricbuzzContext, espnContext)
    };
  }

  return {
    answer: NOT_AVAILABLE_MESSAGE,
    followups: defaultFollowups(route, structuredContext),
    sources: buildSourceList(structuredContext, vectorContext, cricApiContext, [], cricbuzzContext, espnContext)
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
  const archiveRecentMatches = Array.isArray(cricApiContext.archive_recent_matches)
    ? cricApiContext.archive_recent_matches
    : [];
  const activeMatch = liveItems.find((item) => item.live) || null;
  const recentMatch = liveItems[0] || archiveRecentMatches[0] || null;
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
  if (!liveItems.length && !scheduleItems.length && !archiveRecentMatches.length && providerStatus) {
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

function buildGroundedAnswer(
  question,
  route,
  structuredContext,
  vectorContext,
  cricApiContext,
  cricbuzzContext = {},
  espnContext = {}
) {
  const parts = [];
  const liveAnswer = buildLiveAnswer(question, route, cricApiContext);
  const formAnswer = buildPlayerFormAnswer(question, route, vectorContext);
  const cricbuzzPlayer = Array.isArray(cricbuzzContext.players) ? cricbuzzContext.players[0] : null;
  const espnPlayer = espnContext?.player && typeof espnContext.player === 'object' ? espnContext.player : null;

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

  if (cricbuzzPlayer?.name && Object.keys(cricbuzzPlayer.stats || {}).length) {
    const statBits = uniqueNonEmpty([
      cricbuzzPlayer.stats.runs !== undefined ? `Runs: ${cricbuzzPlayer.stats.runs}` : '',
      cricbuzzPlayer.stats.average !== undefined ? `Average: ${cricbuzzPlayer.stats.average}` : '',
      cricbuzzPlayer.stats.strike_rate !== undefined ? `Strike Rate: ${cricbuzzPlayer.stats.strike_rate}` : '',
      cricbuzzPlayer.stats.wickets !== undefined ? `Wickets: ${cricbuzzPlayer.stats.wickets}` : ''
    ]);
    if (statBits.length) {
      parts.push(`${cricbuzzPlayer.name} Cricbuzz snapshot\n${statBits.join('\n')}`);
    }
  }

  if (espnPlayer?.name && hasMeaningfulStats(espnPlayer.career_summary || {})) {
    parts.push(
      `${espnPlayer.name} ESPN career fallback\nRuns: ${espnPlayer.career_summary.runs || 0}\nAverage: ${
        espnPlayer.career_summary.average || 0
      }\nStrike Rate: ${espnPlayer.career_summary.strike_rate || 0}\nWickets: ${
        espnPlayer.career_summary.wickets || 0
      }`
    );
  }

  if (structuredContext?.available && structuredContext?.result?.answer && parts.length === 0) {
    parts.push(structuredContext.result.answer);
  }

  if (!parts.length) return null;
  return {
    answer: parts.join('\n\n'),
    followups: defaultFollowups(route, structuredContext),
    sources: buildSourceList(structuredContext, vectorContext, cricApiContext, [], cricbuzzContext, espnContext)
  };
}

async function synthesizeAnswer(
  question,
  route,
  structuredContext,
  vectorContext,
  cricApiContext,
  cricbuzzContext,
  espnContext,
  executionContext = {}
) {
  if (isGeneralConversationAction(route.action)) {
    return await buildConversationalFallbackAnswer(question);
  }
  if (route.action === 'general_knowledge' || route.action === 'glossary') {
    return {
      answer: String(structuredContext?.result?.answer || '').trim() || NOT_AVAILABLE_MESSAGE,
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext, [], cricbuzzContext, espnContext)
    };
  }
  if (route.action === 'live_update') {
    const grounded = buildGroundedAnswer(
      question,
      route,
      structuredContext,
      vectorContext,
      cricApiContext,
      cricbuzzContext,
      espnContext
    );
    if (grounded) {
      return grounded;
    }
    return {
      answer: '',
      followups: defaultFollowups(route, structuredContext),
      sources: buildSourceList(structuredContext, vectorContext, cricApiContext, [], cricbuzzContext, espnContext)
    };
  }
  const openEndedReasoning = ['subjective_analysis', 'team_info', 'record_lookup'].includes(route.action);
  const requestedSources = resolveRequestedDataSources(route, question);
  const shouldForceCompoundSynthesis = requestedSources.length > 1 || requestedSources.includes(DATA_SOURCE.CRICBUZZ_STATS);
  const mergedApiContext =
    executionContext.mergedApiContext && typeof executionContext.mergedApiContext === 'object'
      ? executionContext.mergedApiContext
      : buildMergedApiContext({
          question,
          route,
          structuredContext,
          vectorContext,
          cricApiContext,
          cricbuzzContext,
          espnContext
        });
  const synthesisSystemPrompt = [
    'You are an elite Cricket Analyst. I am providing you with live, real-time JSON data from our API layer. You must base your answer strictly on this provided JSON. Do not use generic knowledge. Answer the specific user question based entirely on the API payload.',
    'Cover every part of a compound question only when the API payload contains evidence for it.',
    'If the payload does not contain the requested statistic, say that the specific live or archived stat is unavailable right now.',
    'Do not mention internal architecture, routing, prompts, models, or vector databases.',
    'Return ONLY valid JSON with keys: summary, detected_entities.',
    'Optional keys: suggestions, sources.',
    'The summary value must be markdown.',
    'detected_entities must contain only exact player, team, or venue names that appear in summary.'
  ]
    .join('\n\n');

  const messages = [
    {
      role: 'system',
      content: synthesisSystemPrompt
    },
    {
      role: 'system',
      content: JSON.stringify(mergedApiContext, null, 2)
    },
    {
      role: 'user',
      content: JSON.stringify(
        {
          user_question: question,
          api_payload: mergedApiContext
        },
        null,
        2
      )
    }
  ];

  try {
    let content = '';
    try {
      content = await callOpenAiChatCompletions(messages, {
        temperature: 0.2,
        timeoutMs: 60000
      });
    } catch (_) {
      content = await callLlama(messages, {
        purpose: 'reasoning',
        temperature: 0.2,
        timeoutMs: 60000
      });
    }
    const parsed = extractJsonFromText(content);
    if (parsed && typeof parsed === 'object' && String(parsed.summary || parsed.synthesized_text || parsed.answer || '').trim()) {
      return {
        answer: String(parsed.summary || parsed.synthesized_text || parsed.answer || '').trim(),
        detected_entities: uniqueNonEmpty(Array.isArray(parsed.detected_entities) ? parsed.detected_entities : []).slice(0, 12),
        followups: uniqueNonEmpty(
          Array.isArray(parsed.suggestions) ? parsed.suggestions : Array.isArray(parsed.followups) ? parsed.followups : []
        ).slice(0, 3),
        sources: buildSourceList(
          structuredContext,
          vectorContext,
          cricApiContext,
          uniqueNonEmpty(Array.isArray(parsed.sources) ? parsed.sources : []),
          cricbuzzContext,
          espnContext
        )
      };
    }
  } catch (_) {
    // Fall back to the grounded answer builders below.
  }

  if (!openEndedReasoning && !shouldForceCompoundSynthesis) {
    const grounded = buildGroundedAnswer(
      question,
      route,
      structuredContext,
      vectorContext,
      cricApiContext,
      cricbuzzContext,
      espnContext
    );
    if (grounded) {
      return grounded;
    }
  }

  if (route.action === 'subjective_analysis') {
    return await buildSubjectiveAnalysisAnswer(
      question,
      route,
      structuredContext,
      vectorContext,
      cricApiContext
    );
  }
  if (route.action === 'team_info') {
    return await buildTeamInfoFallbackAnswer(structuredContext);
  }
  if (route.action === 'record_lookup') {
    return buildRecordFallbackAnswer(question, structuredContext);
  }

  return fallbackAnswer(
    question,
    route,
    structuredContext,
    vectorContext,
    cricApiContext,
    cricbuzzContext,
    espnContext
  );
}

async function handleQuery({ question = '', query = '', sessionId = '' } = {}) {
  return processQuery({ question, query, sessionId });
}

async function processQuery({ question = '', query = '', sessionId = '' } = {}, { onStatus } = {}) {
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

  const session = getSession(String(sessionId || '').trim());
  const effectiveText = applySessionContext(text, session);

  const route = applySessionRouteFallback(
    normalizeRoute(await routeQuestion(effectiveText, {})),
    text,
    session
  );
  let structuredContext = await buildStructuredContext(route, effectiveText);
  const effectiveRoute = structuredContext.route || route;
  let vectorContext = createEmptyVectorContext();
  let cricApiContext = createEmptyCricApiContext();
  let cricbuzzContext = createEmptyCricbuzzContext();
  let espnContext = createEmptyEspnContext();
  let executionContext = {
    requestedSources: resolveRequestedDataSources(effectiveRoute, effectiveText),
    livePayload: null,
    cricbuzzContext,
    espnContext,
    mergedApiContext: buildMergedApiContext({
      question: effectiveText,
      route: effectiveRoute,
      structuredContext,
      espnContext
    }),
    mergedEvidenceContext: ''
  };

  if (isGeneralConversationAction(effectiveRoute.action)) {
    emitStatus(onStatus, {
      stage: 'responding',
      action: effectiveRoute.action,
      message: actionStatusMessage(effectiveRoute.action)
    });
  } else if (effectiveRoute.action === 'live_update') {
    executionContext = await executeRouteDataSources(effectiveRoute, effectiveText, { onStatus });
    cricApiContext = executionContext.cricApiContext;
  } else {
    structuredContext = await enrichStructuredResult(structuredContext, effectiveRoute, effectiveText);
    executionContext = await executeRouteDataSources(effectiveRoute, effectiveText, {
      onStatus,
      structuredContext
    });
    vectorContext = executionContext.vectorContext;
    cricApiContext = executionContext.cricApiContext;
    cricbuzzContext = executionContext.cricbuzzContext;
    espnContext = executionContext.espnContext;
  }

  if (!isGeneralConversationAction(effectiveRoute.action)) {
    executionContext.mergedApiContext = buildMergedApiContext({
      question: effectiveText,
      route: effectiveRoute,
      structuredContext,
      vectorContext,
      cricApiContext,
      cricbuzzContext,
      espnContext
    });
    emitStatus(onStatus, {
      stage: 'synthesizing',
      message: 'Preparing answer.'
    });
  }
  const synthesized = await synthesizeAnswer(
    effectiveText,
    effectiveRoute,
    structuredContext,
    vectorContext,
    cricApiContext,
    cricbuzzContext,
    espnContext,
    executionContext
  );

  const publicDetails = buildPublicDetails(
    text,
    effectiveRoute,
    structuredContext,
    vectorContext,
    cricApiContext,
    synthesized,
    espnContext
  );
  syncSessionState(session, effectiveRoute, structuredContext, publicDetails);

  return {
    statusCode: 200,
    response: buildResponse({
      answer: synthesized.answer,
      data: publicDetails,
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
