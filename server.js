require('./loadEnv');

const express = require('express');
const cors = require('cors');
const path = require('path');
const { handleQuery, processQuery } = require('./queryService');
const { querySemanticCache, saveSemanticCacheEntry, resolveDbDir, readChromaManifest } = require('./chromaService');
const { getSession, clearPendingClarification, setPendingClarification, updateContext } = require('./sessionStore');
const { startDailyIngestor } = require('./workers/dailyIngestor');
const { getPlayerProfile } = require('./playerProfileService');
const {
  loadPlayerProfiles,
  searchPlayers: searchVectorPlayers,
  getPlayerById,
  loadTeamSummaries,
  searchTeams: searchVectorTeams,
  loadMatchSummaries,
  getMatchById,
  findMatchesForTeam,
  getTopPlayersByMetric
} = require('./vectorIndexService');
const {
  getLiveScores,
  searchPlayers,
  getPlayerInfo,
  getMatchSchedule,
  getSeriesList,
  getSeriesInfo,
  toBoolean,
  toPositiveInteger
} = require('./cricApiService');

const app = express();
const port = Number(process.env.PORT || 3000);
const frontendPath = path.join(__dirname, '../frontend');
const SESSION_CONTEXT_PRONOUN_REGEX = /\b(he|him|his|she|her|they|them|their)\b/i;
const CHIT_CHAT_QUERY_REGEX = /^(hi|hello|hey|hii|heya|how are you|who are you|thanks|thank you)\b/i;
const CACHE_BYPASS_QUERY_REGEX =
  /\b(vs|versus|compare|better|stronger|dangerous|most|highest|fastest|best|top|prediction|predict|why|choke|inconsistent|overrated|greatest|goat|strongest|upcoming|schedule|latest|today|current|live|captain|coach|owner|troph(?:y|ies)|titles?|history|founded|ground|stadium|retired|retirement)\b/i;

app.use(cors());
app.use(express.json());
app.use(express.static(frontendPath));

function toApiMatch(match = {}) {
  const team1 = String(match.team1 || '').trim();
  const team2 = String(match.team2 || '').trim();
  const winner = String(match.winner || '').trim();
  const inningsSummary = String(match.innings_summary || '').trim();
  return {
    id: String(match.id || '').trim(),
    name: team1 && team2 ? `${team1} vs ${team2}` : 'Match Summary',
    teams: [team1, team2].filter(Boolean),
    date: String(match.date || '').trim(),
    venue: String(match.venue || '').trim(),
    status: winner ? `${winner} won` : 'Result unavailable',
    winner,
    match_type: String(match.format || '').trim(),
    summary: [winner ? `${winner} won.` : '', inningsSummary].filter(Boolean).join(' '),
    top_batters: [],
    top_bowlers: [],
    score: []
  };
}

function toPlayerSearchItem(player = {}) {
  return {
    id: String(player.id || '').trim(),
    name: String(player.canonical_name || player.name || '').trim(),
    canonical_name: String(player.canonical_name || player.name || '').trim(),
    dataset_name: String(player.name || '').trim(),
    team: String(player.team || '').trim(),
    role: String(player.role || '').trim(),
    stats: {
      matches: Number(player.matches || 0),
      runs: Number(player.runs || 0),
      average: Number(player.average || 0),
      strike_rate: Number(player.strike_rate || 0),
      wickets: Number(player.wickets || 0),
      economy: Number(player.economy || 0),
      fours: Number(player.fours || 0),
      sixes: Number(player.sixes || 0)
    }
  };
}

function getVectorStatus() {
  const dbDir = resolveDbDir();
  const manifest = readChromaManifest();
  const summary = manifest?.dataset_summary && typeof manifest.dataset_summary === 'object'
    ? manifest.dataset_summary
    : {};
  return {
    status: dbDir ? 'ready' : 'missing',
    source: 'chroma',
    db_dir: dbDir,
    collection: String(manifest?.collection || process.env.CHROMA_COLLECTION || 'cricket_semantic_index'),
    counts: {
      documents: Number(manifest?.collection_count || 0),
      players: Number(manifest?.player_docs || 0),
      teams: Number(manifest?.team_docs || 0),
      matches: Number(manifest?.match_docs || 0)
    },
    summary
  };
}

app.get('/api/status', (req, res) => {
  return res.json(getVectorStatus());
});

function writeSseEvent(res, event, payload) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function uniqueNonEmpty(values = []) {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
}

function readPositiveInt(value, fallback, options) {
  return toPositiveInteger(value, fallback, options);
}

function toSortableTimestamp(value = '') {
  const parsed = Date.parse(String(value || '').trim());
  return Number.isNaN(parsed) ? 0 : parsed;
}

function handleExternalError(res, error) {
  const statusCode = Number(error?.statusCode || 500);
  return res.status(statusCode).json({
    message: error?.message || 'External source request failed.',
    ...(error?.details && typeof error.details === 'object' ? error.details : {})
  });
}

function toUnifiedTypeFromLegacy(details = {}) {
  const rawType = String(details.type || '').trim();
  if (rawType === 'player_stats' || rawType === 'player_season_stats') return 'player';
  if (rawType === 'team_stats') return 'team';
  if (rawType === 'match_summary' || rawType === 'live_update') return 'match';
  if (rawType === 'compare_players' || rawType === 'head_to_head') return 'comparison';
  return 'record';
}

function statLabelToKey(label = '') {
  return String(label || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'value';
}

function normalizeCachedStats(response = {}, details = {}) {
  if (response?.stats && typeof response.stats === 'object' && !Array.isArray(response.stats)) {
    return response.stats;
  }

  const keyStats = Array.isArray(response?.key_stats) ? response.key_stats : [];
  if (keyStats.length) {
    return keyStats.reduce((accumulator, item, index) => {
      const key = statLabelToKey(item?.label || `stat_${index + 1}`);
      if (item && typeof item === 'object' && 'left' in item && 'right' in item) {
        accumulator[`${key}_left`] = item.left;
        accumulator[`${key}_right`] = item.right;
      } else if (item && typeof item === 'object' && 'value' in item) {
        accumulator[key] = item.value;
      }
      return accumulator;
    }, {});
  }

  if (details?.stats && typeof details.stats === 'object') {
    return details.stats;
  }

  return {};
}

function normalizeCachedExtra(response = {}, details = {}) {
  if (response?.extra && typeof response.extra === 'object') {
    return response.extra;
  }

  const suggestions = Array.isArray(response?.suggestions)
    ? response.suggestions
    : Array.isArray(response?.followups)
      ? response.followups
      : [];
  const insights = Array.isArray(response?.insights) ? response.insights : [];
  const extra = {
    action: String(details.type || response.type || 'summary').trim(),
    suggestions,
    insights
  };

  if (details.player) {
    extra.entities = { player: details.player };
  } else if (details.team) {
    extra.entities = { team: details.team };
  } else if (details.left || details.right) {
    extra.entities = {
      left: details.left || {},
      right: details.right || {}
    };
  }

  return extra;
}

function normalizeCachedImage(response = {}, details = {}) {
  const candidates = [
    response?.image,
    details?.player?.image_url,
    details?.team?.image_url,
    details?.image_url,
    details?.left?.image_url
  ];
  return candidates.map((value) => String(value || '').trim()).find(Boolean) || '';
}

function normalizeCachedResponseShape(response = {}, fallbackQuestion = '') {
  if (
    response &&
    typeof response === 'object' &&
    'type' in response &&
    'title' in response &&
    'summary' in response &&
    'stats' in response &&
    'extra' in response
  ) {
    return response;
  }

  const details =
    response?.details && typeof response.details === 'object'
      ? response.details
      : response?.data && typeof response.data === 'object'
        ? response.data
        : {};
  const summary =
    String(response?.summary || response?.answer || '').trim() ||
    'A cached response was found, but no answer text was available.';

  return {
    type: toUnifiedTypeFromLegacy(details),
    title: String(details.title || response?.title || 'Cricket Intelligence').trim() || 'Cricket Intelligence',
    image: normalizeCachedImage(response, details),
    summary,
    stats: normalizeCachedStats(response, details),
    extra: normalizeCachedExtra(response, details)
  };
}

function buildCachedQueryResponse(cacheHit = {}, fallbackQuestion = '') {
  if (cacheHit?.response && typeof cacheHit.response === 'object') {
    return normalizeCachedResponseShape(cacheHit.response, fallbackQuestion);
  }

  const answerText =
    String(cacheHit?.answer_text || '').trim() ||
    'A cached response was found, but no answer text was available.';

  return {
    type: 'record',
    title: 'Cricket Intelligence',
    image: '',
    summary: answerText,
    stats: {},
    extra: {
      action: 'semantic_cache',
      question: fallbackQuestion,
      suggestions: [],
      insights:
        cacheHit?.ui_payload && typeof cacheHit.ui_payload === 'object'
          ? [answerText]
          : []
    }
  };
}

async function maybeGetSemanticCacheHit(question = '') {
  const cleanQuestion = String(question || '').trim();
  if (!cleanQuestion) {
    return {
      hit: false
    };
  }
  return querySemanticCache(cleanQuestion);
}

function canUseSemanticCache(question = '', sessionId = '') {
  const cleanQuestion = String(question || '').trim();
  const cleanSessionId = String(sessionId || '').trim();
  if (!cleanQuestion) return false;
  if (CHIT_CHAT_QUERY_REGEX.test(cleanQuestion)) {
    return false;
  }
  if (CACHE_BYPASS_QUERY_REGEX.test(cleanQuestion)) {
    return false;
  }
  if (cleanSessionId && SESSION_CONTEXT_PRONOUN_REGEX.test(cleanQuestion)) {
    return false;
  }
  return true;
}

function syncSessionFromResponse(sessionId = '', response = {}) {
  const cleanSessionId = String(sessionId || '').trim();
  if (!cleanSessionId) return;

  const session = getSession(cleanSessionId);
  const extra = response?.extra && typeof response.extra === 'object' ? response.extra : {};
  const entities = extra?.entities && typeof extra.entities === 'object' ? extra.entities : {};
  const type = String(response?.type || '').trim();

  clearPendingClarification(session);
  if (type === 'player' && entities.player?.name) {
    const patch = {
      action: 'player_stats',
      player_id: String(entities.player.id || '').trim(),
      player_name: String(entities.player.name || '').trim()
    };
    if (entities.player.team) {
      patch.team_name = String(entities.player.team || '').trim();
    }
    updateContext(session, patch);
    return;
  }

  if (type === 'team' && entities.team?.name) {
    updateContext(session, {
      action: 'team_stats',
      team_id: String(entities.team.id || '').trim(),
      team_name: String(entities.team.name || '').trim()
    });
  }
}

app.get('/api/about', async (req, res) => {
  const manifest = readChromaManifest();
  return res.json({
    ...getVectorStatus(),
    built_at: String(manifest?.built_at || ''),
    min_date: String(manifest?.dataset_summary?.min_date || ''),
    max_date: String(manifest?.dataset_summary?.max_date || '')
  });
});

app.get('/api/home', async (req, res) => {
  const manifest = readChromaManifest();
  const [topBatters, topBowlers, teams, recentMatches] = await Promise.all([
    getTopPlayersByMetric('runs', { limit: 5 }),
    getTopPlayersByMetric('wickets', { limit: 5 }),
    loadTeamSummaries(),
    loadMatchSummaries()
  ]);

  return res.json({
    status: getVectorStatus(),
    summary: manifest?.dataset_summary || {},
    leaders: {
      runs: topBatters,
      wickets: topBowlers,
      teams: [...teams]
        .sort((left, right) => Number(right.win_rate || 0) - Number(left.win_rate || 0))
        .slice(0, 5)
        .map((team) => ({
          id: team.id,
          name: team.name,
          matches: team.matches,
          wins: team.wins,
          win_rate: team.win_rate
        }))
    },
    recent_matches: [...recentMatches]
      .sort((left, right) => toSortableTimestamp(right.date) - toSortableTimestamp(left.date))
      .slice(0, 5)
      .map(toApiMatch)
  });
});

app.get('/api/players/search', async (req, res) => {
  const query = String(req.query.q || '').trim();
  const limit = readPositiveInt(req.query.limit, 20, { min: 1, max: 50 });
  const page = readPositiveInt(req.query.page, 1, { min: 1, max: 5000 });
  const offset = (page - 1) * limit;
  const items = await searchVectorPlayers(query, offset + limit);
  const pagedItems = items.slice(offset, offset + limit).map(toPlayerSearchItem);
  return res.json({
    page,
    limit,
    total: items.length,
    items: pagedItems
  });
});

app.get('/api/players/:id', async (req, res) => {
  const player = await getPlayerById(req.params.id);
  if (!player) {
    return res.status(404).json({
      message: 'Player not found.'
    });
  }

  const profile = await getPlayerProfile({
    query: player.canonical_name || player.name,
    datasetName: player.name
  }).catch(() => null);

  return res.json({
    id: player.id,
    name: String(profile?.canonical_name || player.canonical_name || player.name || '').trim() || player.name,
    canonical_name: String(profile?.canonical_name || player.canonical_name || player.name || '').trim() || player.name,
    dataset_name: String(player.name || '').trim(),
    team: String(player.team || '').trim(),
    role: String(player.role || '').trim(),
    country: String(profile?.country || '').trim(),
    image_url: String(profile?.image_url || '').trim(),
    wikipedia_url: String(profile?.wikipedia_url || '').trim(),
    description: String(profile?.description || '').trim(),
    stats: toPlayerSearchItem(player).stats,
    recent_matches: []
  });
});

app.get('/api/players/:id/summary', async (req, res) => {
  const player = await getPlayerById(req.params.id);
  if (!player) {
    return res.status(404).json({
      message: 'Player stats not found.'
    });
  }
  return res.json({
    id: player.id,
    name: String(player.canonical_name || player.name || '').trim() || player.name,
    team: String(player.team || '').trim(),
    stats: toPlayerSearchItem(player).stats
  });
});

app.get('/api/teams/search', async (req, res) => {
  const query = String(req.query.q || '').trim();
  const limit = readPositiveInt(req.query.limit, 20, { min: 1, max: 50 });
  const items = (await searchVectorTeams(query, limit)).map((team) => ({
    id: team.id,
    name: team.name,
    matches: team.matches,
    wins: team.wins,
    win_rate: team.win_rate
  }));
  return res.json({
    total: items.length,
    items
  });
});

app.get('/api/options', async (req, res) => {
  const teams = await loadTeamSummaries();
  const seasons = uniqueNonEmpty((await loadMatchSummaries()).map((match) => String(match.season || '').trim()))
    .sort((left, right) => right.localeCompare(left));
  return res.json({
    teams: teams.map((team) => team.name).sort((left, right) => left.localeCompare(right)),
    seasons,
    venues: []
  });
});

app.get('/api/matches', async (req, res) => {
  const team = String(req.query.team || '').trim();
  const season = String(req.query.season || '').trim();
  const format = String(req.query.format || '').trim();
  const limit = readPositiveInt(req.query.limit, 10, { min: 1, max: 100 });
  const offset = readPositiveInt(req.query.offset, 0, { min: 0, max: 5000 });

  let items = [];
  if (team) {
    items = await findMatchesForTeam(team, { limit, offset, year: season, format });
  } else {
    items = (await loadMatchSummaries())
      .filter((match) => !season || String(match.date || '').startsWith(season))
      .filter((match) => !format || String(match.format || '').toLowerCase() === format.toLowerCase())
      .slice(offset, offset + limit);
  }

  return res.json({
    total: items.length,
    items: items.map(toApiMatch)
  });
});

app.get('/api/matches/:id', async (req, res) => {
  const match = await getMatchById(req.params.id);
  if (!match) {
    return res.status(404).json({
      message: 'Match not found.'
    });
  }
  return res.json(toApiMatch(match));
});

app.get('/api/cricapi/live-scores', async (req, res) => {
  try {
    const result = await getLiveScores({
      offset: readPositiveInt(req.query.offset, 0, { min: 0, max: 5000 }),
      limit: readPositiveInt(req.query.limit, 10, { min: 1, max: 50 }),
      includeRecent: toBoolean(req.query.includeRecent, false),
      team: String(req.query.team || ''),
      matchType: String(req.query.matchType || req.query.format || '')
    });
    return res.json(result);
  } catch (error) {
    return handleExternalError(res, error);
  }
});

app.get('/api/cricapi/players/search', async (req, res) => {
  try {
    const result = await searchPlayers({
      q: String(req.query.q || ''),
      offset: readPositiveInt(req.query.offset, 0, { min: 0, max: 5000 }),
      limit: readPositiveInt(req.query.limit, 10, { min: 1, max: 50 })
    });
    return res.json(result);
  } catch (error) {
    return handleExternalError(res, error);
  }
});

app.get('/api/cricapi/players/:id', async (req, res) => {
  try {
    const result = await getPlayerInfo(req.params.id);
    return res.json(result);
  } catch (error) {
    return handleExternalError(res, error);
  }
});

app.get('/api/cricapi/schedule', async (req, res) => {
  try {
    const result = await getMatchSchedule({
      offset: readPositiveInt(req.query.offset, 0, { min: 0, max: 5000 }),
      limit: readPositiveInt(req.query.limit, 10, { min: 1, max: 50 }),
      team: String(req.query.team || ''),
      matchType: String(req.query.matchType || req.query.format || ''),
      seriesId: String(req.query.seriesId || req.query.series_id || ''),
      upcomingOnly: toBoolean(req.query.upcomingOnly ?? req.query.upcoming_only, true)
    });
    return res.json(result);
  } catch (error) {
    return handleExternalError(res, error);
  }
});

app.get('/api/cricapi/series', async (req, res) => {
  try {
    const result = await getSeriesList({
      q: String(req.query.q || ''),
      offset: readPositiveInt(req.query.offset, 0, { min: 0, max: 5000 }),
      limit: readPositiveInt(req.query.limit, 10, { min: 1, max: 50 })
    });
    return res.json(result);
  } catch (error) {
    return handleExternalError(res, error);
  }
});

app.get('/api/cricapi/series/:id', async (req, res) => {
  try {
    const result = await getSeriesInfo(req.params.id);
    return res.json(result);
  } catch (error) {
    return handleExternalError(res, error);
  }
});

app.post('/api/query', async (req, res) => {
  try {
    const question = String(req.body?.question || req.body?.query || '').trim();
    const sessionId = String(req.body?.sessionId || '').trim();
    const allowSemanticCache = canUseSemanticCache(question, sessionId);
    const cacheHit = allowSemanticCache ? await maybeGetSemanticCacheHit(question) : { hit: false };
    if (cacheHit.hit) {
      const cachedResponse = buildCachedQueryResponse(cacheHit, question);
      syncSessionFromResponse(sessionId, cachedResponse);
      return res.status(200).json(cachedResponse);
    }

    const outcome = await handleQuery(req.body || {});
    if (allowSemanticCache && (outcome.statusCode || 200) < 400) {
      void saveSemanticCacheEntry({
        question,
        response: outcome.response,
        uiPayload: outcome.response?.extra
      });
    }
    return res.status(outcome.statusCode || 200).json(outcome.response);
  } catch (error) {
    console.error('Query failed:', error);
    return res.status(500).json({
      type: 'record',
      title: 'Cricket Intelligence',
      image: '',
      summary: 'Something went wrong while processing the question.',
      stats: {},
      extra: {
        action: 'error',
        suggestions: [],
        insights: ['Something went wrong while processing the question.']
      }
    });
  }
});

app.get('/api/query/stream', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders?.();

  const question = String(req.query.question || req.query.q || '').trim();
  const sessionId = String(req.query.sessionId || '').trim();
  const allowSemanticCache = canUseSemanticCache(question, sessionId);
  let closed = false;
  const heartbeat = setInterval(() => {
    if (!closed) {
      res.write(': keep-alive\n\n');
    }
  }, 15000);

  function closeStream() {
    if (closed) return;
    closed = true;
    clearInterval(heartbeat);
    res.end();
  }

  function sendEvent(event, payload) {
    if (closed) return;
    writeSseEvent(res, event, payload);
  }

  req.on('close', () => {
    closed = true;
    clearInterval(heartbeat);
  });

  try {
    const cacheHit = allowSemanticCache ? await maybeGetSemanticCacheHit(question) : { hit: false };
    if (cacheHit.hit) {
      const cachedResponse = buildCachedQueryResponse(cacheHit, question);
      syncSessionFromResponse(sessionId, cachedResponse);
      sendEvent('status', {
        stage: 'cache_hit',
        message: 'Served from semantic cache.'
      });
      sendEvent('token', {
        content: String(cacheHit.answer_text || cachedResponse.summary || cachedResponse.answer || '')
      });
      if (cacheHit.ui_payload && typeof cacheHit.ui_payload === 'object') {
        sendEvent('ui_command', {
          component: 'cached_response',
          payload: cacheHit.ui_payload
        });
      }
      sendEvent('answer', cachedResponse);
      return;
    }

    const outcome = await processQuery(
      { question, sessionId },
      {
        onStatus: (status) => {
          sendEvent('status', status);
        }
      }
    );

    if (closed) return;

    if ((outcome.statusCode || 200) >= 400) {
      sendEvent('error', {
        statusCode: outcome.statusCode || 500,
        ...outcome.response
      });
    } else {
      if (allowSemanticCache) {
        void saveSemanticCacheEntry({
          question,
          response: outcome.response,
          uiPayload: outcome.response?.extra
        });
      }
      sendEvent('answer', outcome.response);
    }
  } catch (error) {
    console.error('Streaming query failed:', error);
    sendEvent('error', {
      statusCode: 500,
      type: 'record',
      title: 'Cricket Intelligence',
      image: '',
      summary: 'Something went wrong while processing the question.',
      stats: {},
      extra: {
        action: 'error',
        suggestions: [],
        insights: ['Something went wrong while processing the question.']
      }
    });
  } finally {
    if (!closed) {
      sendEvent('done', { done: true });
      closeStream();
    }
  }
});

app.get('*', (req, res) => {
  if (req.path.startsWith('/api')) {
    return res.status(404).json({ message: 'Endpoint not found.' });
  }
  return res.sendFile(path.join(frontendPath, 'index.html'));
});

const server = app.listen(port);

server.on('listening', () => {
  const address = server.address();
  const activePort =
    address && typeof address === 'object' && Number.isFinite(Number(address.port))
      ? Number(address.port)
      : port;
  console.log(`Server running on http://localhost:${activePort}`);
  startDailyIngestor();
});

server.on('error', (error) => {
  if (error?.code === 'EADDRINUSE') {
    console.error(
      [
        `Port ${port} is already in use.`,
        `The backend is likely already running on http://localhost:${port}.`,
        'Stop the existing process before starting again, or run on another port.',
        `PowerShell example: $env:PORT='${port + 1}'; npm start`
      ].join('\n')
    );
    process.exit(1);
    return;
  }

  console.error('Server failed to start:', error);
  process.exit(1);
});
