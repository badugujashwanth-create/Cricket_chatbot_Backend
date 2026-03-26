require('./loadEnv');

const express = require('express');
const cors = require('cors');
const path = require('path');
const datasetStore = require('./datasetStore');
const { handleQuery, processQuery } = require('./queryService');
const { getPlayerProfile } = require('./playerProfileService');
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

app.use(cors());
app.use(express.json());
app.use(express.static(frontendPath));

app.get('/api/status', (req, res) => {
  return res.json(datasetStore.getStatus());
});

function writeSseEvent(res, event, payload) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function readPositiveInt(value, fallback, options) {
  return toPositiveInteger(value, fallback, options);
}

function handleExternalError(res, error) {
  const statusCode = Number(error?.statusCode || 500);
  return res.status(statusCode).json({
    message: error?.message || 'External source request failed.',
    ...(error?.details && typeof error.details === 'object' ? error.details : {})
  });
}

app.get('/api/about', async (req, res) => {
  await datasetStore.waitUntilReady(60000);
  const about = datasetStore.getAbout();
  if (!about) {
    return res.status(503).json({
      message: 'Data is loading. Please try again in a moment.'
    });
  }
  return res.json(about);
});

app.get('/api/home', async (req, res) => {
  await datasetStore.waitUntilReady(60000);
  const data = datasetStore.getHomeData();
  if (!data) {
    return res.status(503).json({
      message: 'Data is loading. Please try again in a moment.'
    });
  }
  return res.json(data);
});

app.get('/api/players/search', async (req, res) => {
  await datasetStore.waitUntilReady(60000);
  const result = datasetStore.searchPlayers({
    q: req.query.q || '',
    page: Number(req.query.page || 1),
    limit: Number(req.query.limit || 20)
  });
  if (!result) {
    return res.status(503).json({
      message: 'Data is loading. Please try again in a moment.'
    });
  }
  return res.json(result);
});

app.get('/api/players/:id', async (req, res) => {
  await datasetStore.waitUntilReady(60000);
  const player = datasetStore.getPlayerById(req.params.id);
  if (!player) {
    return res.status(404).json({
      message: 'Player not found.'
    });
  }

  const profile = await getPlayerProfile({
    query: player.name,
    datasetName: player.name
  }).catch(() => null);

  return res.json({
    id: player.id,
    name: String(profile?.canonical_name || player.name || '').trim() || player.name,
    canonical_name: String(profile?.canonical_name || player.name || '').trim() || player.name,
    dataset_name: player.name,
    team: player.team,
    country: String(profile?.country || '').trim(),
    image_url: String(profile?.image_url || '').trim(),
    wikipedia_url: String(profile?.wikipedia_url || '').trim(),
    description: String(profile?.description || '').trim(),
    stats: player.stats,
    recent_matches: (player.stats?.recent_matches || []).slice(0, 8)
  });
});

app.get('/api/players/:id/summary', async (req, res) => {
  await datasetStore.waitUntilReady(60000);
  const summary = datasetStore.getPlayerSummary(req.params.id, {
    season: req.query.season || '',
    format: req.query.format || ''
  });
  if (!summary) {
    return res.status(404).json({
      message: 'Player stats not found.'
    });
  }
  return res.json(summary);
});

app.get('/api/teams/search', async (req, res) => {
  await datasetStore.waitUntilReady(60000);
  const result = datasetStore.searchTeams(String(req.query.q || ''));
  if (!result) {
    return res.status(503).json({
      message: 'Data is loading. Please try again in a moment.'
    });
  }
  return res.json(result);
});

app.get('/api/options', async (req, res) => {
  await datasetStore.waitUntilReady(60000);
  const about = datasetStore.getAbout();
  const teams = datasetStore.searchTeams('');
  if (!about || !teams) {
    return res.status(503).json({
      message: 'Data is loading. Please try again in a moment.'
    });
  }
  return res.json({
    teams: teams.map((team) => team.name),
    seasons: about.seasons || [],
    venues: about.venues || []
  });
});

app.get('/api/matches', async (req, res) => {
  await datasetStore.waitUntilReady(60000);
  const result = datasetStore.getMatches({
    team: req.query.team || '',
    season: req.query.season || '',
    venue: req.query.venue || '',
    limit: Number(req.query.limit || 10),
    offset: Number(req.query.offset || 0)
  });
  if (!result) {
    return res.status(503).json({
      message: 'Data is loading. Please try again in a moment.'
    });
  }
  return res.json(result);
});

app.get('/api/matches/:id', async (req, res) => {
  await datasetStore.waitUntilReady(60000);
  const match = datasetStore.getMatchById(req.params.id);
  if (!match) {
    return res.status(404).json({
      message: 'Match not found.'
    });
  }
  return res.json(match);
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
    const outcome = await handleQuery(req.body || {});
    return res.status(outcome.statusCode || 200).json(outcome.response);
  } catch (error) {
    console.error('Query failed:', error);
    return res.status(500).json({
      answer: 'Something went wrong while processing the question.',
      summary: 'Something went wrong while processing the question.',
      details: {},
      suggestions: [],
      data: {},
      followups: []
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

  sendEvent('status', {
    stage: 'connected',
    message: 'Connected. Searching stats.'
  });

  try {
    const outcome = await processQuery(
      { question },
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
      sendEvent('answer', outcome.response);
    }
  } catch (error) {
    console.error('Streaming query failed:', error);
    sendEvent('error', {
      statusCode: 500,
      answer: 'Something went wrong while processing the question.',
      summary: 'Something went wrong while processing the question.',
      details: {},
      suggestions: [],
      data: {},
      followups: []
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

app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
  datasetStore.start().catch((error) => {
    console.error('Dataset preload failed:', error);
  });
});
