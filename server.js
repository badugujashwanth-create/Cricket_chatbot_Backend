const express = require('express');
const cors = require('cors');
const path = require('path');
const datasetStore = require('./datasetStore');
const { handleQuery } = require('./queryService');

const app = express();
const port = Number(process.env.PORT || 3000);
const frontendPath = path.join(__dirname, '../frontend');

app.use(cors());
app.use(express.json());
app.use(express.static(frontendPath));

datasetStore.start().catch((error) => {
  console.error('Failed to load dataset:', error.message);
});

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
    limit: Number(req.query.limit || 12)
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
  return res.json({
    id: player.id,
    name: player.name,
    team: player.team,
    stats: player.stats,
    recent_matches: (player.stats?.recent_matches || []).slice(0, 5)
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

app.post('/api/query', async (req, res) => {
  try {
    const outcome = await handleQuery(req.body || {});
    return res.status(outcome.statusCode || 200).json(outcome.response);
  } catch (error) {
    console.error('Query failed:', error);
    return res.status(500).json({
      answer: 'Something went wrong while processing the question.',
      data: {},
      followups: []
    });
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
});
