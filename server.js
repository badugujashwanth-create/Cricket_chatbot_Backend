const express = require('express');
const cors = require('cors');
const path = require('path');
const csvAnalytics = require('./csvAnalytics');
const chromaSearch = require('./chromaSearch');
const cricketAssistant = require('./cricketAssistant');
const liveCricketService = require('./liveCricketService');
const {
  players,
  matches,
  teams,
  insights,
  metrics,
  findPlayerByName,
  findTeamByName,
  getRecentMatchesForTeam,
  getRelatedMatchesForPlayer,
  getMatchHighlights
} = require('./data');

const app = express();
const port = process.env.PORT || 3000;
const frontendPath = path.join(__dirname, '../frontend');

app.use(cors());
app.use(express.json());
app.use(express.static(frontendPath));

const summaryPayload = {
  matches: matches.length,
  players: players.length,
  teams: teams.length,
  metrics: metrics.length,
  insights: insights.length
};

csvAnalytics.startIndexing().catch((error) => {
  console.error('CSV analytics indexing failed:', error.message);
});

app.get('/api/summary', (req, res) => {
  const summary = csvAnalytics.summarizeForDashboard(summaryPayload);
  if (!csvAnalytics.getCache()) {
    const status = csvAnalytics.getStatus();
    summary.index_status = status.status;
    if (status.rowsProcessed) {
      summary.rows_indexed_so_far = status.rowsProcessed;
    }
  }
  res.json(summary);
});

app.get('/api/players', (req, res) => {
  res.json(csvAnalytics.toDashboardPlayers(6) || players);
});

app.get('/api/players/:id', (req, res) => {
  const target = players.find((player) => player.id === req.params.id);
  if (!target) {
    return res.status(404).json({ message: 'Player not found' });
  }
  res.json(target);
});

app.get('/api/matches', (req, res) => {
  const requested = Math.min(10, Math.max(1, parseInt(req.query.limit, 10) || 4));
  res.json(csvAnalytics.toDashboardMatches(requested) || getMatchHighlights(requested));
});

app.get('/api/metrics', (req, res) => {
  res.json(csvAnalytics.toDashboardMetrics(metrics));
});

app.get('/api/insights', (req, res) => {
  res.json(insights);
});

app.get('/api/index-status', (req, res) => {
  res.json({
    ...csvAnalytics.getStatus(),
    chroma: chromaSearch.getStatus(),
    live: liveCricketService.getStatus()
  });
});

app.get('/api/live/status', (req, res) => {
  res.json(liveCricketService.getStatus());
});

app.get('/api/live/score', async (req, res) => {
  try {
    const query = String(req.query.q || '').trim();
    const force = String(req.query.force || '').toLowerCase() === 'true';
    const snapshot = await liveCricketService.getSnapshot({ query, force });
    res.json(snapshot);
  } catch (error) {
    console.error('Live score fetch failed:', error);
    res.status(500).json({
      available: false,
      configured: liveCricketService.getStatus().configured,
      message: 'Failed to fetch live score',
      error: error.message
    });
  }
});

app.post('/api/query', async (req, res) => {
  try {
    const result = await cricketAssistant.handleQuery(req.body || {});
    if (!result.success) {
      return res.status(result.statusCode || 400).json(result);
    }
    res.json(result);
  } catch (error) {
    console.error('Query handling failed:', error);
    res.status(500).json({ success: false, message: 'Failed to process query' });
  }
});

app.get('*', (req, res) => {
  if (req.path.startsWith('/api')) {
    return res.status(404).json({ message: 'Endpoint not found' });
  }
  res.sendFile(path.join(frontendPath, 'index.html'));
});

app.listen(port, () => {
  console.log(`Cricket chat backend listening on http://localhost:${port}`);
});

function avgRecent(scores = []) {
  if (!scores.length) return 0;
  return scores.reduce((sum, value) => sum + value, 0) / scores.length;
}

function craftPlayerSummary(player) {
  const recentAvg = avgRecent(player.stats.recentScores || []);
  const consistency = recentAvg > player.stats.average ? 'above' : 'around';
  return `${player.name} averages ${player.stats.average} (${player.stats.matches} matches) and is currently performing ${consistency} career form with last five innings averaging ${recentAvg.toFixed(1)}.`;
}

function parseComparisonQuery(query) {
  const text = String(query || '').trim();
  if (!text) return null;

  const vsMatch = text.match(/^(.+?)\s+vs\s+(.+)$/i);
  if (vsMatch) {
    return { left: vsMatch[1].trim(), right: vsMatch[2].trim() };
  }

  const compareMatch = text.match(/^compare\s+(.+?)\s+(?:with|and)\s+(.+)$/i);
  if (compareMatch) {
    return { left: compareMatch[1].trim(), right: compareMatch[2].trim() };
  }

  return null;
}

function craftComparisonSummary(left, right) {
  const leftRecent = avgRecent(left.stats.recentScores || []);
  const rightRecent = avgRecent(right.stats.recentScores || []);
  const formLeader = leftRecent === rightRecent ? null : leftRecent > rightRecent ? left.name : right.name;
  const avgLeader =
    typeof left.stats.average === 'number' && typeof right.stats.average === 'number'
      ? left.stats.average === right.stats.average
        ? null
        : left.stats.average > right.stats.average
          ? left.name
          : right.name
      : null;

  const formLine = formLeader ? `${formLeader} has the stronger recent five-innings form.` : 'Their recent form is very close.';
  const avgLine = avgLeader ? `${avgLeader} leads on listed career average.` : 'Their listed averages are comparable in this sample.';
  return `${left.name} vs ${right.name}: ${avgLine} ${formLine}`;
}

function craftTeamSummary(team) {
  const recent = getRecentMatchesForTeam(team.name, 3);
  if (!recent.length) {
    return `${team.name} is profiled with ${team.captain} as captain and ${team.coach} as coach, but no recent sample matches are indexed yet.`;
  }

  const wins = recent.filter((match) => match.result.toLowerCase().startsWith(team.name.toLowerCase())).length;
  return `${team.name} (${team.region}) is captained by ${team.captain}. In the latest ${recent.length} indexed matches, they won ${wins}.`;
}
