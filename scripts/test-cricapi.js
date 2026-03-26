require('../loadEnv');

const baseUrl = (process.env.TEST_BASE_URL || 'http://localhost:3000').replace(/\/+$/, '');

async function fetchJson(path) {
  const response = await fetch(`${baseUrl}${path}`);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.message || `Request failed with ${response.status}`);
  }
  return payload;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function run() {
  const live = await fetchJson('/api/cricapi/live-scores?includeRecent=true&limit=2');
  assert(live.provider === 'cricapi', 'live-scores: missing provider');
  assert(Array.isArray(live.items), 'live-scores: missing items');

  const players = await fetchJson('/api/cricapi/players/search?q=Virat&limit=5');
  assert(Array.isArray(players.items) && players.items.length > 0, 'players/search: no players returned');

  const playerId = players.items[0].id;
  const player = await fetchJson(`/api/cricapi/players/${encodeURIComponent(playerId)}`);
  assert(player.player?.id === playerId, 'players/:id: wrong player returned');

  const schedule = await fetchJson('/api/cricapi/schedule?limit=3');
  assert(Array.isArray(schedule.items), 'schedule: missing items');

  const series = await fetchJson('/api/cricapi/series?q=Indian&limit=3');
  assert(Array.isArray(series.items) && series.items.length > 0, 'series: no series returned');

  const seriesId = series.items[0].id;
  const seriesDetail = await fetchJson(`/api/cricapi/series/${encodeURIComponent(seriesId)}`);
  assert(seriesDetail.series?.id === seriesId, 'series/:id: wrong series returned');
  assert(Array.isArray(seriesDetail.matches), 'series/:id: missing matches');

  console.log('CricAPI endpoints verified.');
}

run().catch((error) => {
  console.error('CricAPI test failed:', error.message);
  process.exitCode = 1;
});
