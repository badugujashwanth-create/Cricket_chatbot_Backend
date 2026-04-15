require('../loadEnv');

const { execFile } = require('child_process');
const path = require('path');
const { promisify } = require('util');

const { getLiveScores, getMatchScorecard } = require('../cricApiService');
const { callLlama } = require('../llamaClient');
const {
  clearVectorQueryCache,
  clearCollectionCache,
  hasMatchDocument,
  upsertDocuments
} = require('../chromaService');
const { clearVectorIndexCache } = require('../vectorIndexService');
const {
  recordCompletedMatch,
  getPlayerByIdFromSql,
  getTeamByIdFromSql,
  getMatchByIdFromSql,
  extractPlayerDeltasFromMatch,
  extractTeamTotalsFromMatch
} = require('../sqlStatsService');

const execFileAsync = promisify(execFile);

const BACKEND_DIR = path.join(__dirname, '..');
const PYTHON_BIN = process.env.PYTHON_BIN || 'python';
const SCRAPER_SCRIPT = path.join(BACKEND_DIR, 'scripts', 'scrape_match.py');
const CHROMA_COLLECTION = process.env.CHROMA_COLLECTION || 'cricket_semantic_index';
const DEFAULT_INTERVAL_MS = Math.max(60_000, Number(process.env.INGEST_INTERVAL_MS || 60_000));
const MATCH_LOOKBACK_HOURS = Number(process.env.INGEST_LOOKBACK_HOURS || 24);
const DEFAULT_MATCH_LIMIT = Math.max(1, Number(process.env.INGEST_MATCH_LIMIT || 10));

let ingestorTimer = null;
let activeRun = null;
let rateLimitBackoffMs = 0;
let rateLimitRetryCount = 0;

function parseJsonText(raw = '') {
  const text = String(raw || '').trim();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (_) {
    const start = text.indexOf('{');
    const end = text.lastIndexOf('}');
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(text.slice(start, end + 1));
      } catch (_) {
        return null;
      }
    }
  }
  return null;
}

function dateValue(value = '') {
  const parsed = Date.parse(String(value || '').trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function recentCompletedMatches(items = [], lookbackHours = MATCH_LOOKBACK_HOURS) {
  const cutoff =
    Date.now() - Math.max(1, Number(lookbackHours) || MATCH_LOOKBACK_HOURS) * 60 * 60 * 1000;
  return (Array.isArray(items) ? items : []).filter((item) => {
    if (!item?.match_ended) return false;
    const timestamp = dateValue(item.date_time_gmt || item.date || '');
    return timestamp !== null && timestamp >= cutoff;
  });
}

function scoreLine(items = []) {
  return (Array.isArray(items) ? items : [])
    .map((row) => {
      const wickets = row.wickets === null || row.wickets === undefined ? '-' : row.wickets;
      const overs = row.overs === null || row.overs === undefined ? '-' : row.overs;
      return `${row.inning || 'Innings'} ${row.runs || 0}/${wickets} (${overs})`;
    })
    .join(', ');
}

function buildFallbackNarrative(match = {}) {
  const innings = Array.isArray(match.scorecard) ? match.scorecard : [];
  const topBatters = innings
    .flatMap((inning) => (Array.isArray(inning.batting) ? inning.batting : []))
    .sort((left, right) => Number(right.runs || 0) - Number(left.runs || 0))
    .slice(0, 3)
    .map((row) => `${row.batsman?.name || 'Unknown'} (${row.runs || 0})`);
  const topBowlers = innings
    .flatMap((inning) => (Array.isArray(inning.bowling) ? inning.bowling : []))
    .sort((left, right) => Number(right.wickets || 0) - Number(left.wickets || 0))
    .slice(0, 3)
    .map((row) => `${row.bowler?.name || 'Unknown'} (${row.wickets || 0}/${row.runs_conceded || 0})`);

  return [
    `${match.name || 'Completed match'} finished with status: ${match.status || 'result unavailable'}.`,
    match.match_winner ? `${match.match_winner} won the match.` : '',
    match.venue ? `Venue: ${match.venue}.` : '',
    match.score?.length ? `Score summary: ${scoreLine(match.score)}.` : '',
    topBatters.length ? `Top batting performances: ${topBatters.join(', ')}.` : '',
    topBowlers.length ? `Top bowling figures: ${topBowlers.join(', ')}.` : ''
  ]
    .filter(Boolean)
    .join(' ');
}

async function summarizeMatchNarrative(match = {}) {
  const rawJson = JSON.stringify(match, null, 2);
  const messages = [
    {
      role: 'system',
      content:
        'You are an expert cricket analyst. Summarize this match into a rich, semantic narrative focusing on key player performances. Use only the provided JSON. Mention the winning side, notable batting contributions, notable bowling spells, and the overall flow of the match. Return plain text only.'
    },
    {
      role: 'user',
      content: `Summarize this match into a rich, semantic narrative focusing on key player performances.\n\n${rawJson}`
    }
  ];

  try {
    const content = await callLlama(messages, {
      purpose: 'reasoning',
      temperature: 0.2,
      timeoutMs: 60000
    });
    const clean = String(content || '').trim();
    return clean || buildFallbackNarrative(match);
  } catch (_) {
    return buildFallbackNarrative(match);
  }
}

function formatNumber(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) return 0;
  return Number.isInteger(numeric) ? numeric : Number(numeric.toFixed(2));
}

function buildPlayerDocument(player = {}) {
  const name = String(player.name || player.canonical_name || '').trim();
  return {
    id: String(player.id || '').trim(),
    document: `Cricket player profile ${name}. Team ${player.team || 'Unknown'}. Role ${
      player.role || 'Cricketer'
    }. Indexed matches ${formatNumber(player.matches)}. Runs ${formatNumber(player.runs)}, wickets ${formatNumber(
      player.wickets
    )}, batting average ${formatNumber(player.average)}, strike rate ${formatNumber(
      player.strike_rate
    )}, bowling economy ${formatNumber(player.economy)}. Fours ${formatNumber(
      player.fours
    )}, sixes ${formatNumber(player.sixes)}. Semantic profile tags: ${
      player.is_active ? 'Active player, live-updated profile.' : 'Archive-backed profile.'
    }`,
    metadata: {
      doc_type: 'player_profile',
      player: name,
      team: String(player.team || '').trim(),
      role: String(player.role || '').trim(),
      matches: formatNumber(player.matches),
      runs: formatNumber(player.runs),
      wickets: formatNumber(player.wickets),
      average: formatNumber(player.average),
      strike_rate: formatNumber(player.strike_rate),
      economy: formatNumber(player.economy),
      fours: formatNumber(player.fours),
      sixes: formatNumber(player.sixes),
      is_active: Boolean(player.is_active),
      source: 'live_sync'
    }
  };
}

function buildTeamDocument(team = {}) {
  return {
    id: String(team.id || '').trim(),
    document: `Cricket team summary for ${team.name}. Indexed matches ${formatNumber(
      team.matches
    )}, wins ${formatNumber(team.wins)}, losses ${formatNumber(team.losses)}, no result ${formatNumber(
      team.no_result
    )}, win rate ${formatNumber(team.win_rate)} percent. Total batting runs ${formatNumber(
      team.runs
    )}, average score ${formatNumber(team.average_score)}, team strike rate ${formatNumber(
      team.strike_rate
    )}.`,
    metadata: {
      doc_type: 'team_summary',
      team: String(team.name || '').trim(),
      matches: formatNumber(team.matches),
      wins: formatNumber(team.wins),
      losses: formatNumber(team.losses),
      no_result: formatNumber(team.no_result),
      win_rate: formatNumber(team.win_rate),
      runs: formatNumber(team.runs),
      average_score: formatNumber(team.average_score),
      strike_rate: formatNumber(team.strike_rate),
      source: 'live_sync'
    }
  };
}

function buildMatchDocument(match = {}) {
  const teams = [String(match.team1 || '').trim(), String(match.team2 || '').trim()].filter(Boolean);
  return {
    id: `match:${String(match.id || '').trim()}`,
    document: `Cricket match ${match.id} on ${match.date || 'date unavailable'} ${
      match.match_type || ''
    } at ${match.venue || 'venue unavailable'}. Teams: ${teams.join(' vs ') || match.name || 'Unknown'}. Winner: ${
      match.winner || 'unknown'
    }. Narrative: ${match.summary || 'n/a'}.`,
    metadata: {
      doc_type: 'match_summary',
      match_id: String(match.id || '').trim(),
      date: String(match.date || '').trim(),
      match_type: String(match.match_type || '').trim(),
      venue: String(match.venue || '').trim(),
      winner: String(match.winner || '').trim(),
      source: String(match.source || 'live_sync').trim() || 'live_sync'
    }
  };
}

function isRateLimitedError(error) {
  const upstreamStatus = Number(error?.details?.upstream_status || error?.statusCode || 0);
  const message = String(error?.message || '').toLowerCase();
  return (
    upstreamStatus === 429 ||
    upstreamStatus === 503 ||
    message.includes('rate limit') ||
    message.includes('hits today exceeded') ||
    message.includes('quota') ||
    message.includes('too many requests') ||
    message.includes('service unavailable')
  );
}

async function scrapeMatchFromUrl(matchUrl = '') {
  const cleanUrl = String(matchUrl || '').trim();
  if (!cleanUrl) {
    throw new Error('A scraper URL is required.');
  }

  const { stdout } = await execFileAsync(
    PYTHON_BIN,
    [SCRAPER_SCRIPT, '--url', cleanUrl],
    {
      cwd: BACKEND_DIR,
      timeout: 120000,
      maxBuffer: 8 * 1024 * 1024
    }
  );

  const payload = parseJsonText(stdout) || {};
  const match = payload.match && typeof payload.match === 'object' ? payload.match : payload;
  if (!match || typeof match !== 'object' || !match.id) {
    throw new Error('Web scraper did not return a normalized match payload.');
  }
  return {
    provider: 'web_scraper',
    source: 'Web Scraper',
    match
  };
}

function resolveMatchUrl(matchFeedItem = {}) {
  return String(
    matchFeedItem.match_url ||
      matchFeedItem.scorecard_url ||
      matchFeedItem.url ||
      matchFeedItem.source_url ||
      ''
  ).trim();
}

async function refreshVectorDocsForMatch(match = {}, syncResult = {}) {
  const basePlayerDeltas = Array.isArray(syncResult.playerDeltas) && syncResult.playerDeltas.length
    ? syncResult.playerDeltas
    : extractPlayerDeltasFromMatch(match);
  const baseTeamDeltas = Array.isArray(syncResult.teamDeltas) && syncResult.teamDeltas.length
    ? syncResult.teamDeltas
    : extractTeamTotalsFromMatch(match);

  const playerDocs = basePlayerDeltas
    .map((delta) => getPlayerByIdFromSql(delta.id))
    .filter(Boolean)
    .map(buildPlayerDocument);
  const teamDocs = baseTeamDeltas
    .map((delta) => getTeamByIdFromSql(delta.id))
    .filter(Boolean)
    .map(buildTeamDocument);
  const sqlMatch = getMatchByIdFromSql(match.id);
  const matchDocs = sqlMatch ? [buildMatchDocument(sqlMatch)] : [];
  const docs = [...playerDocs, ...teamDocs, ...matchDocs];

  if (docs.length) {
    await upsertDocuments(docs, { collection: CHROMA_COLLECTION });
    clearVectorIndexCache();
    clearVectorQueryCache();
    clearCollectionCache();
  }

  return {
    player_documents: playerDocs.length,
    team_documents: teamDocs.length,
    match_documents: matchDocs.length
  };
}

async function runLiveSqlVectorSync(match = {}, narrative = '', sourceLabel = 'CricAPI') {
  const syncResult = recordCompletedMatch(match, { narrative: narrative || buildFallbackNarrative(match) });
  const docCounts = await refreshVectorDocsForMatch(match, syncResult);

  return {
    match_id: String(match.id || '').trim(),
    narrative_saved: Boolean(syncResult.applied || syncResult.reason === 'already_synced'),
    updated_existing: Boolean(syncResult.reason === 'already_synced'),
    source: sourceLabel,
    summary: String(syncResult.summary || '').trim(),
    ...docCounts
  };
}

async function ensureMatchIndexed(
  { matchId = '', matchUrl = '', matchFeedItem = null, force = false } = {},
  { collection = CHROMA_COLLECTION } = {}
) {
  const cleanMatchId = String(matchId || matchFeedItem?.id || '').trim();
  const cleanMatchUrl = String(matchUrl || resolveMatchUrl(matchFeedItem || {})).trim();

  if (!force && cleanMatchId && (await hasMatchDocument(cleanMatchId, { collection }))) {
    return {
      skipped: true,
      reason: 'already_indexed',
      match_id: cleanMatchId,
      source: 'Vector Archive'
    };
  }

  let provider = 'CricAPI';
  let matchPayload = null;
  try {
    if (!cleanMatchId) {
      throw new Error('A match id is required for CricAPI lookup.');
    }
    const scorecardPayload = await getMatchScorecard(cleanMatchId);
    matchPayload = scorecardPayload.match || null;
  } catch (error) {
    if (!isRateLimitedError(error) || !cleanMatchUrl) {
      throw error;
    }
    provider = 'Web Scraper';
    const scraped = await scrapeMatchFromUrl(cleanMatchUrl);
    matchPayload = scraped.match;
  }

  if (!matchPayload?.id) {
    throw new Error('Match payload was empty after live fetch.');
  }

  const narrative = await summarizeMatchNarrative(matchPayload);
  return runLiveSqlVectorSync(matchPayload, narrative, provider);
}

async function ingestCompletedMatch(matchFeedItem = {}, collection = CHROMA_COLLECTION) {
  const result = await ensureMatchIndexed(
    {
      matchId: matchFeedItem.id,
      matchUrl: resolveMatchUrl(matchFeedItem),
      matchFeedItem
    },
    { collection }
  );

  if (result.skipped) {
    return result;
  }

  return result;
}

function buildLiveAlertPayload(match = {}) {
  return {
    type: 'live_snapshot',
    match_id: String(match.id || '').trim(),
    title: String(match.name || 'Live Match Alert').trim(),
    summary: [String(match.status || '').trim(), scoreLine(match.score || [])].filter(Boolean).join(' | '),
    teams: Array.isArray(match.teams) ? match.teams : [],
    status: String(match.status || '').trim(),
    score: Array.isArray(match.score) ? match.score : []
  };
}

async function runDailyIngestorOnce({
  lookbackHours = MATCH_LOOKBACK_HOURS,
  maxMatches = DEFAULT_MATCH_LIMIT,
  collection = CHROMA_COLLECTION,
  onEvent = null
} = {}) {
  if (activeRun) return activeRun;

  activeRun = (async () => {
    // Check if we should skip due to rate limiting
    if (rateLimitBackoffMs > 0) {
      const now = Date.now();
      const timeSinceLastCheck = now - (activeRun?.lastRateLimitCheck || 0);
      if (timeSinceLastCheck < rateLimitBackoffMs) {
        return {
          ran: false,
          skipped: true,
          reason: 'rate_limited',
          retry_after_ms: Math.max(0, rateLimitBackoffMs - timeSinceLastCheck),
          message: `Backing off due to rate limit. Retry after ${Math.ceil((rateLimitBackoffMs - timeSinceLastCheck) / 1000)}s.`
        };
      }
      rateLimitBackoffMs = 0;
      rateLimitRetryCount = 0;
    }

    let liveResult;
    try {
      liveResult = await getLiveScores({
        includeRecent: true,
        limit: Math.max(10, Number(maxMatches || DEFAULT_MATCH_LIMIT) * 3)
      });
    } catch (error) {
      if (isRateLimitedError(error)) {
        // Exponential backoff: 5s, 10s, 20s, 40s, 80s, max 5 minutes
        rateLimitRetryCount++;
        rateLimitBackoffMs = Math.min(
          300_000, // 5 minutes max
          Math.pow(2, Math.min(rateLimitRetryCount - 1, 4)) * 5_000 // 5s * 2^n
        );
        console.warn(
          `[daily-ingestor] rate limited. Retrying in ${Math.ceil(rateLimitBackoffMs / 1000)}s (attempt ${rateLimitRetryCount})`
        );
        return {
          ran: false,
          skipped: true,
          reason: 'rate_limited',
          retry_after_ms: rateLimitBackoffMs,
          message: `CricAPI rate limit hit. Will retry in ${Math.ceil(rateLimitBackoffMs / 1000)}s.`
        };
      }
      throw error;
    }

    const liveItems = Array.isArray(liveResult.items) ? liveResult.items : [];
    const activeLiveMatch = liveItems.find((item) => item.live) || liveItems[0] || null;
    if (activeLiveMatch && typeof onEvent === 'function') {
      onEvent(buildLiveAlertPayload(activeLiveMatch));
    }

    const completed = recentCompletedMatches(liveItems, lookbackHours).slice(
      0,
      Math.max(1, Number(maxMatches) || DEFAULT_MATCH_LIMIT)
    );

    if (!completed.length) {
      return {
        ran: true,
        ingested: 0,
        message: 'No completed matches were found in the last 24 hours.'
      };
    }

    const results = [];
    for (const match of completed) {
      try {
        const result = await ingestCompletedMatch(match, collection);
        results.push(result);
        if (!result.skipped && typeof onEvent === 'function') {
          onEvent({
            type: 'match_ingested',
            match_id: result.match_id,
            title: 'Archive Sync Complete',
            summary: `${result.match_id} synced from ${result.source}.`,
            source: result.source
          });
        }
      } catch (error) {
        results.push({
          match_id: String(match.id || ''),
          narrative_saved: false,
          player_documents: 0,
          error: error?.message || 'ingest_failed'
        });
      }
    }

    return {
      ran: true,
      ingested: results.filter((item) => item.narrative_saved).length,
      results
    };
  })().finally(() => {
    activeRun = null;
  });

  return activeRun;
}

function startDailyIngestor({
  intervalMs = DEFAULT_INTERVAL_MS,
  runOnStart = String(process.env.RUN_DAILY_INGESTOR_ON_BOOT || 'true').trim().toLowerCase() === 'true',
  io = null
} = {}) {
  if (String(process.env.ENABLE_DAILY_INGESTOR || 'true').trim().toLowerCase() === 'false') {
    return null;
  }

  if (ingestorTimer) return ingestorTimer;

  const emitEvent = (payload) => {
    if (!io) return;
    io.emit('live-score-alert', payload);
  };

  ingestorTimer = setInterval(() => {
    runDailyIngestorOnce({ onEvent: emitEvent }).catch((error) => {
      console.error('[daily-ingestor] run failed:', error?.message || error);
    });
  }, Math.max(60_000, Number(intervalMs) || DEFAULT_INTERVAL_MS));

  ingestorTimer.unref?.();

  if (runOnStart) {
    runDailyIngestorOnce({ onEvent: emitEvent }).catch((error) => {
      console.error('[daily-ingestor] initial run failed:', error?.message || error);
    });
  }

  return ingestorTimer;
}

function stopDailyIngestor() {
  if (!ingestorTimer) return;
  clearInterval(ingestorTimer);
  ingestorTimer = null;
}

module.exports = {
  startDailyIngestor,
  stopDailyIngestor,
  runDailyIngestorOnce,
  ensureMatchIndexed
};
