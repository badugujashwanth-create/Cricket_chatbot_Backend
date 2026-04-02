require('../loadEnv');

const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');
const { promisify } = require('util');

const { getLiveScores, getMatchScorecard } = require('../cricApiService');
const { callLlama } = require('../llamaClient');
const { slugify } = require('../textUtils');

const execFileAsync = promisify(execFile);

const BACKEND_DIR = path.join(__dirname, '..');
const CHROMA_ADMIN_SCRIPT = path.join(BACKEND_DIR, 'scripts', 'chroma_collection_local.py');
const PYTHON_BIN = process.env.PYTHON_BIN || 'python';
const CHROMA_COLLECTION = process.env.CHROMA_COLLECTION || 'cricket_semantic_index';
const DEFAULT_INTERVAL_MS = 24 * 60 * 60 * 1000;
const MATCH_LOOKBACK_HOURS = Number(process.env.INGEST_LOOKBACK_HOURS || 24);
const DEFAULT_MATCH_LIMIT = Math.max(1, Number(process.env.INGEST_MATCH_LIMIT || 10));

let ingestorTimer = null;
let activeRun = null;

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

function toIsoDate(value = '') {
  const parsed = dateValue(value);
  if (parsed === null) return String(value || '').trim();
  return new Date(parsed).toISOString().slice(0, 10);
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

function stableMatchDocId(match = {}) {
  return `match-completed-${String(match.id || '').trim()}`;
}

function stablePlayerDocId(playerName = '') {
  return `player-stats-${slugify(playerName || 'unknown-player')}`;
}

function buildTempInputPath(prefix = 'chroma-admin') {
  return path.join(
    os.tmpdir(),
    `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}.json`
  );
}

async function writeTempJson(prefix = 'chroma-admin', payload = {}) {
  const filePath = buildTempInputPath(prefix);
  await fs.promises.writeFile(filePath, JSON.stringify(payload), 'utf8');
  return filePath;
}

async function chromaGet(where = {}, limit = 1, collection = CHROMA_COLLECTION) {
  const { stdout } = await execFileAsync(
    PYTHON_BIN,
    [
      CHROMA_ADMIN_SCRIPT,
      'get',
      '--collection',
      collection,
      '--where-json',
      JSON.stringify(where),
      '--limit',
      String(Math.max(1, Number(limit) || 1))
    ],
    {
      cwd: BACKEND_DIR,
      timeout: 30000,
      maxBuffer: 8 * 1024 * 1024
    }
  );

  const payload = parseJsonText(stdout) || {};
  const ids = Array.isArray(payload.ids) ? payload.ids : [];
  const documents = Array.isArray(payload.documents) ? payload.documents : [];
  const metadatas = Array.isArray(payload.metadatas) ? payload.metadatas : [];
  return ids.map((id, index) => ({
    id,
    document: documents[index] || '',
    metadata: metadatas[index] && typeof metadatas[index] === 'object' ? metadatas[index] : {}
  }));
}

async function chromaUpsert(documents = [], collection = CHROMA_COLLECTION) {
  if (!Array.isArray(documents) || !documents.length) {
    return {
      ok: true,
      count: 0
    };
  }

  const inputPath = await writeTempJson('chroma-upsert', { documents });
  try {
    const { stdout } = await execFileAsync(
      PYTHON_BIN,
      [
        CHROMA_ADMIN_SCRIPT,
        'upsert',
        '--collection',
        collection,
        '--input',
        inputPath
      ],
      {
        cwd: BACKEND_DIR,
        timeout: 60000,
        maxBuffer: 8 * 1024 * 1024
      }
    );

    return parseJsonText(stdout) || { ok: false, count: 0 };
  } finally {
    fs.promises.unlink(inputPath).catch(() => {});
  }
}

function recentCompletedMatches(items = [], lookbackHours = MATCH_LOOKBACK_HOURS) {
  const cutoff = Date.now() - Math.max(1, Number(lookbackHours) || MATCH_LOOKBACK_HOURS) * 60 * 60 * 1000;
  return (Array.isArray(items) ? items : []).filter((item) => {
    if (!item?.match_ended) return false;
    const timestamp = dateValue(item.date_time_gmt || item.date || '');
    return timestamp !== null && timestamp >= cutoff;
  });
}

function buildFallbackNarrative(match = {}) {
  const innings = Array.isArray(match.scorecard) ? match.scorecard : [];
  const topBatters = innings
    .flatMap((inning) => Array.isArray(inning.batting) ? inning.batting : [])
    .sort((left, right) => Number(right.runs || 0) - Number(left.runs || 0))
    .slice(0, 3)
    .map((row) => `${row.batsman?.name || 'Unknown'} (${row.runs || 0})`);
  const topBowlers = innings
    .flatMap((inning) => Array.isArray(inning.bowling) ? inning.bowling : [])
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

function getOrCreatePlayerDelta(map, name = '') {
  const cleanName = String(name || '').trim();
  if (!cleanName) return null;
  if (!map.has(cleanName)) {
    map.set(cleanName, {
      player_name: cleanName,
      runs_added: 0,
      wickets_added: 0,
      appearances: 0
    });
  }
  return map.get(cleanName);
}

function extractPlayerDeltas(match = {}) {
  const deltas = new Map();
  const seenAppearance = new Set();
  const innings = Array.isArray(match.scorecard) ? match.scorecard : [];

  innings.forEach((inning) => {
    (Array.isArray(inning.batting) ? inning.batting : []).forEach((row) => {
      const name = String(row.batsman?.name || '').trim();
      const delta = getOrCreatePlayerDelta(deltas, name);
      if (!delta) return;
      delta.runs_added += Number(row.runs || 0);
      if (!seenAppearance.has(name)) {
        seenAppearance.add(name);
        delta.appearances += 1;
      }
    });

    (Array.isArray(inning.bowling) ? inning.bowling : []).forEach((row) => {
      const name = String(row.bowler?.name || '').trim();
      const delta = getOrCreatePlayerDelta(deltas, name);
      if (!delta) return;
      delta.wickets_added += Number(row.wickets || 0);
      if (!seenAppearance.has(name)) {
        seenAppearance.add(name);
        delta.appearances += 1;
      }
    });
  });

  return [...deltas.values()];
}

function extractStructuredStats(documentText = '') {
  const marker = 'STRUCTURED_STATS_JSON:';
  const index = String(documentText || '').lastIndexOf(marker);
  if (index < 0) return null;
  const raw = String(documentText || '').slice(index + marker.length).trim();
  return parseJsonText(raw);
}

function normalizeRole(role = '') {
  const clean = String(role || '').trim().toLowerCase();
  if (!clean) return 'batter';
  if (clean.includes('all')) return 'all-rounder';
  if (clean.includes('bowl')) return 'bowler';
  return 'batter';
}

function inferRole(current = {}, delta = {}) {
  const existingRole = normalizeRole(current.role || '');
  if (current.role) return existingRole;
  if (Number(delta.wickets_added || 0) > 0 && Number(delta.runs_added || 0) > 0) return 'all-rounder';
  if (Number(delta.wickets_added || 0) > 0) return 'bowler';
  return 'batter';
}

async function readBasePlayerState(playerName = '', collection = CHROMA_COLLECTION) {
  const currentDocs = await chromaGet(
    {
      doc_type: 'player_stats',
      player_name: String(playerName || '').trim()
    },
    1,
    collection
  );

  const currentDoc = currentDocs[0] || null;
  if (currentDoc) {
    const structured = extractStructuredStats(currentDoc.document) || {};
    return {
      id: currentDoc.id || stablePlayerDocId(playerName),
      role: normalizeRole(currentDoc.metadata?.role || structured.role || ''),
      matches: Number(structured.matches || 0),
      runs: Number(structured.runs || 0),
      wickets: Number(structured.wickets || 0),
      ingested_match_ids: Array.isArray(structured.ingested_match_ids) ? structured.ingested_match_ids : [],
      recent_updates: Array.isArray(structured.recent_updates) ? structured.recent_updates : []
    };
  }

  const profileDocs = await chromaGet(
    {
      doc_type: 'player_profile',
      player: String(playerName || '').trim()
    },
    1,
    collection
  );

  const profile = profileDocs[0] || null;
  return {
    id: stablePlayerDocId(playerName),
    role: normalizeRole(profile?.metadata?.role || ''),
    matches: Number(profile?.metadata?.matches || 0),
    runs: Number(profile?.metadata?.runs || 0),
    wickets: Number(profile?.metadata?.wickets || 0),
    ingested_match_ids: [],
    recent_updates: []
  };
}

function buildPlayerStatsDocument(playerName = '', role = 'batter', state = {}) {
  const updates = (Array.isArray(state.recent_updates) ? state.recent_updates : []).slice(0, 5);
  const lines = [
    `Player statistics profile for ${playerName}.`,
    `Role: ${role}.`,
    `Matches: ${Number(state.matches || 0)}.`,
    `Total Runs: ${Number(state.runs || 0)}.`,
    `Total Wickets: ${Number(state.wickets || 0)}.`
  ];

  if (updates.length) {
    lines.push(
      `Recent live updates: ${updates
        .map(
          (entry) =>
            `${entry.date}: ${entry.match_name} (+${entry.runs_added} runs, +${entry.wickets_added} wickets)`
        )
        .join('; ')}.`
    );
  }

  lines.push(
    `STRUCTURED_STATS_JSON: ${JSON.stringify(
      {
        role,
        matches: Number(state.matches || 0),
        runs: Number(state.runs || 0),
        wickets: Number(state.wickets || 0),
        ingested_match_ids: Array.isArray(state.ingested_match_ids) ? state.ingested_match_ids : [],
        recent_updates: updates
      },
      null,
      0
    )}`
  );

  return lines.join(' ');
}

function buildMatchNarrativeDocument(match = {}, narrative = '') {
  return [
    `Completed match: ${match.name || 'Unknown match'}.`,
    match.status ? `Result: ${match.status}.` : '',
    match.venue ? `Venue: ${match.venue}.` : '',
    match.score?.length ? `Score summary: ${scoreLine(match.score)}.` : '',
    String(narrative || '').trim()
  ]
    .filter(Boolean)
    .join(' ');
}

async function ingestCompletedMatch(matchFeedItem = {}, playerStateCache = new Map(), collection = CHROMA_COLLECTION) {
  const scorecardPayload = await getMatchScorecard(matchFeedItem.id);
  const match = scorecardPayload.match || {};
  const narrative = await summarizeMatchNarrative(match);
  const docs = [
    {
      id: stableMatchDocId(match),
      document: buildMatchNarrativeDocument(match, narrative),
      metadata: {
        doc_type: 'match',
        status: 'completed',
        date: toIsoDate(match.date_time_gmt || match.date || '')
      }
    }
  ];

  const deltas = extractPlayerDeltas(match);
  for (const delta of deltas) {
    const cached = playerStateCache.get(delta.player_name);
    const baseState = cached || (await readBasePlayerState(delta.player_name, collection));
    if (Array.isArray(baseState.ingested_match_ids) && baseState.ingested_match_ids.includes(match.id)) {
      continue;
    }

    const nextRole = inferRole(baseState, delta);
    const recentUpdates = [
      {
        match_id: match.id,
        date: toIsoDate(match.date_time_gmt || match.date || ''),
        match_name: match.name || 'Completed match',
        runs_added: Number(delta.runs_added || 0),
        wickets_added: Number(delta.wickets_added || 0)
      },
      ...(Array.isArray(baseState.recent_updates) ? baseState.recent_updates : [])
    ].slice(0, 10);

    const nextState = {
      id: baseState.id || stablePlayerDocId(delta.player_name),
      role: nextRole,
      matches: Number(baseState.matches || 0) + Number(delta.appearances || 0),
      runs: Number(baseState.runs || 0) + Number(delta.runs_added || 0),
      wickets: Number(baseState.wickets || 0) + Number(delta.wickets_added || 0),
      ingested_match_ids: [
        ...(Array.isArray(baseState.ingested_match_ids) ? baseState.ingested_match_ids : []),
        match.id
      ].slice(-40),
      recent_updates: recentUpdates
    };

    playerStateCache.set(delta.player_name, nextState);
    docs.push({
      id: nextState.id,
      document: buildPlayerStatsDocument(delta.player_name, nextRole, nextState),
      metadata: {
        doc_type: 'player_stats',
        player_name: delta.player_name,
        role: nextRole
      }
    });
  }

  await chromaUpsert(docs, collection);
  return {
    match_id: match.id,
    narrative_saved: true,
    player_documents: docs.length - 1
  };
}

async function runDailyIngestorOnce({
  lookbackHours = MATCH_LOOKBACK_HOURS,
  maxMatches = DEFAULT_MATCH_LIMIT,
  collection = CHROMA_COLLECTION
} = {}) {
  if (activeRun) return activeRun;

  activeRun = (async () => {
    const liveResult = await getLiveScores({
      includeRecent: true,
      limit: Math.max(10, Number(maxMatches || DEFAULT_MATCH_LIMIT) * 3)
    });

    const completed = recentCompletedMatches(liveResult.items || [], lookbackHours).slice(
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

    const playerStateCache = new Map();
    const results = [];
    for (const match of completed) {
      try {
        results.push(await ingestCompletedMatch(match, playerStateCache, collection));
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
  runOnStart = String(process.env.RUN_DAILY_INGESTOR_ON_BOOT || '').trim().toLowerCase() === 'true'
} = {}) {
  if (String(process.env.ENABLE_DAILY_INGESTOR || 'true').trim().toLowerCase() === 'false') {
    return null;
  }

  if (ingestorTimer) return ingestorTimer;

  ingestorTimer = setInterval(() => {
    runDailyIngestorOnce().catch((error) => {
      console.error('[daily-ingestor] run failed:', error?.message || error);
    });
  }, Math.max(60_000, Number(intervalMs) || DEFAULT_INTERVAL_MS));

  ingestorTimer.unref?.();

  if (runOnStart) {
    runDailyIngestorOnce().catch((error) => {
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
  runDailyIngestorOnce
};
