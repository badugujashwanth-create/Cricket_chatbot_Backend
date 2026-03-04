const fs = require('fs');
const path = require('path');
const { execFile } = require('child_process');
const { promisify } = require('util');

const execFileAsync = promisify(execFile);

const BACKEND_DIR = __dirname;
const DB_DIR = path.join(BACKEND_DIR, 'chroma_db');
const MANIFEST_PATH = path.join(BACKEND_DIR, 'chroma_manifest.json');
const QUERY_SCRIPT = path.join(BACKEND_DIR, 'scripts', 'query_chroma_local.py');
const COLLECTION = 'cricket_semantic_index';
const PYTHON_BIN = process.env.PYTHON_BIN || 'python';

let cachedManifest = null;
let manifestMtimeMs = 0;
let lastQueryErrorAt = 0;

function normalizeText(value = '') {
  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function scoreLexicalMatch(query, item) {
  const q = normalizeText(query);
  if (!q) return 0;

  const tokens = q.split(' ').filter(Boolean);
  const title = normalizeText(item.title || '');
  const snippet = normalizeText(item.snippet || '');

  let score = 0;
  if (title.includes(q)) score += 0.6;

  for (const token of tokens) {
    if (token.length < 2) continue;
    if (title.includes(token)) score += 0.18;
    else if (snippet.includes(token)) score += 0.06;
  }

  if (item.type === 'player_profile' && tokens.length >= 2) {
    const last = tokens[tokens.length - 1];
    const first = tokens[0];
    const playerName = normalizeText(item.title.replace(/\(player\)\s*$/, ''));
    const parts = playerName.split(' ').filter(Boolean);
    if (parts.length >= 2) {
      const candFirst = parts[0];
      const candLast = parts[parts.length - 1];
      if (candLast === last) score += 0.5;
      if (candLast === last && candFirst[0] === first[0]) score += 0.45;
      if (candFirst === first && candLast === last) score += 0.7;
    }
  }

  return score;
}

function readManifest() {
  try {
    const stat = fs.statSync(MANIFEST_PATH);
    if (cachedManifest && stat.mtimeMs === manifestMtimeMs) {
      return cachedManifest;
    }

    cachedManifest = JSON.parse(fs.readFileSync(MANIFEST_PATH, 'utf8'));
    manifestMtimeMs = stat.mtimeMs;
    return cachedManifest;
  } catch (_) {
    return null;
  }
}

function getStatus() {
  const manifest = readManifest();
  const ready = Boolean(manifest && fs.existsSync(DB_DIR) && fs.existsSync(QUERY_SCRIPT));
  return {
    ready,
    collection: manifest?.collection || COLLECTION,
    docs: manifest?.collection_count || manifest?.docs_upserted || 0,
    rows: manifest?.rows_processed || 0,
    builtAt: manifest?.built_at || null
  };
}

function buildSemanticItem(row = {}) {
  const metadata = row.metadata || {};
  const type = metadata.doc_type || 'semantic_doc';

  let title = row.id || 'Semantic match';
  if (type === 'player_profile' && metadata.player) {
    title = `${metadata.player} (player)`;
  } else if (type === 'team_summary' && metadata.team) {
    title = `${metadata.team} (team)`;
  } else if (type === 'match_summary' && metadata.match_id) {
    title = `Match ${metadata.match_id}`;
  } else if (type === 'delivery_chunk') {
    const matchId = metadata.match_id ? `Match ${metadata.match_id}` : 'Match context';
    const innings = metadata.inning ? `Innings ${metadata.inning}` : '';
    const team = metadata.batting_team || '';
    title = [matchId, innings, team].filter(Boolean).join(' | ');
  }

  const tags = [];
  if (metadata.team) tags.push(metadata.team);
  if (metadata.batting_team) tags.push(metadata.batting_team);
  if (metadata.match_type) tags.push(metadata.match_type);
  if (metadata.date) tags.push(metadata.date);
  if (metadata.venue) tags.push(metadata.venue);

  return {
    id: row.id,
    type,
    title,
    distance: typeof row.distance === 'number' ? row.distance : null,
    metaLine: tags.slice(0, 3).join(' | '),
    snippet: String(row.document_preview || '').trim()
  };
}

async function querySemantic(query, k = 4) {
  const status = getStatus();
  if (!status.ready) {
    return null;
  }

  try {
    const { stdout } = await execFileAsync(
      PYTHON_BIN,
      [QUERY_SCRIPT, '--query', String(query), '--k', String(Math.max(1, Math.min(10, k)))],
      {
        cwd: BACKEND_DIR,
        timeout: 20000,
        maxBuffer: 10 * 1024 * 1024
      }
    );

    const parsed = JSON.parse(stdout);
    const results = Array.isArray(parsed?.results) ? parsed.results.map(buildSemanticItem) : [];
    results.sort((a, b) => {
      const aBoost = scoreLexicalMatch(query, a);
      const bBoost = scoreLexicalMatch(query, b);
      if (aBoost !== bBoost) return bBoost - aBoost;

      const aDistance = typeof a.distance === 'number' ? a.distance : Number.POSITIVE_INFINITY;
      const bDistance = typeof b.distance === 'number' ? b.distance : Number.POSITIVE_INFINITY;
      return aDistance - bDistance;
    });

    return {
      query: String(query),
      count: results.length,
      collection: status.collection,
      results
    };
  } catch (error) {
    // Keep logs quiet on repeated errors, but preserve debuggability.
    if (Date.now() - lastQueryErrorAt > 30000) {
      console.warn('Chroma semantic query failed:', error.message);
      lastQueryErrorAt = Date.now();
    }
    return null;
  }
}

module.exports = {
  getStatus,
  querySemantic
};
