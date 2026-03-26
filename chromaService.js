const fs = require('fs');
const path = require('path');
const { execFile } = require('child_process');
const { promisify } = require('util');

const execFileAsync = promisify(execFile);

const BACKEND_DIR = __dirname;
const QUERY_SCRIPT = path.join(BACKEND_DIR, 'scripts', 'query_chroma_local.py');
const DEFAULT_COLLECTION = process.env.CHROMA_COLLECTION || 'cricket_semantic_index';
const PYTHON_BIN = process.env.PYTHON_BIN || 'python';
const VECTOR_CACHE_TTL_MS = 5 * 60 * 1000;
const VECTOR_CACHE_LIMIT = 100;
const vectorQueryCache = new Map();

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

function existingPath(targetPath = '') {
  if (!targetPath) return '';
  return fs.existsSync(targetPath) ? targetPath : '';
}

function resolveDbDir() {
  const explicit = existingPath(process.env.CHROMA_DB_DIR || '');
  if (explicit) return explicit;

  const manifestPath = path.join(BACKEND_DIR, 'chroma_manifest.json');
  const dbPath = path.join(BACKEND_DIR, 'chroma_db');
  if (fs.existsSync(dbPath) && fs.existsSync(manifestPath)) {
    return dbPath;
  }
  return fs.existsSync(dbPath) ? dbPath : '';
}

function buildVectorCacheKey(query = '', { k = 5, dbDir = '', collection = DEFAULT_COLLECTION } = {}) {
  return JSON.stringify({
    query: String(query || '').trim().toLowerCase(),
    k: Math.max(1, Number(k) || 5),
    dbDir: String(dbDir || '').trim(),
    collection: String(collection || DEFAULT_COLLECTION).trim()
  });
}

function getCachedVectorQuery(key = '') {
  const cached = vectorQueryCache.get(key);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    vectorQueryCache.delete(key);
    return null;
  }
  vectorQueryCache.delete(key);
  vectorQueryCache.set(key, cached);
  return cached.value;
}

function setCachedVectorQuery(key = '', value) {
  if (!key) return value;
  if (vectorQueryCache.has(key)) {
    vectorQueryCache.delete(key);
  }
  vectorQueryCache.set(key, {
    value,
    expiresAt: Date.now() + VECTOR_CACHE_TTL_MS
  });
  if (vectorQueryCache.size > VECTOR_CACHE_LIMIT) {
    const oldestKey = vectorQueryCache.keys().next().value;
    if (oldestKey) vectorQueryCache.delete(oldestKey);
  }
  return value;
}

async function queryVectorDb(query = '', { k = 5, dbDir = '', collection = DEFAULT_COLLECTION } = {}) {
  const cleanQuery = String(query || '').trim();
  const resolvedDbDir = dbDir || resolveDbDir();
  if (!cleanQuery) {
    return {
      available: false,
      db_dir: resolvedDbDir,
      collection,
      query: cleanQuery,
      results: [],
      warning: 'empty_query'
    };
  }

  if (!resolvedDbDir) {
    return {
      available: false,
      db_dir: '',
      collection,
      query: cleanQuery,
      results: [],
      warning: 'vector_db_missing'
    };
  }

  const cacheKey = buildVectorCacheKey(cleanQuery, {
    k,
    dbDir: resolvedDbDir,
    collection
  });
  const cached = getCachedVectorQuery(cacheKey);
  if (cached) return cached;

  try {
    const { stdout } = await execFileAsync(
      PYTHON_BIN,
      [
        QUERY_SCRIPT,
        '--db-dir',
        resolvedDbDir,
        '--collection',
        collection,
        '--query',
        cleanQuery,
        '--k',
        String(Math.max(1, Number(k) || 5))
      ],
      {
        cwd: BACKEND_DIR,
        timeout: 60000,
        maxBuffer: 8 * 1024 * 1024
      }
    );

    const payload = parseJsonText(stdout);
    return setCachedVectorQuery(cacheKey, {
      available: true,
      db_dir: resolvedDbDir,
      collection,
      query: cleanQuery,
      results: Array.isArray(payload?.results) ? payload.results : [],
      warning: String(payload?.warning || '').trim()
    });
  } catch (error) {
    return {
      available: false,
      db_dir: resolvedDbDir,
      collection,
      query: cleanQuery,
      results: [],
      warning: error?.message || 'vector_query_failed'
    };
  }
}

module.exports = {
  queryVectorDb,
  resolveDbDir
};
