const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { execFile } = require('child_process');
const { promisify } = require('util');

const execFileAsync = promisify(execFile);

const BACKEND_DIR = __dirname;
const QUERY_SCRIPT = path.join(BACKEND_DIR, 'scripts', 'query_chroma_local.py');
const SEMANTIC_CACHE_SCRIPT = path.join(BACKEND_DIR, 'scripts', 'semantic_cache_local.py');
const ADMIN_SCRIPT = path.join(BACKEND_DIR, 'scripts', 'chroma_collection_local.py');
const DEFAULT_COLLECTION = process.env.CHROMA_COLLECTION || 'cricket_semantic_index';
const DEFAULT_SEMANTIC_CACHE_COLLECTION =
  process.env.SEMANTIC_CACHE_COLLECTION || 'semantic_cache';
const DEFAULT_SEMANTIC_CACHE_DISTANCE =
  Number(process.env.SEMANTIC_CACHE_DISTANCE_THRESHOLD || 0.05);
const PYTHON_BIN = process.env.PYTHON_BIN || 'python';
const VECTOR_CACHE_TTL_MS = 5 * 60 * 1000;
const VECTOR_CACHE_LIMIT = 100;
const vectorQueryCache = new Map();
const SEMANTIC_CACHE_STOP_WORDS = new Set([
  'a',
  'an',
  'and',
  'are',
  'for',
  'how',
  'i',
  'in',
  'is',
  'me',
  'of',
  'please',
  'show',
  'tell',
  'the',
  'to',
  'what',
  'whats',
  'who'
]);

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

function ensureDbDir() {
  const resolved = resolveDbDir() || path.join(BACKEND_DIR, 'chroma_db');
  fs.mkdirSync(resolved, { recursive: true });
  return resolved;
}

function buildSemanticCacheDocument(question = '') {
  const normalized = String(question || '')
    .toLowerCase()
    .replace(/['’]s\b/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  const tokens = normalized
    .split(' ')
    .map((token) => {
      if (token === 'highest' || token === 'high' || token === 'max' || token === 'maximum') {
        return 'highest';
      }
      if (token === 'score' || token === 'scores') {
        return 'score';
      }
      return token;
    })
    .filter((token) => token && token.length > 1 && !SEMANTIC_CACHE_STOP_WORDS.has(token));

  return [...new Set(tokens)].sort((left, right) => left.localeCompare(right)).join(' ');
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

async function getCollectionDocs(
  where = {},
  { limit = 100, dbDir = '', collection = DEFAULT_COLLECTION } = {}
) {
  const resolvedDbDir = dbDir || resolveDbDir();
  if (!resolvedDbDir) {
    return {
      available: false,
      db_dir: '',
      collection,
      docs: [],
      warning: 'vector_db_missing'
    };
  }

  try {
    const { stdout } = await execFileAsync(
      PYTHON_BIN,
      [
        ADMIN_SCRIPT,
        'get',
        '--db-dir',
        resolvedDbDir,
        '--collection',
        collection,
        '--where-json',
        JSON.stringify(where && typeof where === 'object' ? where : {}),
        '--limit',
        String(Math.max(1, Number(limit) || 100))
      ],
      {
        cwd: BACKEND_DIR,
        timeout: 60000,
        maxBuffer: 32 * 1024 * 1024
      }
    );

    const payload = parseJsonText(stdout) || {};
    const ids = Array.isArray(payload.ids) ? payload.ids : [];
    const documents = Array.isArray(payload.documents) ? payload.documents : [];
    const metadatas = Array.isArray(payload.metadatas) ? payload.metadatas : [];

    return {
      available: true,
      db_dir: resolvedDbDir,
      collection,
      docs: ids.map((id, index) => ({
        id: String(id || '').trim(),
        document: String(documents[index] || ''),
        metadata: metadatas[index] && typeof metadatas[index] === 'object' ? metadatas[index] : {}
      }))
    };
  } catch (error) {
    return {
      available: false,
      db_dir: resolvedDbDir,
      collection,
      docs: [],
      warning: error?.message || 'collection_get_failed'
    };
  }
}

function readChromaManifest() {
  const manifestPath = path.join(BACKEND_DIR, 'chroma_manifest.json');
  if (!fs.existsSync(manifestPath)) return null;
  try {
    return JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  } catch (_) {
    return null;
  }
}

async function querySemanticCache(
  question = '',
  {
    k = 1,
    dbDir = '',
    collection = DEFAULT_SEMANTIC_CACHE_COLLECTION,
    maxDistance = DEFAULT_SEMANTIC_CACHE_DISTANCE
  } = {}
) {
  const cleanQuestion = String(question || '').trim();
  const cacheDocument = buildSemanticCacheDocument(cleanQuestion);
  if (!cleanQuestion) {
    return {
      hit: false,
      question: cleanQuestion,
      collection,
      results: []
    };
  }

  const resolvedDbDir = dbDir || ensureDbDir();
  try {
    const { stdout } = await execFileAsync(
      PYTHON_BIN,
      [
        SEMANTIC_CACHE_SCRIPT,
        'query',
        '--db-dir',
        resolvedDbDir,
        '--collection',
        collection,
        '--question',
        cacheDocument || cleanQuestion,
        '--k',
        String(Math.max(1, Number(k) || 1))
      ],
      {
        cwd: BACKEND_DIR,
        timeout: 30000,
        maxBuffer: 8 * 1024 * 1024
      }
    );

    const payload = parseJsonText(stdout) || {};
    const results = Array.isArray(payload.results) ? payload.results : [];
    const first = results[0] || null;
    const distance = Number(first?.distance);
    const metadata = first?.metadata && typeof first.metadata === 'object' ? first.metadata : {};
    const response = parseJsonText(metadata.response_json || '') || null;
    const uiPayload = parseJsonText(metadata.ui_payload_json || '') || {};
    const answerText =
      String(metadata.answer_text || '').trim() ||
      String(response?.summary || response?.answer || '').trim();

    return {
      hit: Boolean(
        first &&
          Number.isFinite(distance) &&
          distance < Number(maxDistance) &&
          (answerText || response)
      ),
      question: cleanQuestion,
      cache_document: cacheDocument,
      collection,
      distance: Number.isFinite(distance) ? distance : null,
      answer_text: answerText,
      ui_payload: uiPayload,
      response,
      results
    };
  } catch (error) {
    return {
      hit: false,
      question: cleanQuestion,
      collection,
      results: [],
      warning: error?.message || 'semantic_cache_query_failed'
    };
  }
}

function buildSemanticCacheId(question = '') {
  const normalized = String(question || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
  return `semantic-cache-${crypto.createHash('sha1').update(normalized).digest('hex').slice(0, 24)}`;
}

async function writeSemanticCacheInput(payload = {}) {
  const tempPath = path.join(
    os.tmpdir(),
    `semantic-cache-${Date.now()}-${Math.random().toString(16).slice(2)}.json`
  );
  await fs.promises.writeFile(tempPath, JSON.stringify(payload), 'utf8');
  return tempPath;
}

async function saveSemanticCacheEntry(
  {
    question = '',
    response = {},
    uiPayload = null,
    answerText = ''
  } = {},
  {
    dbDir = '',
    collection = DEFAULT_SEMANTIC_CACHE_COLLECTION
  } = {}
) {
  const cleanQuestion = String(question || '').trim();
  const cacheDocument = buildSemanticCacheDocument(cleanQuestion);
  const cleanAnswer =
    String(answerText || response?.summary || response?.answer || '').trim();
  if (!cleanQuestion || !cleanAnswer) {
    return {
      saved: false,
      reason: 'missing_question_or_answer'
    };
  }

  const resolvedDbDir = dbDir || ensureDbDir();
  const inputPath = await writeSemanticCacheInput({
    id: buildSemanticCacheId(cleanQuestion),
    question: cleanQuestion,
    document_text: cacheDocument || cleanQuestion,
    answer_text: cleanAnswer,
    response,
    ui_payload:
      uiPayload && typeof uiPayload === 'object'
        ? uiPayload
        : response?.data && typeof response.data === 'object'
          ? response.data
          : {}
  });

  try {
    const { stdout } = await execFileAsync(
      PYTHON_BIN,
      [
        SEMANTIC_CACHE_SCRIPT,
        'upsert',
        '--db-dir',
        resolvedDbDir,
        '--collection',
        collection,
        '--input',
        inputPath
      ],
      {
        cwd: BACKEND_DIR,
        timeout: 30000,
        maxBuffer: 8 * 1024 * 1024
      }
    );

    const payload = parseJsonText(stdout) || {};
    return {
      saved: Boolean(payload.ok),
      id: String(payload.id || '').trim()
    };
  } catch (error) {
    return {
      saved: false,
      reason: error?.message || 'semantic_cache_upsert_failed'
    };
  } finally {
    fs.promises.unlink(inputPath).catch(() => {});
  }
}

module.exports = {
  queryVectorDb,
  resolveDbDir,
  getCollectionDocs,
  readChromaManifest,
  querySemanticCache,
  saveSemanticCacheEntry
};
