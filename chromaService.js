const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { execFile } = require('child_process');
const { promisify } = require('util');
const { ChromaClient } = require('chromadb');
const { normalizeText, similarityScore, tokenize } = require('./textUtils');

const execFileAsync = promisify(execFile);

const BACKEND_DIR = __dirname;
const SCRIPTS_DIR = path.join(BACKEND_DIR, 'scripts');
const QUERY_LOCAL_SCRIPT = path.join(SCRIPTS_DIR, 'query_chroma_local.py');
const COLLECTION_LOCAL_SCRIPT = path.join(SCRIPTS_DIR, 'chroma_collection_local.py');
const SEMANTIC_CACHE_LOCAL_SCRIPT = path.join(SCRIPTS_DIR, 'semantic_cache_local.py');
const DEFAULT_COLLECTION = process.env.CHROMA_COLLECTION || 'cricket_semantic_index';
const DEFAULT_SEMANTIC_CACHE_COLLECTION =
  process.env.SEMANTIC_CACHE_COLLECTION || 'semantic_cache';
const DEFAULT_SEMANTIC_CACHE_DISTANCE =
  Number(process.env.SEMANTIC_CACHE_DISTANCE_THRESHOLD || 0.05);
const VECTOR_CACHE_TTL_MS = 5 * 60 * 1000;
const VECTOR_CACHE_LIMIT = 100;
const COLLECTION_GET_BATCH_LIMIT = 10000;
const LOCAL_HELPER_TIMEOUT_MS = Number(process.env.CHROMA_HELPER_TIMEOUT_MS || 30000);
const EXPLICIT_CHROMA_MODE = normalizeText(process.env.CHROMA_MODE || 'auto') || 'auto';
const PYTHON_BIN = String(process.env.CHROMA_PYTHON_BIN || process.env.PYTHON_BIN || '').trim();
const CHROMA_DEBUG =
  String(process.env.CHROMA_DEBUG || '').trim().toLowerCase() === 'true' ||
  String(process.env.NODE_ENV || '').trim() !== 'production';

const vectorQueryCache = new Map();
const collectionCache = new Map();
let chromaClientPromise = null;

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
const SEMANTIC_CACHE_MIN_QUESTION_SIMILARITY = Number(
  process.env.SEMANTIC_CACHE_MIN_QUESTION_SIMILARITY || 0.92
);
const SEMANTIC_CACHE_MIN_TOKEN_OVERLAP = Number(
  process.env.SEMANTIC_CACHE_MIN_TOKEN_OVERLAP || 0.75
);

function logChromaDebug(message = '', data = null) {
  if (!CHROMA_DEBUG) return;
  if (data === null || data === undefined) {
    console.log(`[chroma] ${message}`);
    return;
  }
  console.log(`[chroma] ${message}`, data);
}

function existingPath(targetPath = '') {
  if (!targetPath) return '';
  return fs.existsSync(targetPath) ? targetPath : '';
}

function parseJsonText(raw = '') {
  const text = String(raw || '').trim();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (_) {
    const firstBrace = text.indexOf('{');
    const lastBrace = text.lastIndexOf('}');
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      try {
        return JSON.parse(text.slice(firstBrace, lastBrace + 1));
      } catch (_) {
        return null;
      }
    }
  }
  return null;
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

function readChromaManifest() {
  const manifestPath = path.join(BACKEND_DIR, 'chroma_manifest.json');
  if (!fs.existsSync(manifestPath)) return null;
  try {
    return JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  } catch (_) {
    return null;
  }
}

function hasExplicitRemoteConfig() {
  return Boolean(
    String(process.env.CHROMA_URL || '').trim() ||
      String(process.env.CHROMA_PATH || '').trim() ||
      String(process.env.CHROMA_HOST || '').trim() ||
      String(process.env.CHROMA_PORT || '').trim()
  );
}

function localHelperScriptsAvailable() {
  return [QUERY_LOCAL_SCRIPT, COLLECTION_LOCAL_SCRIPT, SEMANTIC_CACHE_LOCAL_SCRIPT].every((scriptPath) =>
    fs.existsSync(scriptPath)
  );
}

function resolveChromaMode({ dbDir = resolveDbDir() } = {}) {
  if (EXPLICIT_CHROMA_MODE === 'local') return 'local';
  if (EXPLICIT_CHROMA_MODE === 'server') return 'server';
  if (dbDir && localHelperScriptsAvailable()) return 'local';
  if (hasExplicitRemoteConfig()) return 'server';
  return dbDir ? 'local' : 'server';
}

function resolveChromaClientArgs() {
  const explicitUrl = String(process.env.CHROMA_URL || process.env.CHROMA_PATH || '').trim();
  if (explicitUrl) {
    try {
      const parsed = new URL(explicitUrl);
      return {
        host: parsed.hostname,
        port: Number(parsed.port || (parsed.protocol === 'https:' ? 443 : 8000)),
        ssl: parsed.protocol === 'https:'
      };
    } catch (_) {
      return { path: explicitUrl };
    }
  }

  const host = String(process.env.CHROMA_HOST || '127.0.0.1').trim();
  const port = Number(process.env.CHROMA_PORT || 8000);
  const ssl = String(process.env.CHROMA_SSL || '').trim().toLowerCase() === 'true';
  return {
    host,
    port: Number.isFinite(port) ? port : 8000,
    ssl
  };
}

function pythonCommandAttempts(scriptPath = '', args = []) {
  const attempts = [];
  if (PYTHON_BIN) {
    attempts.push({
      command: PYTHON_BIN,
      args: [scriptPath, ...args]
    });
    return attempts;
  }

  attempts.push({ command: 'python', args: [scriptPath, ...args] });
  if (process.platform === 'win32') {
    attempts.push({ command: 'py', args: ['-3', scriptPath, ...args] });
  }
  return attempts;
}

async function runPythonJson(scriptPath = '', args = []) {
  let lastError = null;
  for (const attempt of pythonCommandAttempts(scriptPath, args)) {
    try {
      const { stdout, stderr } = await execFileAsync(attempt.command, attempt.args, {
        cwd: BACKEND_DIR,
        timeout: LOCAL_HELPER_TIMEOUT_MS,
        windowsHide: true,
        maxBuffer: 8 * 1024 * 1024
      });

      const parsed = parseJsonText(stdout);
      if (parsed) {
        return parsed;
      }

      const stderrParsed = parseJsonText(stderr);
      if (stderrParsed) {
        throw new Error(stderrParsed.error || JSON.stringify(stderrParsed));
      }

      if (String(stderr || '').trim()) {
        throw new Error(String(stderr || '').trim());
      }

      throw new Error('Chroma helper returned invalid JSON.');
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error('Unable to execute local Chroma helper.');
}

function buildSemanticCacheDocument(question = '') {
  const normalized = String(question || '')
    .toLowerCase()
    .replace(/['â€™]s\b/g, '')
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

function computeTokenOverlap(left = '', right = '') {
  const leftTokens = [...new Set(tokenize(left))];
  const rightTokens = new Set(tokenize(right));
  if (!leftTokens.length || !rightTokens.size) return 0;
  const overlap = leftTokens.filter((token) => rightTokens.has(token)).length;
  return overlap / leftTokens.length;
}

function semanticCacheQuestionsAreCompatible(
  inputQuestion = '',
  cachedQuestion = '',
  inputDocument = '',
  cachedDocument = ''
) {
  const normalizedInputQuestion = normalizeText(inputQuestion);
  const normalizedCachedQuestion = normalizeText(cachedQuestion);
  const normalizedInputDocument = normalizeText(inputDocument);
  const normalizedCachedDocument = normalizeText(cachedDocument);

  if (!normalizedInputQuestion || !normalizedCachedQuestion) {
    return false;
  }

  if (
    normalizedInputDocument &&
    normalizedCachedDocument &&
    normalizedInputDocument === normalizedCachedDocument
  ) {
    return true;
  }

  const questionSimilarity = similarityScore(normalizedInputQuestion, normalizedCachedQuestion);
  const questionOverlap = computeTokenOverlap(normalizedInputQuestion, normalizedCachedQuestion);
  const documentOverlap = computeTokenOverlap(normalizedInputDocument, normalizedCachedDocument);

  return Boolean(
    questionSimilarity >= SEMANTIC_CACHE_MIN_QUESTION_SIMILARITY &&
      Math.max(questionOverlap, documentOverlap) >= SEMANTIC_CACHE_MIN_TOKEN_OVERLAP
  );
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

function clearVectorQueryCache() {
  vectorQueryCache.clear();
}

function clearCollectionCache() {
  collectionCache.clear();
}

function normalizeServerQueryResults(payload = {}) {
  const ids = Array.isArray(payload.ids?.[0]) ? payload.ids[0] : [];
  const documents = Array.isArray(payload.documents?.[0]) ? payload.documents[0] : [];
  const metadatas = Array.isArray(payload.metadatas?.[0]) ? payload.metadatas[0] : [];
  const distances = Array.isArray(payload.distances?.[0]) ? payload.distances[0] : [];

  return ids.map((id, index) => {
    const document = String(documents[index] || '');
    return {
      id: String(id || '').trim(),
      distance: Number.isFinite(Number(distances[index])) ? Number(distances[index]) : null,
      document,
      document_preview: document.length > 800 ? `${document.slice(0, 797).trim()}...` : document,
      metadata: metadatas[index] && typeof metadatas[index] === 'object' ? metadatas[index] : {}
    };
  });
}

function normalizeLocalQueryResults(payload = {}) {
  const rows = Array.isArray(payload.results) ? payload.results : [];
  return rows.map((row) => {
    const document = String(row.document || row.document_preview || '').trim();
    return {
      id: String(row.id || '').trim(),
      distance: Number.isFinite(Number(row.distance)) ? Number(row.distance) : null,
      document,
      document_preview:
        document.length > 800 ? `${document.slice(0, 797).trim()}...` : document,
      metadata: row.metadata && typeof row.metadata === 'object' ? row.metadata : {}
    };
  });
}

function normalizeLocalCollectionDocs(payload = {}) {
  const ids = Array.isArray(payload.ids) ? payload.ids : [];
  const documents = Array.isArray(payload.documents) ? payload.documents : [];
  const metadatas = Array.isArray(payload.metadatas) ? payload.metadatas : [];

  return ids.map((id, index) => ({
    id: String(id || '').trim(),
    document: String(documents[index] || ''),
    metadata: metadatas[index] && typeof metadatas[index] === 'object' ? metadatas[index] : {}
  }));
}

function normalizeSemanticCacheResults(payload = {}) {
  const rows = Array.isArray(payload.results) ? payload.results : [];
  return rows.map((row) => ({
    id: String(row.id || '').trim(),
    distance: Number.isFinite(Number(row.distance)) ? Number(row.distance) : null,
    document: String(row.document || '').trim(),
    document_preview: String(row.document || '').trim().slice(0, 800),
    metadata: row.metadata && typeof row.metadata === 'object' ? row.metadata : {}
  }));
}

async function getChromaClient() {
  if (!chromaClientPromise) {
    const clientArgs = resolveChromaClientArgs();
    chromaClientPromise = (async () => {
      const client = new ChromaClient(clientArgs);
      await client.heartbeat();
      return client;
    })().catch((error) => {
      chromaClientPromise = null;
      throw error;
    });
  }
  return chromaClientPromise;
}

async function getServerCollectionHandle(collectionName = DEFAULT_COLLECTION, { create = false } = {}) {
  const cacheKey = `${create ? 'create' : 'get'}:${collectionName}`;
  if (collectionCache.has(cacheKey)) return collectionCache.get(cacheKey);

  const promise = (async () => {
    const client = await getChromaClient();
    if (create) {
      return client.getOrCreateCollection({ name: collectionName });
    }
    return client.getCollection({ name: collectionName });
  })().catch((error) => {
    collectionCache.delete(cacheKey);
    throw error;
  });

  collectionCache.set(cacheKey, promise);
  return promise;
}

async function queryVectorDbLocal(query = '', { k = 5, dbDir = '', collection = DEFAULT_COLLECTION } = {}) {
  const cleanQuery = String(query || '').trim();
  const resolvedDbDir = dbDir || resolveDbDir();
  const payload = await runPythonJson(QUERY_LOCAL_SCRIPT, [
    '--db-dir',
    resolvedDbDir,
    '--collection',
    collection,
    '--query',
    cleanQuery,
    '--k',
    String(Math.max(1, Number(k) || 5))
  ]);

  return {
    available: !String(payload.warning || '').toLowerCase().includes('collection_unavailable'),
    db_dir: resolvedDbDir,
    collection,
    query: cleanQuery,
    results: normalizeLocalQueryResults(payload),
    warning: String(payload.warning || '').trim()
  };
}

async function queryVectorDbServer(query = '', { k = 5, dbDir = '', collection = DEFAULT_COLLECTION } = {}) {
  const cleanQuery = String(query || '').trim();
  const resolvedDbDir = dbDir || resolveDbDir();
  const collectionHandle = await getServerCollectionHandle(collection);
  const payload = await collectionHandle.query({
    queryTexts: [cleanQuery],
    nResults: Math.max(1, Number(k) || 5),
    include: ['documents', 'metadatas', 'distances']
  });

  return {
    available: true,
    db_dir: resolvedDbDir,
    collection,
    query: cleanQuery,
    results: normalizeServerQueryResults(payload),
    warning: ''
  };
}

async function getCollectionDocsLocal(
  where = {},
  { limit = 100, offset = 0, dbDir = '', collection = DEFAULT_COLLECTION } = {}
) {
  const resolvedDbDir = dbDir || resolveDbDir();
  const args = [
    'get',
    '--db-dir',
    resolvedDbDir,
    '--collection',
    collection,
    '--limit',
    String(Math.max(1, Number(limit) || 100))
  ];
  const cleanOffset = Math.max(0, Number(offset) || 0);
  if (cleanOffset) {
    args.push('--offset', String(cleanOffset));
  }

  if (where && typeof where === 'object' && Object.keys(where).length) {
    args.push('--where-json', JSON.stringify(where));
  }

  const payload = await runPythonJson(COLLECTION_LOCAL_SCRIPT, args);
  return {
    available: true,
    db_dir: resolvedDbDir,
    collection,
    docs: normalizeLocalCollectionDocs(payload),
    warning: ''
  };
}

async function getCollectionDocsServer(
  where = {},
  { limit = 100, offset = 0, dbDir = '', collection = DEFAULT_COLLECTION } = {}
) {
  const resolvedDbDir = dbDir || resolveDbDir();
  const collectionHandle = await getServerCollectionHandle(collection);
  const payload = await collectionHandle.get({
    where: where && typeof where === 'object' && Object.keys(where).length ? where : undefined,
    limit: Math.max(1, Number(limit) || 100),
    offset: Math.max(0, Number(offset) || 0),
    include: ['documents', 'metadatas']
  });
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
    })),
    warning: ''
  };
}

async function getCollectionDocsPaginated(
  reader,
  where = {},
  { limit = 100, offset = 0, dbDir = '', collection = DEFAULT_COLLECTION } = {}
) {
  const resolvedDbDir = dbDir || resolveDbDir();
  const cleanLimit = Math.max(1, Number(limit) || 100);
  let remaining = cleanLimit;
  let nextOffset = Math.max(0, Number(offset) || 0);
  const docs = [];

  while (remaining > 0) {
    const batchLimit = Math.min(COLLECTION_GET_BATCH_LIMIT, remaining);
    const payload = await reader(where, {
      limit: batchLimit,
      offset: nextOffset,
      dbDir: resolvedDbDir,
      collection
    });
    const batchDocs = Array.isArray(payload.docs) ? payload.docs : [];
    docs.push(...batchDocs);

    if (batchDocs.length < batchLimit) {
      break;
    }

    remaining -= batchDocs.length;
    nextOffset += batchDocs.length;
  }

  return {
    available: true,
    db_dir: resolvedDbDir,
    collection,
    docs,
    warning: ''
  };
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

  const cacheKey = buildVectorCacheKey(cleanQuery, {
    k,
    dbDir: resolvedDbDir,
    collection
  });
  const cached = getCachedVectorQuery(cacheKey);
  if (cached) return cached;

  const mode = resolveChromaMode({ dbDir: resolvedDbDir });
  const attemptLocalFirst = mode === 'local';

  try {
    const payload = attemptLocalFirst
      ? await queryVectorDbLocal(cleanQuery, { k, dbDir: resolvedDbDir, collection })
      : await queryVectorDbServer(cleanQuery, { k, dbDir: resolvedDbDir, collection });
    return setCachedVectorQuery(cacheKey, payload);
  } catch (primaryError) {
    if (attemptLocalFirst) {
      logChromaDebug('Local Chroma query failed; returning degraded response.', {
        query: cleanQuery,
        error: primaryError?.message || 'local_query_failed'
      });
      return {
        available: false,
        db_dir: resolvedDbDir,
        collection,
        query: cleanQuery,
        results: [],
        warning: primaryError?.message || 'vector_query_failed'
      };
    }

    try {
      const fallbackPayload = await queryVectorDbLocal(cleanQuery, {
        k,
        dbDir: resolvedDbDir,
        collection
      });
      logChromaDebug('Remote Chroma query failed; local helper fallback succeeded.', {
        query: cleanQuery,
        error: primaryError?.message || 'remote_query_failed'
      });
      return setCachedVectorQuery(cacheKey, fallbackPayload);
    } catch (fallbackError) {
      return {
        available: false,
        db_dir: resolvedDbDir,
        collection,
        query: cleanQuery,
        results: [],
        warning:
          fallbackError?.message ||
          primaryError?.message ||
          'vector_query_failed'
      };
    }
  }
}

async function getCollectionDocs(
  where = {},
  { limit = 100, offset = 0, dbDir = '', collection = DEFAULT_COLLECTION } = {}
) {
  const resolvedDbDir = dbDir || resolveDbDir();
  const mode = resolveChromaMode({ dbDir: resolvedDbDir });
  const attemptLocalFirst = mode === 'local';
  const readDocs = attemptLocalFirst ? getCollectionDocsLocal : getCollectionDocsServer;

  try {
    return await getCollectionDocsPaginated(readDocs, where, {
      limit,
      offset,
      dbDir: resolvedDbDir,
      collection
    });
  } catch (primaryError) {
    if (attemptLocalFirst) {
      return {
        available: false,
        db_dir: resolvedDbDir,
        collection,
        docs: [],
        warning: primaryError?.message || 'collection_get_failed'
      };
    }

    try {
      const fallbackPayload = await getCollectionDocsPaginated(getCollectionDocsLocal, where, {
        limit,
        offset,
        dbDir: resolvedDbDir,
        collection
      });
      logChromaDebug('Remote Chroma collection read failed; local helper fallback succeeded.', {
        collection,
        error: primaryError?.message || 'remote_collection_failed'
      });
      return fallbackPayload;
    } catch (fallbackError) {
      return {
        available: false,
        db_dir: resolvedDbDir,
        collection,
        docs: [],
        warning:
          fallbackError?.message ||
          primaryError?.message ||
          'collection_get_failed'
      };
    }
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

  const resolvedDbDir = dbDir || resolveDbDir();
  const mode = resolveChromaMode({ dbDir: resolvedDbDir });

  try {
    let payload = null;
    if (mode === 'local') {
      payload = await runPythonJson(SEMANTIC_CACHE_LOCAL_SCRIPT, [
        'query',
        '--db-dir',
        resolvedDbDir,
        '--collection',
        collection,
        '--question',
        cacheDocument || cleanQuestion,
        '--k',
        String(Math.max(1, Number(k) || 1))
      ]);
    } else {
      const collectionHandle = await getServerCollectionHandle(collection, { create: true });
      const serverPayload = await collectionHandle.query({
        queryTexts: [cacheDocument || cleanQuestion],
        nResults: Math.max(1, Number(k) || 1),
        include: ['documents', 'metadatas', 'distances']
      });
      payload = {
        results: normalizeServerQueryResults(serverPayload)
      };
    }

    const results =
      mode === 'local'
        ? normalizeSemanticCacheResults(payload)
        : Array.isArray(payload.results)
          ? payload.results
          : [];
    const first = results[0] || null;
    const distance = Number(first?.distance);
    const metadata = first?.metadata && typeof first.metadata === 'object' ? first.metadata : {};
    const cachedQuestion = String(metadata.question_text || '').trim();
    const cachedDocument = String(metadata.document_text || '').trim();
    const response = parseJsonText(metadata.response_json || '') || null;
    const uiPayload = parseJsonText(metadata.ui_payload_json || '') || {};
    const answerText =
      String(metadata.answer_text || '').trim() ||
      String(response?.summary || response?.answer || '').trim();
    const compatible = semanticCacheQuestionsAreCompatible(
      cleanQuestion,
      cachedQuestion,
      cacheDocument || cleanQuestion,
      cachedDocument || cachedQuestion
    );

    return {
      hit: Boolean(
        first &&
          Number.isFinite(distance) &&
          distance < Number(maxDistance) &&
          compatible &&
          (answerText || response)
      ),
      question: cleanQuestion,
      cache_document: cacheDocument,
      collection,
      distance: Number.isFinite(distance) ? distance : null,
      cached_question: cachedQuestion,
      compatible,
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

  const resolvedDbDir = dbDir || resolveDbDir();
  const mode = resolveChromaMode({ dbDir: resolvedDbDir });

  try {
    if (mode === 'local') {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cricket-semantic-cache-'));
      const tempFile = path.join(tempDir, 'payload.json');
      fs.writeFileSync(
        tempFile,
        JSON.stringify({
          id: buildSemanticCacheId(cleanQuestion),
          question: cleanQuestion,
          document_text: cacheDocument || cleanQuestion,
          answer_text: cleanAnswer,
          ui_payload:
            uiPayload && typeof uiPayload === 'object'
              ? uiPayload
              : response?.data && typeof response.data === 'object'
                ? response.data
                : {},
          response
        }),
        'utf8'
      );

      try {
        const payload = await runPythonJson(SEMANTIC_CACHE_LOCAL_SCRIPT, [
          'upsert',
          '--db-dir',
          resolvedDbDir,
          '--collection',
          collection,
          '--input',
          tempFile
        ]);
        return {
          saved: Boolean(payload.ok),
          id: String(payload.id || '').trim(),
          reason: payload.ok ? '' : String(payload.error || 'semantic_cache_upsert_failed')
        };
      } finally {
        fs.rmSync(tempDir, { recursive: true, force: true });
      }
    }

    const collectionHandle = await getServerCollectionHandle(collection, { create: true });
    const cacheId = buildSemanticCacheId(cleanQuestion);
    await collectionHandle.upsert({
      ids: [cacheId],
      documents: [cacheDocument || cleanQuestion],
      metadatas: [
        {
          question_text: cleanQuestion,
          document_text: cacheDocument || cleanQuestion,
          answer_text: cleanAnswer,
          response_json: JSON.stringify(response || {}),
          ui_payload_json: JSON.stringify(
            uiPayload && typeof uiPayload === 'object'
              ? uiPayload
              : response?.data && typeof response.data === 'object'
                ? response.data
                : {}
          )
        }
      ]
    });

    return {
      saved: true,
      id: cacheId
    };
  } catch (error) {
    return {
      saved: false,
      reason: error?.message || 'semantic_cache_upsert_failed'
    };
  }
}

async function upsertDocuments(
  docs = [],
  { collection = DEFAULT_COLLECTION } = {}
) {
  const normalizedDocs = (Array.isArray(docs) ? docs : [])
    .map((doc) => ({
      id: String(doc.id || '').trim(),
      document: String(doc.document || '').trim(),
      metadata: doc.metadata && typeof doc.metadata === 'object' ? doc.metadata : {}
    }))
    .filter((doc) => doc.id && doc.document);

  if (!normalizedDocs.length) {
    return { ok: true, upserted: 0, collection };
  }

  const resolvedDbDir = resolveDbDir();
  const mode = resolveChromaMode({ dbDir: resolvedDbDir });

  try {
    if (mode === 'local') {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cricket-chroma-upsert-'));
      const tempFile = path.join(tempDir, 'docs.json');
      fs.writeFileSync(
        tempFile,
        JSON.stringify({ documents: normalizedDocs }, null, 2),
        'utf8'
      );

      try {
        const payload = await runPythonJson(COLLECTION_LOCAL_SCRIPT, [
          'upsert',
          '--db-dir',
          resolvedDbDir,
          '--collection',
          collection,
          '--input',
          tempFile
        ]);
        clearVectorQueryCache();
        clearCollectionCache();
        return {
          ok: Boolean(payload.ok),
          upserted: Number(payload.count || 0),
          collection
        };
      } finally {
        fs.rmSync(tempDir, { recursive: true, force: true });
      }
    }

    const collectionHandle = await getServerCollectionHandle(collection, { create: true });
    await collectionHandle.upsert({
      ids: normalizedDocs.map((doc) => doc.id),
      documents: normalizedDocs.map((doc) => doc.document),
      metadatas: normalizedDocs.map((doc) => doc.metadata)
    });
    clearVectorQueryCache();
    clearCollectionCache();
    return {
      ok: true,
      upserted: normalizedDocs.length,
      collection
    };
  } catch (error) {
    return {
      ok: false,
      upserted: 0,
      collection,
      warning: error?.message || 'document_upsert_failed'
    };
  }
}

async function hasMatchDocument(matchId = '', { collection = DEFAULT_COLLECTION } = {}) {
  const cleanMatchId = String(matchId || '').trim();
  if (!cleanMatchId) return false;
  const payload = await getCollectionDocs(
    { doc_type: 'match_summary', match_id: cleanMatchId },
    { limit: 1, collection }
  );
  return Boolean(payload.available && Array.isArray(payload.docs) && payload.docs.length);
}

async function getChromaHealth({
  dbDir = '',
  collection = DEFAULT_COLLECTION,
  includeProbe = true
} = {}) {
  const resolvedDbDir = dbDir || resolveDbDir();
  const manifest = readChromaManifest();
  const mode = resolveChromaMode({ dbDir: resolvedDbDir });
  const summary = {
    mode,
    db_dir: resolvedDbDir,
    collection: String(collection || DEFAULT_COLLECTION).trim(),
    helper_scripts_ready: localHelperScriptsAvailable(),
    manifest_present: Boolean(manifest),
    manifest_counts: manifest
      ? {
          documents: Number(manifest.collection_count || 0),
          players: Number(manifest.player_docs || 0),
          teams: Number(manifest.team_docs || 0),
          matches: Number(manifest.match_docs || 0)
        }
      : null,
    available: false,
    warning: ''
  };

  if (!includeProbe) {
    summary.available = Boolean(resolvedDbDir || hasExplicitRemoteConfig());
    return summary;
  }

  try {
    const probe = await getCollectionDocs({}, { limit: 1, dbDir: resolvedDbDir, collection });
    summary.available = Boolean(probe.available);
    summary.warning = String(probe.warning || '').trim();
    summary.peek_count = Array.isArray(probe.docs) ? probe.docs.length : 0;
    return summary;
  } catch (error) {
    summary.available = false;
    summary.warning = error?.message || 'chroma_probe_failed';
    return summary;
  }
}

module.exports = {
  queryVectorDb,
  resolveDbDir,
  getCollectionDocs,
  readChromaManifest,
  clearVectorQueryCache,
  clearCollectionCache,
  querySemanticCache,
  saveSemanticCacheEntry,
  upsertDocuments,
  hasMatchDocument,
  getChromaHealth
};
