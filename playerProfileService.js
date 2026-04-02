const { searchPlayers: searchCricApiPlayers } = require('./cricApiService');
const { getCanonicalPlayerName } = require('./playerMaster');
const { normalizeText, similarityScore, tokenize } = require('./textUtils');

const PROFILE_CACHE_TTL_MS = 12 * 60 * 60 * 1000;
const PROFILE_CACHE_LIMIT = 300;
const profileCache = new Map();

function getCacheKey(query = '', datasetName = '') {
  return JSON.stringify({
    query: String(query || '').trim().toLowerCase(),
    datasetName: String(datasetName || '').trim().toLowerCase()
  });
}

function getCachedProfile(key = '') {
  const cached = profileCache.get(key);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    profileCache.delete(key);
    return null;
  }
  profileCache.delete(key);
  profileCache.set(key, cached);
  return cached.value;
}

function setCachedProfile(key = '', value) {
  if (!key) return value;
  if (profileCache.has(key)) {
    profileCache.delete(key);
  }
  profileCache.set(key, {
    value,
    expiresAt: Date.now() + PROFILE_CACHE_TTL_MS
  });
  while (profileCache.size > PROFILE_CACHE_LIMIT) {
    const oldestKey = profileCache.keys().next().value;
    if (!oldestKey) break;
    profileCache.delete(oldestKey);
  }
  return value;
}

function looksAbbreviated(name = '') {
  const parts = tokenize(name);
  if (!parts.length) return false;
  if (parts.length === 1) return parts[0].length <= 3;
  return parts.some((part, index) => index < parts.length - 1 && part.length <= 2);
}

function chooseBaseCanonicalName(query = '', datasetName = '') {
  const queryCanonical = getCanonicalPlayerName(query);
  if (queryCanonical) return queryCanonical;

  const datasetCanonical = getCanonicalPlayerName(datasetName);
  if (datasetCanonical) return datasetCanonical;

  const cleanQuery = String(query || '').trim();
  if (
    tokenize(cleanQuery).length >= 2 &&
    similarityScore(cleanQuery, datasetName) >= 0.78
  ) {
    return cleanQuery;
  }

  const cleanDatasetName = String(datasetName || '').trim();
  if (cleanDatasetName && !looksAbbreviated(cleanDatasetName)) {
    return cleanDatasetName;
  }

  return cleanDatasetName || cleanQuery;
}

async function fetchWikipediaSummary(playerName = '') {
  const cleanPlayerName = String(playerName || '').trim();
  if (!cleanPlayerName) return null;

  try {
    const response = await fetch(
      `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(cleanPlayerName)}`,
      {
        headers: {
          Accept: 'application/json',
          'User-Agent': 'Cricket-Intelligence-Console/1.0'
        }
      }
    );
    if (!response.ok) return null;
    const payload = await response.json();
    return {
      title: String(payload?.title || cleanPlayerName),
      image_url: String(payload?.thumbnail?.source || ''),
      wikipedia_url: String(payload?.content_urls?.desktop?.page || ''),
      short_description: String(payload?.description || ''),
      description: String(payload?.extract || payload?.description || '')
    };
  } catch (_) {
    return null;
  }
}

async function fetchExternalPlayer(query = '', datasetName = '') {
  const searchTerms = [query, datasetName].map((value) => String(value || '').trim()).filter(Boolean);
  for (const term of searchTerms) {
    try {
      const payload = await searchCricApiPlayers({
        q: term,
        offset: 0,
        limit: 5
      });
      const items = Array.isArray(payload?.items) ? payload.items : [];
      if (!items.length) continue;
      const ranked = items
        .map((item) => ({
          item,
          score: Math.max(
            similarityScore(term, item.name),
            similarityScore(normalizeText(datasetName || ''), item.name)
          )
        }))
        .sort((left, right) => right.score - left.score);
      if (ranked[0]?.score >= 0.72) {
        return ranked[0].item;
      }
    } catch (_) {
      // Ignore external provider failures and fall back to local data.
    }
  }
  return null;
}

async function getPlayerProfile({ query = '', datasetName = '' } = {}) {
  const cacheKey = getCacheKey(query, datasetName);
  const cached = getCachedProfile(cacheKey);
  if (cached) return cached;

  const external = await fetchExternalPlayer(query, datasetName);
  const preferredCanonical = chooseBaseCanonicalName(query, datasetName);
  const externalName = String(external?.name || '').trim();
  const externalScore = externalName
    ? Math.max(
        similarityScore(preferredCanonical, externalName),
        similarityScore(String(datasetName || '').trim(), externalName)
      )
    : 0;
  const canonicalName =
    externalName && (!preferredCanonical || externalScore >= 0.82)
      ? externalName
      : preferredCanonical || externalName;
  const wiki = await fetchWikipediaSummary(canonicalName);

  return setCachedProfile(cacheKey, {
    canonical_name: canonicalName || String(datasetName || query || '').trim(),
    image_url: String(wiki?.image_url || external?.image_url || ''),
    wikipedia_url: String(wiki?.wikipedia_url || ''),
    short_description: String(wiki?.short_description || ''),
    description: String(wiki?.description || ''),
    country: String(external?.country || '')
  });
}

module.exports = {
  getPlayerProfile
};
