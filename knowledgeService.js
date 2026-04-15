const fs = require('fs');
const path = require('path');
const { normalizeText, similarityScore, tokenize } = require('./textUtils');

const DATA_DIR = path.join(__dirname, 'data');
const KNOWLEDGE_FILES = [
  'cricket_rules.json',
  'cricket_terms.json',
  'cricket_history.json',
  'cricket_records.json',
  'worldcup_winners.json',
  'equipment_and_training.json'
];

let knowledgeCache = null;

function uniqueNonEmpty(values = []) {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
}

function loadKnowledgeBase() {
  if (knowledgeCache) return knowledgeCache;

  const items = [];
  for (const fileName of KNOWLEDGE_FILES) {
    const filePath = path.join(DATA_DIR, fileName);
    if (!fs.existsSync(filePath)) continue;
    try {
      const payload = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      if (!Array.isArray(payload)) continue;
      for (const entry of payload) {
        if (!entry || typeof entry !== 'object') continue;
        items.push({
          id: String(entry.id || '').trim(),
          category: String(entry.category || 'general').trim(),
          sub_intent: String(entry.sub_intent || '').trim(),
          title: String(entry.title || '').trim(),
          aliases: uniqueNonEmpty(entry.aliases || []),
          keywords: uniqueNonEmpty(entry.keywords || []),
          answer: String(entry.answer || '').trim(),
          examples: uniqueNonEmpty(entry.examples || []),
          related_topics: uniqueNonEmpty(entry.related_topics || [])
        });
      }
    } catch (_) {
      // Ignore a single malformed file and continue loading the rest.
    }
  }

  knowledgeCache = items.filter((entry) => entry.id && entry.title && entry.answer);
  return knowledgeCache;
}

function scoreAlias(query = '', alias = '') {
  const normalizedQuery = normalizeText(query);
  const normalizedAlias = normalizeText(alias);
  if (!normalizedQuery || !normalizedAlias) return 0;
  if (normalizedQuery === normalizedAlias) return 4;
  if (normalizedQuery.includes(normalizedAlias)) return 2.6;
  if (normalizedAlias.includes(normalizedQuery)) return 2.2;
  return similarityScore(normalizedQuery, normalizedAlias);
}

function keywordOverlapScore(query = '', keywords = []) {
  const queryTokens = new Set(tokenize(normalizeText(query)));
  if (!queryTokens.size) return 0;
  let score = 0;
  for (const keyword of uniqueNonEmpty(keywords)) {
    const keywordTokens = tokenize(normalizeText(keyword));
    if (!keywordTokens.length) continue;
    if (keywordTokens.every((token) => queryTokens.has(token))) {
      score += keywordTokens.length > 1 ? 0.75 : 0.35;
    }
  }
  return score;
}

function scoreKnowledgeEntry(query = '', entry = {}, { subIntent = '' } = {}) {
  const aliases = uniqueNonEmpty([entry.title, ...(entry.aliases || [])]);
  let bestAliasScore = 0;
  for (const alias of aliases) {
    bestAliasScore = Math.max(bestAliasScore, scoreAlias(query, alias));
  }

  let score = bestAliasScore;
  score += keywordOverlapScore(query, entry.keywords || []);
  score += Math.max(0, similarityScore(normalizeText(query), normalizeText(entry.title || '')) - 0.35);

  if (subIntent && String(entry.sub_intent || '').trim() === String(subIntent || '').trim()) {
    score += 0.8;
  }

  return Number(score.toFixed(4));
}

function lookupKnowledge(question = '', { subIntent = '', limit = 5 } = {}) {
  const query = String(question || '').trim();
  if (!query) {
    return {
      found: false,
      score: 0,
      entry: null,
      candidates: []
    };
  }

  const candidates = loadKnowledgeBase()
    .map((entry) => ({
      ...entry,
      score: scoreKnowledgeEntry(query, entry, { subIntent })
    }))
    .filter((entry) => entry.score >= 0.9)
    .sort((left, right) => right.score - left.score || left.title.localeCompare(right.title));

  return {
    found: Boolean(candidates[0]),
    score: Number(candidates[0]?.score || 0),
    entry: candidates[0] || null,
    candidates: candidates.slice(0, Math.max(1, Number(limit) || 5))
  };
}

module.exports = {
  loadKnowledgeBase,
  lookupKnowledge
};
