const { normalizeText, similarityScore, tokenize } = require('./textUtils');

// Only treat as LIVE if explicitly live-time keywords are used
// Removed 'score' from this regex - "highest score" should NOT trigger live routing
const LIVE_LEANING_REGEX =
  /\b(live|current|today|latest|now|ongoing|recent|playing|schedule|fixture|upcoming|next match|tomorrow|who is playing|playing xi|playing 11)\b/i;

function cleanEntitySegment(value = '') {
  let cleaned = String(value || '')
    .replace(/^(?:please\s+)?compare\s+/i, '')
    .replace(/^(?:player|team)\s+/i, '')
    .replace(/^[\s,:-]+|[\s?!.,:;-]+$/g, '')
    .trim();

  const trailingContextRegexes = [
    /\bhead\s+to\s+head(?:\s+who\s+win(?:s)?\s+more)?\b.*$/i,
    /\bwho\s+win(?:s)?\s+more\b.*$/i,
    /\bwho\s+won\s+more\b.*$/i,
    /\bwho\s+is\s+(?:best|better|stronger|greater|more\s+dangerous)\b.*$/i,
    /\bwhich\s+team\s+more\s+strong\b.*$/i,
    /\bbatting\s+lineup\b.*$/i,
    /\bbowling\s+attack\b.*$/i,
    /\boverall\s+team\b.*$/i,
    /\bin\s+\w+\s+stats?\b.*$/i,
    /\bstats?\s+compare\b.*$/i,
    /\bcompare\s+fast\b.*$/i,
    /\bright\s+now\b.*$/i
  ];
  for (const pattern of trailingContextRegexes) {
    cleaned = cleaned.replace(pattern, '').replace(/^[\s,:-]+|[\s?!.,:;-]+$/g, '').trim();
  }

  const trailingNoiseRegex =
    /\b(?:stats?|statistic(?:s)?|record(?:s)?|profile|information|info|career|summary|details|numbers?|performance|form|player|team)\b$/i;

  while (cleaned && trailingNoiseRegex.test(cleaned)) {
    cleaned = cleaned.replace(trailingNoiseRegex, '').replace(/^[\s,:-]+|[\s?!.,:;-]+$/g, '').trim();
  }

  return cleaned;
}

function parseVsSides(question = '') {
  const raw = String(question || '').trim();
  if (!raw) return null;

  const stripped = cleanEntitySegment(raw);
  const patterns = [
    /^(.+?)\s+(?:vs\.?|versus|v)\s+(.+)$/i,
    /^(?:please\s+)?compare\s+(.+?)\s+(?:with|to|and)\s+(.+)$/i,
    /^how\s+does\s+(.+?)\s+compare\s+to\s+(.+)$/i,
    /^(.+?)\s+compared\s+(?:to|with)\s+(.+)$/i,
    /^who\s+is\s+(?:better|stronger|greater|more\s+dangerous)\s+(.+?)\s+or\s+(.+)$/i,
    /^is\s+(.+?)\s+better\s+than\s+(.+)$/i,
    /^(.+?)\s+or\s+(.+?)\s+who\s+is\s+(?:better|stronger|greater|more\s+dangerous)$/i,
    /^(?:what(?:'s| is)\s+the\s+)?difference\s+between\s+(.+?)\s+and\s+(.+)$/i,
    /^(?:show\s+)?comparison\s+between\s+(.+?)\s+and\s+(.+)$/i,
    /^head\s+to\s+head(?:\s+between)?\s+(.+?)\s+(?:and|vs\.?|versus)\s+(.+)$/i
  ];

  for (const pattern of patterns) {
    const match = stripped.match(pattern);
    if (!match) continue;
    const left = cleanEntitySegment(match[1]);
    const right = cleanEntitySegment(match[2]);
    if (!left || !right) continue;
    return { left, right };
  }

  return null;
}

function isLiveLeaningQuestion(question = '') {
  return LIVE_LEANING_REGEX.test(normalizeText(question));
}

function uniqueNonEmpty(values = []) {
  return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
}

function candidateAliases(candidate = {}) {
  return uniqueNonEmpty([
    candidate.name,
    candidate.canonical_name,
    candidate.dataset_name,
    candidate.player,
    candidate.team
  ]);
}

function scoreEntityCandidate(query = '', candidate = {}, { liveLeaning = false, liveAliases = [] } = {}) {
  const normalizedQuery = normalizeText(query);
  if (!normalizedQuery) return 0;

  const aliases = candidateAliases(candidate);
  const liveAliasSet = new Set(
    uniqueNonEmpty(liveAliases).map((value) => normalizeText(value))
  );

  let best = Number(candidate.score || 0);
  for (const alias of aliases) {
    const normalizedAlias = normalizeText(alias);
    if (!normalizedAlias) continue;

    let score = similarityScore(normalizedQuery, normalizedAlias);
    if (normalizedAlias === normalizedQuery) score += 1;
    else if (normalizedAlias.startsWith(normalizedQuery)) score += 0.45;
    else if (normalizedAlias.includes(normalizedQuery)) score += 0.2;

    const queryTokens = tokenize(normalizedQuery);
    const aliasTokens = tokenize(normalizedAlias);
    if (queryTokens.length === 1 && aliasTokens.includes(queryTokens[0])) {
      score += 0.2;
    }
    if (queryTokens.length > 1 && queryTokens.every((token) => aliasTokens.includes(token))) {
      score += 0.18;
    }
    if (liveLeaning && liveAliasSet.has(normalizedAlias)) {
      score += 0.45;
    }

    best = Math.max(best, score);
  }

  if (liveLeaning) {
    if (candidate.is_active) best += 0.3;
    if (candidate.source === 'cricapi' || candidate.provider === 'cricapi') best += 0.22;
  } else if (candidate.source === 'archive' || candidate.source === 'vector') {
    best += 0.04;
  }

  return Number(best.toFixed(4));
}

// Detect entity type: PLAYER, TEAM, TOURNAMENT, or UNKNOWN
function detectEntityType(candidate = {}, query = '') {
  // If candidate already has a type field, use it as hint
  const candidateType = String(candidate.type || candidate.entity_type || '').trim().toUpperCase();
  if (['PLAYER', 'TEAM', 'TOURNAMENT'].includes(candidateType)) {
    return candidateType;
  }

  // Check if it's a team based on properties
  if (candidate.role && !candidate.role.includes('unknown') && candidate.role !== 'team') {
    // Has a player role (batsman, bowler, etc.)
    return 'PLAYER';
  }
  if (candidate.is_team || candidate.team_id || (candidate.source === 'team' && !candidate.role)) {
    return 'TEAM';
  }

  // Heuristic: check the name pattern
  const nameStr = String(candidate.name || '').trim();
  const knownTeams = new Set([
    'india', 'australia', 'england', 'south africa', 'pakistan', 'west indies', 'new zealand', 
    'sri lanka', 'bangladesh', 'afghanistan', 'ireland', 'zimbabwe', 'namibia', 'netherlands',
    'scotland', 'omani', 'uae', 'thailand', 'kenya', 'canada', 'nepal', 'hong kong',
    'mumbai', 'delhi', 'bangalore', 'kolkata', 'hyderabad', 'mumbai indians', 'delhi capitals',
    'royal challengers', 'kolkata knight riders', 'sunrisers hyderabad', 'csk', 'rr',
    'mi', 'dc', 'kkr', 'kxip', 'srh', 'rcb', 'dc', 'kkr'
  ]);

  if (knownTeams.has(nameStr.toLowerCase())) {
    return 'TEAM';
  }

  // Default: if we have no other clues, check if it looks like multiple words (likely player name)
  if (nameStr.split(/\s+/).length >= 2) {
    return 'PLAYER';
  }

  return 'UNKNOWN';
}

function rankEntityCandidates(query = '', candidates = [], options = {}) {
  return (Array.isArray(candidates) ? candidates : [])
    .map((candidate) => ({
      ...candidate,
      entity_type: candidate.entity_type || detectEntityType(candidate, query),
      weighted_score: scoreEntityCandidate(query, candidate, options)
    }))
    .sort((left, right) => {
      return (
        Number(right.weighted_score || 0) - Number(left.weighted_score || 0) ||
        Number(right.score || 0) - Number(left.score || 0) ||
        String(left.name || left.canonical_name || '').localeCompare(
          String(right.name || right.canonical_name || '')
        )
      );
    });
}

module.exports = {
  cleanEntitySegment,
  parseVsSides,
  isLiveLeaningQuestion,
  scoreEntityCandidate,
  detectEntityType,
  rankEntityCandidates
};
