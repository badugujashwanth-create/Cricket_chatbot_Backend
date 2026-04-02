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

module.exports = {
  cleanEntitySegment,
  parseVsSides
};
