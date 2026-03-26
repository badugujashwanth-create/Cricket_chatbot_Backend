function cleanEntitySegment(value = '') {
  let cleaned = String(value || '')
    .replace(/^(?:please\s+)?compare\s+/i, '')
    .replace(/^(?:player|team)\s+/i, '')
    .replace(/^[\s,:-]+|[\s?!.,:;-]+$/g, '')
    .trim();

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
  const vsMatch = stripped.match(/^(.+?)\s+(?:vs\.?|versus|v)\s+(.+)$/i);
  if (vsMatch) {
    return {
      left: cleanEntitySegment(vsMatch[1]),
      right: cleanEntitySegment(vsMatch[2])
    };
  }

  if (/^(?:please\s+)?compare\b/i.test(raw)) {
    const compareMatch = stripped.match(/^(.+?)\s+(?:with|and)\s+(.+)$/i);
    if (compareMatch) {
      return {
        left: cleanEntitySegment(compareMatch[1]),
        right: cleanEntitySegment(compareMatch[2])
      };
    }
  }

  return null;
}

module.exports = {
  cleanEntitySegment,
  parseVsSides
};
