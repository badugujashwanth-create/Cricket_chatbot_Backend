function normalizeText(value = '') {
  return String(value || '')
    .toLowerCase()
    .replace(/[.]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenize(value = '') {
  return normalizeText(value).split(' ').filter(Boolean);
}

function slugify(value = '') {
  return normalizeText(value).replace(/\s+/g, '-');
}

function toNumber(value, fallback = 0) {
  if (value === null || value === undefined || value === '') return fallback;
  const parsed = Number(String(value).replace(/[^\d.-]/g, ''));
  return Number.isFinite(parsed) ? parsed : fallback;
}

function round(value, decimals = 2) {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function safeDivide(a, b) {
  if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) return 0;
  return a / b;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function toOverString(balls = 0) {
  const safeBalls = Math.max(0, Math.floor(Number(balls || 0)));
  const overs = Math.floor(safeBalls / 6);
  const rem = safeBalls % 6;
  return `${overs}.${rem}`;
}

function parseDateToIso(value = '') {
  const text = String(value || '').trim();
  if (!text) return '';
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return text;
  return date.toISOString().slice(0, 10);
}

function normalizeSeasonLabel(value = '') {
  const text = String(value || '').trim();
  if (!text) return '';
  return text;
}

function seasonIncludes(seasonLabel = '', seasonQuery = '') {
  const label = normalizeText(seasonLabel);
  const query = normalizeText(seasonQuery);
  if (!label || !query) return false;
  return label.includes(query);
}

function formatMaybeNumber(value, decimals = 2) {
  if (!Number.isFinite(Number(value))) return 'N/A';
  return round(Number(value), decimals);
}

function chooseMostFrequent(map) {
  let winner = '';
  let maxCount = -1;
  for (const [key, count] of map.entries()) {
    if (count > maxCount) {
      maxCount = count;
      winner = key;
    }
  }
  return winner;
}

function buildPairKey(a = '', b = '') {
  const sorted = [String(a || ''), String(b || '')].sort((x, y) => x.localeCompare(y));
  return `${sorted[0]}__${sorted[1]}`;
}

function levenshtein(a = '', b = '') {
  const s = normalizeText(a);
  const t = normalizeText(b);
  if (!s) return t.length;
  if (!t) return s.length;

  const rows = s.length + 1;
  const cols = t.length + 1;
  const matrix = Array.from({ length: rows }, () => new Array(cols).fill(0));

  for (let i = 0; i < rows; i += 1) matrix[i][0] = i;
  for (let j = 0; j < cols; j += 1) matrix[0][j] = j;

  for (let i = 1; i < rows; i += 1) {
    for (let j = 1; j < cols; j += 1) {
      const cost = s[i - 1] === t[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost
      );
    }
  }
  return matrix[rows - 1][cols - 1];
}

function similarityScore(a = '', b = '') {
  const left = normalizeText(a);
  const right = normalizeText(b);
  if (!left || !right) return 0;
  if (left === right) return 1;

  const leftTokens = tokenize(left);
  const rightTokens = tokenize(right);
  const overlap = leftTokens.filter((token) => rightTokens.includes(token)).length;
  const tokenScore = leftTokens.length ? overlap / leftTokens.length : 0;

  const dist = levenshtein(left, right);
  const len = Math.max(left.length, right.length);
  const distanceScore = len ? 1 - dist / len : 0;

  const substringScore = right.includes(left) || left.includes(right) ? 0.9 : 0;

  if (leftTokens.length >= 2 && rightTokens.length >= 2) {
    const leftFirst = leftTokens[0];
    const leftLast = leftTokens[leftTokens.length - 1];
    const rightFirst = rightTokens[0];
    const rightLast = rightTokens[rightTokens.length - 1];
    const initialLastMatch =
      leftLast === rightLast && leftFirst[0] && rightFirst[0] && leftFirst[0] === rightFirst[0];
    if (initialLastMatch) return Math.max(0.92, tokenScore, substringScore, distanceScore);
  }

  return Math.max(tokenScore, substringScore, distanceScore);
}

module.exports = {
  normalizeText,
  tokenize,
  slugify,
  toNumber,
  round,
  safeDivide,
  clamp,
  toOverString,
  parseDateToIso,
  normalizeSeasonLabel,
  seasonIncludes,
  formatMaybeNumber,
  chooseMostFrequent,
  buildPairKey,
  levenshtein,
  similarityScore
};
