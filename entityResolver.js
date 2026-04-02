const datasetStore = require('./datasetStore');
const { normalizeText, tokenize, similarityScore } = require('./textUtils');
const { buildPlayerAliases, getCanonicalPlayerName } = require('./playerMaster');

function acronym(text = '') {
  const parts = tokenize(text);
  if (!parts.length) return '';
  return parts.map((part) => part[0]).join('');
}

function makeTeamAliases(name = '') {
  const out = new Set([name]);
  const norm = normalizeText(name);
  if (!norm) return [...out];
  out.add(acronym(name).toUpperCase());

  if (norm.includes('royal challengers bengaluru')) {
    out.add('Royal Challengers Bangalore');
    out.add('RCB');
  }
  if (norm.includes('royal challengers bangalore')) {
    out.add('Royal Challengers Bengaluru');
    out.add('RCB');
  }
  if (norm.includes('delhi daredevils')) {
    out.add('Delhi Capitals');
  }
  if (norm.includes('delhi capitals')) {
    out.add('Delhi Daredevils');
  }
  if (norm.includes('kings xi punjab')) {
    out.add('Punjab Kings');
  }
  if (norm.includes('punjab kings')) {
    out.add('Kings XI Punjab');
  }
  return [...out];
}

function makePlayerAliases(name = '') {
  return buildPlayerAliases(name);
}

function playerDisplayName(name = '') {
  return getCanonicalPlayerName(name) || name;
}

function entityPriority(type = 'player', row = {}) {
  const cache = typeof datasetStore.getCache === 'function' ? datasetStore.getCache() : null;
  if (!cache) return 0;

  if (type === 'player') {
    const player = cache.playerMapById?.get?.(row.id);
    const stats = player?.stats || {};
    return (
      Number(stats.runs || 0) * 10 +
      Number(stats.wickets || 0) * 25 +
      Number(stats.matches || 0)
    );
  }

  if (type === 'team') {
    const team = cache.teamMapById?.get?.(row.id);
    const stats = team?.stats || {};
    return (
      Number(stats.wins || 0) * 50 +
      Number(stats.matches || 0) * 5 +
      Number(stats.runs || 0) / 100
    );
  }

  return 0;
}

function scoreCandidate(query = '', candidate = '', type = 'player') {
  const q = normalizeText(query);
  const c = normalizeText(candidate);
  if (!q || !c) return 0;
  if (q === c) return 1;

  const qTokens = tokenize(q);
  const cTokens = tokenize(c);
  const singleTokenPlayerQuery = type === 'player' && qTokens.length === 1 && cTokens.length > 1;

  let score = 0;
  if (singleTokenPlayerQuery) {
    const token = qTokens[0];
    if (cTokens[0] === token) {
      score = Math.max(score, 0.98);
    } else if (cTokens.includes(token)) {
      score = Math.max(score, 0.9);
    } else if (c.startsWith(`${token} `)) {
      score = Math.max(score, 0.88);
    }
  }
  if (c.includes(q)) {
    score = Math.max(score, singleTokenPlayerQuery ? 0.62 : 0.88);
  }
  if (q.includes(c)) score = Math.max(score, 0.82);

  const exactHits = qTokens.filter((token) => cTokens.includes(token)).length;
  if (qTokens.length > 0) {
    score = Math.max(score, exactHits / Math.max(qTokens.length, cTokens.length));
  }

  if (type === 'player' && qTokens.length >= 2 && cTokens.length >= 2) {
    const qFirst = qTokens[0];
    const qLast = qTokens[qTokens.length - 1];
    const cFirst = cTokens[0];
    const cLast = cTokens[cTokens.length - 1];
    if (qLast === cLast && qFirst[0] && cFirst[0] && qFirst[0] === cFirst[0]) {
      score = Math.max(score, 0.93);
    }
  }

  if (type === 'team') {
    const qAcronym = acronym(q).toUpperCase();
    const cAcronym = acronym(c).toUpperCase();
    if (qAcronym && qAcronym === cAcronym) {
      score = Math.max(score, 0.9);
    }
  }

  let fuzzy = similarityScore(q, c);
  if (singleTokenPlayerQuery && !cTokens.includes(qTokens[0])) {
    fuzzy = Math.min(fuzzy, 0.66);
  }
  score = Math.max(score, fuzzy);
  return score;
}

function rankEntities(type, query) {
  const index = datasetStore.getEntityIndex();
  if (!index) return [];
  const rows = type === 'player' ? index.players : type === 'team' ? index.teams : index.venues;

  return rows
    .map((row) => {
      const aliases =
        type === 'team'
          ? makeTeamAliases(row.name)
          : type === 'player'
            ? makePlayerAliases(row.name)
            : row.aliases || [row.name];
      const best = aliases.reduce((max, alias) => Math.max(max, scoreCandidate(query, alias, type)), 0);
      return {
        row,
        score: best,
        priority: entityPriority(type, row)
      };
    })
    .filter((row) => row.score >= 0.45)
    .sort((a, b) => b.score - a.score || b.priority - a.priority || a.row.name.localeCompare(b.row.name));
}

function getEntityAliases(type, row = {}) {
  if (type === 'team') return makeTeamAliases(row.name);
  if (type === 'player') return makePlayerAliases(row.name);
  return row.aliases || [row.name];
}

function resolveEntity(type, query) {
  const text = String(query || '').trim();
  if (!text) return { status: 'missing' };
  const index = datasetStore.getEntityIndex();
  if (!index) return { status: 'not_found' };
  const rows = type === 'player' ? index.players : type === 'team' ? index.teams : index.venues;
  const normalizedQuery = normalizeText(text);
  const queryTokens = tokenize(normalizedQuery);
  const singleTokenQuery = queryTokens.length === 1;

  const exactMatches = rows.filter((row) =>
    getEntityAliases(type, row).some((alias) => normalizeText(alias) === normalizedQuery)
  );
  if (exactMatches.length === 1) {
    const exact = exactMatches[0];
    return {
      status: 'resolved',
      item:
        type === 'player'
          ? {
              ...exact,
              dataset_name: exact.name,
              canonical_name: playerDisplayName(exact.name)
            }
          : exact
    };
  }
  if (exactMatches.length > 1) {
    const exactRanked = exactMatches
      .map((row) => ({
        row,
        score: getEntityAliases(type, row).reduce((max, alias) => Math.max(max, scoreCandidate(text, alias, type)), 0),
        priority: entityPriority(type, row)
      }))
      .sort((a, b) => b.score - a.score || b.priority - a.priority || a.row.name.localeCompare(b.row.name));
    const best = exactRanked[0]?.row;
    if (best) {
      return {
        status: 'resolved',
        item:
          type === 'player'
            ? {
                ...best,
                dataset_name: best.name,
                canonical_name: playerDisplayName(best.name)
              }
            : best
      };
    }
    return {
      status: 'clarify',
      query: text,
      choices: [...new Set(exactMatches.slice(0, 6).map((entry) => type === 'player' ? playerDisplayName(entry.name) : entry.name))]
    };
  }

  const ranked = rankEntities(type, text);
  if (!ranked.length) return { status: 'not_found' };

  const top = ranked[0];
  const strongThreshold =
    type === 'team'
      ? singleTokenQuery ? 0.62 : 0.8
      : singleTokenQuery ? 0.58 : 0.78;

  if (!top || top.score < strongThreshold) {
    return {
      status: 'clarify',
      query: text,
      choices: [...new Set(ranked.slice(0, 6).map((entry) => type === 'player' ? playerDisplayName(entry.row.name) : entry.row.name))]
    };
  }

  return {
    status: 'resolved',
    item:
      type === 'player'
        ? {
            ...top.row,
            dataset_name: top.row.name,
            canonical_name: playerDisplayName(top.row.name)
          }
        : top.row
  };
}

function resolvePlayer(query) {
  return resolveEntity('player', query);
}

function resolveTeam(query) {
  return resolveEntity('team', query);
}

function resolveVenue(query) {
  return resolveEntity('venue', query);
}

module.exports = {
  scoreCandidate,
  resolvePlayer,
  resolveTeam,
  resolveVenue,
  resolveEntity
};
