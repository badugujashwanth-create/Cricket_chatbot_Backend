const datasetStore = require('./datasetStore');
const { normalizeText, tokenize, similarityScore } = require('./textUtils');

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
  const out = new Set([name]);
  const parts = tokenize(name);
  if (parts.length >= 2) {
    const first = parts[0];
    const last = parts[parts.length - 1];
    out.add(`${first[0]} ${last}`.toUpperCase());
    out.add(last);
  }
  return [...out];
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
  if (c.includes(q)) {
    score = Math.max(score, singleTokenPlayerQuery ? 0.74 : 0.88);
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
  if (singleTokenPlayerQuery) {
    fuzzy = Math.min(fuzzy, 0.76);
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
      return { row, score: best };
    })
    .filter((row) => row.score >= 0.45)
    .sort((a, b) => b.score - a.score || a.row.name.localeCompare(b.row.name));
}

function resolveEntity(type, query) {
  const text = String(query || '').trim();
  if (!text) return { status: 'missing' };
  const ranked = rankEntities(type, text);
  if (!ranked.length) return { status: 'not_found' };

  const top = ranked[0];
  const second = ranked[1];
  const strongThreshold = type === 'team' ? 0.8 : 0.78;
  const gapThreshold = 0.08;

  if (!top || top.score < strongThreshold) {
    return {
      status: 'clarify',
      query: text,
      choices: ranked.slice(0, 6).map((entry) => entry.row.name)
    };
  }

  if (second && Math.abs(top.score - second.score) <= gapThreshold) {
    return {
      status: 'clarify',
      query: text,
      choices: ranked.slice(0, 6).map((entry) => entry.row.name)
    };
  }

  return {
    status: 'resolved',
    item: top.row
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
  resolvePlayer,
  resolveTeam,
  resolveVenue,
  resolveEntity
};
