const crypto = require('crypto');
const csvAnalytics = require('./csvAnalytics');
const chromaSearch = require('./chromaSearch');
const liveCricketService = require('./liveCricketService');
const seed = require('./data');

const sessions = new Map();
const SESSION_TTL_MS = 60 * 60 * 1000;
const STOPWORDS = new Set([
  'about', 'all', 'and', 'average', 'averages', 'best', 'did', 'for', 'how', 'last', 'latest',
  'many', 'match', 'matches', 'most', 'recent', 'result', 'run', 'runs', 'score', 'scored', 'show',
  'stat', 'stats', 'strike', 'take', 'took', 'team', 'trend', 'vs', 'versus', 'what', 'wicket', 'wickets', 'with'
]);

function normalize(v = '') {
  return String(v).toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
}

function tokens(v = '') {
  return normalize(v).split(' ').filter(Boolean);
}

function compactTokens(list = []) {
  const filtered = list.filter((t) => t && !STOPWORDS.has(t));
  return filtered.length ? filtered : list.filter(Boolean);
}

function round(n, d = 2) {
  if (!Number.isFinite(n)) return 0;
  const f = 10 ** d;
  return Math.round(n * f) / f;
}

function avg(list = []) {
  if (!Array.isArray(list) || !list.length) return 0;
  return list.reduce((s, x) => s + Number(x || 0), 0) / list.length;
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function stddev(list = []) {
  if (!Array.isArray(list) || list.length < 2) return 0;
  const mean = avg(list);
  const variance = avg(list.map((x) => (Number(x || 0) - mean) ** 2));
  return Math.sqrt(variance);
}

function logistic(x) {
  return 1 / (1 + Math.exp(-x));
}

function poissonProbAtLeast(lambda, threshold) {
  const safeLambda = Math.max(0, Number(lambda || 0));
  const k = Math.max(0, Math.floor(Number(threshold || 0)));
  if (k <= 0) return 1;
  let cumulative = 0;
  let term = Math.exp(-safeLambda);
  cumulative += term; // P(0)
  for (let i = 1; i < k; i += 1) {
    term *= safeLambda / i;
    cumulative += term;
  }
  return clamp(1 - cumulative, 0, 1);
}

function confidenceLabel(score) {
  const c = Number(score || 0);
  if (c >= 0.78) return 'High';
  if (c >= 0.58) return 'Medium';
  return 'Low';
}

function pruneSessions() {
  const now = Date.now();
  for (const [id, s] of sessions.entries()) {
    if (!s || now - s.updatedAt > SESSION_TTL_MS) sessions.delete(id);
  }
}

function getSession(sessionId) {
  pruneSessions();
  if (sessionId && sessions.has(sessionId)) {
    const s = sessions.get(sessionId);
    s.updatedAt = Date.now();
    return s;
  }
  const s = {
    id: sessionId || crypto.randomUUID(),
    updatedAt: Date.now(),
    history: [],
    context: { player: null, team: null, matchId: null, lastType: null }
  };
  sessions.set(s.id, s);
  return s;
}

function remember(session, query, payload) {
  session.updatedAt = Date.now();
  session.context.lastType = payload?.type || session.context.lastType;
  if (payload?.player?.name) session.context.player = payload.player.name;
  if (payload?.players?.[0]?.name) session.context.player = payload.players[0].name;
  if (payload?.team?.name) session.context.team = payload.team.name;
  if (payload?.teams?.[0]?.name) session.context.team = payload.teams[0].name;
  if (payload?.match?.id) session.context.matchId = String(payload.match.id);
  if (payload?.live?.match?.id) session.context.matchId = String(payload.live.match.id);
  session.history.push({ at: new Date().toISOString(), query: String(query || ''), type: payload?.type || 'unknown' });
  if (session.history.length > 20) session.history.shift();
}

function mergeKnowledge(cache) {
  const players = [];
  const teams = [];
  const seenPlayers = new Set();
  const seenTeams = new Set();
  const add = (list, seen, source, entity) => {
    if (!entity?.name) return;
    const key = normalize(entity.name);
    if (!key || seen.has(key)) return;
    seen.add(key);
    list.push({ source, entity });
  };
  for (const p of cache?.players || []) add(players, seenPlayers, 'csv', p);
  for (const p of seed.players || []) add(players, seenPlayers, 'seed', p);
  for (const t of cache?.teams || []) add(teams, seenTeams, 'csv', t);
  for (const t of seed.teams || []) add(teams, seenTeams, 'seed', t);

  const matchById = new Map();
  for (const [id, m] of cache?.matchById?.entries?.() || []) matchById.set(String(id), { source: 'csv', match: m });
  for (const m of seed.matches || []) {
    const key = String(m.id || '');
    if (key && !matchById.has(key)) matchById.set(key, { source: 'seed', match: m });
  }
  return { cache, players, teams, matchById, leaderboards: cache?.leaderboards || null };
}

function prominence(kind, entity = {}) {
  if (kind === 'player') {
    const s = entity.stats || {};
    return Math.min(0.07, Math.log10(1 + (s.runs || 0) + (s.wickets || 0) * 20 + (s.matches || 0) * 5) / 20);
  }
  if (kind === 'team') {
    const s = entity.stats || {};
    return Math.min(0.05, Math.log10(1 + (s.matches || 0) + (s.wins || 0) * 3) / 25);
  }
  return 0;
}

function scoreName(query, name, kind, entity) {
  const qRaw = normalize(query);
  const nRaw = normalize(name);
  if (!qRaw || !nRaw) return 0;
  const qTokens = compactTokens(tokens(qRaw));
  const nTokens = tokens(nRaw);
  const q = qTokens.join(' ') || qRaw;
  let score = 0;
  if (nRaw === qRaw || nRaw === q) score = 0.995;
  else if (nRaw.startsWith(q)) score = 0.91;
  else if (q && nRaw.includes(q)) score = 0.84;
  const exact = qTokens.filter((t) => nTokens.includes(t)).length;
  const prefix = qTokens.filter((t) => !nTokens.includes(t) && nTokens.some((nt) => nt.startsWith(t))).length;
  const coverage = qTokens.length ? (exact + prefix * 0.6) / qTokens.length : 0;
  if (coverage > 0) score = Math.max(score, 0.56 + Math.min(0.28, coverage * 0.32));
  if (qTokens.length === 1 && nTokens.length >= 2) {
    const t = qTokens[0];
    const first = nTokens[0];
    const last = nTokens[nTokens.length - 1];
    if (t === last) score = Math.max(score, kind === 'player' ? 0.93 : 0.9);
    if (t === first) score = Math.max(score, kind === 'player' ? 0.82 : 0.88);
  }
  score += prominence(kind, entity);
  return Math.min(0.999, score);
}

function resolveEntity(items, ref, kind) {
  const q = String(ref || '').trim();
  if (!q) return { status: 'none', candidates: [] };
  const rows = items
    .map((row) => ({ ...row, confidence: scoreName(q, row.entity?.name || '', kind, row.entity) }))
    .filter((r) => r.confidence >= 0.34)
    .sort((a, b) => (b.confidence - a.confidence) || String(a.entity?.name || '').localeCompare(String(b.entity?.name || '')));
  if (!rows.length) return { status: 'none', candidates: [] };
  const top = rows[0];
  const second = rows[1];
  const threshold = kind === 'team' ? 0.82 : 0.78;
  const gap = kind === 'team' ? 0.06 : 0.055;
  if (second && top.confidence < 0.985 && second.confidence >= threshold - 0.08 && top.confidence - second.confidence <= gap) {
    return { status: 'ambiguous', top, candidates: rows.slice(0, 5) };
  }
  if (top.confidence < threshold) return { status: 'uncertain', top, candidates: rows.slice(0, 5) };
  return { status: 'resolved', top, candidates: rows.slice(0, 5) };
}

function clarify(kind, ref, result) {
  const options = (result?.candidates || []).slice(0, 5).map((r) => ({
    label: r.entity.name,
    value: r.entity.name,
    kind,
    source: r.source,
    confidence: round(r.confidence, 3),
    meta: kind === 'player' ? `${r.entity.team || 'Unknown'} | ${r.entity.role || 'Cricketer'}` : (r.entity.region || 'Team')
  }));
  return {
    type: 'clarification',
    summary: `I found multiple ${kind} matches for "${ref}". Please choose one so I do not guess incorrectly.`,
    entityType: kind,
    options,
    suggestions: options.map((o) => o.value),
    dataSource: options.some((o) => o.source === 'csv') ? 'csv' : 'seed'
  };
}

function teamView(wrapper) {
  if (wrapper.source === 'csv') return wrapper.entity;
  const recent = seed.getRecentMatchesForTeam(wrapper.entity.name, 5);
  const wins = recent.filter((m) => String(m.result || '').toLowerCase().startsWith(String(wrapper.entity.name).toLowerCase())).length;
  return {
    ...wrapper.entity,
    stats: { matches: recent.length, wins, losses: Math.max(0, recent.length - wins), noResult: 0, winRate: recent.length ? round((wins * 100) / recent.length, 1) : 0, runs: 0, averageScore: 0, strikeRate: 0, wicketsLost: 0 },
    recentMatches: recent,
    topBatters: [],
    topBowlers: []
  };
}

function playerRelated(wrapper) {
  if (wrapper.source === 'csv') return Array.isArray(wrapper.entity.recentMatches) ? wrapper.entity.recentMatches.slice(0, 5) : [];
  return seed.getRelatedMatchesForPlayer(wrapper.entity.name, 5);
}

function teamRecent(wrapper) {
  const t = teamView(wrapper);
  return Array.isArray(t.recentMatches) ? t.recentMatches.slice(0, 5) : [];
}

function leaderboardIntent(query) {
  const q = normalize(query);
  if (!q) return null;
  if (/(top|most|highest).*(run|runs|batter|batsman)/.test(q) || /run scorers?/.test(q)) return { key: 'runs', title: 'Top run scorers' };
  if (/(top|most|highest).*(wicket|wickets|bowler)/.test(q)) return { key: 'wickets', title: 'Top wicket takers' };
  if (/(best|top|highest).*(average|averages)/.test(q) || /best averages?/.test(q)) return { key: 'averages', title: 'Best batting averages' };
  if (/(most|top).*(team|teams).*(wins?)/.test(q) || /most wins/.test(q)) return { key: 'teamWins', title: 'Top teams by wins' };
  return null;
}

function fallbackLeaderboardRows(k, kb) {
  const players = kb.players.map((x) => x.entity);
  const teams = kb.teams.map((x) => teamView(x));
  if (k === 'runs') return [...players].filter((p) => (p.stats?.runs || 0) > 0).sort((a, b) => (b.stats.runs - a.stats.runs) || a.name.localeCompare(b.name)).slice(0, 10).map((p, i) => ({ rank: i + 1, label: p.name, team: p.team, value: `${p.stats.runs} runs`, meta: `Avg ${p.stats.average || 0}` }));
  if (k === 'wickets') return [...players].filter((p) => (p.stats?.wickets || 0) > 0).sort((a, b) => (b.stats.wickets - a.stats.wickets) || a.name.localeCompare(b.name)).slice(0, 10).map((p, i) => ({ rank: i + 1, label: p.name, team: p.team, value: `${p.stats.wickets} wickets`, meta: `Econ ${p.stats.economy || 0}` }));
  if (k === 'averages') return [...players].filter((p) => (p.stats?.average || 0) > 0).sort((a, b) => (b.stats.average - a.stats.average) || (b.stats.runs - a.stats.runs) || a.name.localeCompare(b.name)).slice(0, 10).map((p, i) => ({ rank: i + 1, label: p.name, team: p.team, value: `Avg ${p.stats.average}`, meta: `${p.stats.runs || 0} runs | SR ${p.stats.strikeRate || 0}` }));
  if (k === 'teamWins') return [...teams].filter((t) => t.stats).sort((a, b) => ((b.stats.wins || 0) - (a.stats.wins || 0)) || a.name.localeCompare(b.name)).slice(0, 10).map((t, i) => ({ rank: i + 1, label: t.name, team: t.name, value: `${t.stats.wins || 0} wins`, meta: `${t.stats.matches || 0} matches | ${t.stats.winRate || 0}%` }));
  return [];
}

function sourceUnion(...values) {
  const out = [];
  for (const v of values) {
    if (!v) continue;
    for (const p of String(v).split('+').map((x) => x.trim())) if (p && !out.includes(p)) out.push(p);
  }
  return out.join(' + ');
}

function playerInsight(player) {
  const s = player.stats || {};
  const r = Array.isArray(s.recentScores) ? s.recentScores : [];
  if (!r.length) return 'Recent batting trend sample is not available for this profile.';
  const rAvg = avg(r);
  return `${rAvg > (s.average || 0) ? 'Short-term batting sample is above' : rAvg < (s.average || 0) ? 'Short-term batting sample is below' : 'Short-term batting sample is near'} the listed baseline average (${round(rAvg, 1)} vs ${s.average || 0}).`;
}

function teamInsight(team) {
  return (team.stats?.matches || 0) ? 'Ask for recent matches or a comparison to convert the team profile into match-context analysis.' : 'This is a profile-level record with limited structured match stats.';
}

function playerPayload(wrapper, summary, extras = {}) {
  return { type: 'player', summary, insight: extras.insight || playerInsight(wrapper.entity), player: wrapper.entity, relatedMatches: playerRelated(wrapper), dataSource: wrapper.source, ...extras };
}

function teamPayload(wrapper, summary, extras = {}) {
  const t = teamView(wrapper);
  return { type: 'team', summary, insight: extras.insight || teamInsight(t), team: t, recentMatches: Array.isArray(t.recentMatches) ? t.recentMatches.slice(0, 5) : [], dataSource: wrapper.source, ...extras };
}

function matchPayload(record, cache, summary, extras = {}) {
  const match = record.source === 'csv' ? record.match : (cache?.matchById?.get?.(String(record.match.id || '')) || record.match);
  return { type: 'match', summary, insight: extras.insight || 'Use player/team queries to connect this match to wider form and trend analysis.', match, dataSource: record.source, ...extras };
}

function recentWinForm(recentMatches = [], teamName = '') {
  const list = Array.isArray(recentMatches) ? recentMatches.slice(0, 5) : [];
  if (!list.length || !teamName) return { winRate: 0.5, sample: 0 };
  let weightedWins = 0;
  let weightSum = 0;
  for (let i = 0; i < list.length; i += 1) {
    const match = list[i];
    const weight = Math.max(0.3, 1 - i * 0.12);
    const winner = String(match.winner || '').trim();
    const result = String(match.result || '').toLowerCase();
    const win = winner
      ? winner.toLowerCase() === String(teamName).toLowerCase()
      : result.startsWith(String(teamName).toLowerCase());
    weightedWins += (win ? 1 : 0) * weight;
    weightSum += weight;
  }
  return { winRate: weightSum ? weightedWins / weightSum : 0.5, sample: list.length };
}

function listAllMatchesForTeam(kb, teamName) {
  const out = [];
  for (const rec of kb.matchById.values()) {
    const teams = Array.isArray(rec.match?.teams) ? rec.match.teams : [];
    if (teams.some((t) => normalize(t) === normalize(teamName))) out.push(rec.match);
  }
  out.sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')));
  return out;
}

function headToHeadStats(kb, teamA, teamB) {
  let total = 0;
  let winsA = 0;
  let winsB = 0;
  for (const rec of kb.matchById.values()) {
    const teams = Array.isArray(rec.match?.teams) ? rec.match.teams.map((t) => normalize(t)) : [];
    if (!teams.includes(normalize(teamA)) || !teams.includes(normalize(teamB))) continue;
    total += 1;
    const winner = normalize(rec.match?.winner || '');
    if (winner === normalize(teamA)) winsA += 1;
    if (winner === normalize(teamB)) winsB += 1;
  }
  return { total, winsA, winsB };
}

function estimateTeamAvgScore(kb, wrapper) {
  const t = teamView(wrapper);
  const direct = Number(t.stats?.averageScore || 0);
  if (direct > 0) return direct;

  const matches = listAllMatchesForTeam(kb, t.name).slice(0, 20);
  const samples = [];
  for (const m of matches) {
    if (Array.isArray(m.innings) && m.innings.length) {
      const innings = m.innings.filter((inn) => normalize(inn.battingTeam) === normalize(t.name));
      for (const inn of innings) samples.push(Number(inn.runs || 0));
    } else if (Number.isFinite(Number(m.totalRuns))) {
      const teamsCount = Array.isArray(m.teams) && m.teams.length ? m.teams.length : 2;
      samples.push(Number(m.totalRuns) / Math.max(2, teamsCount));
    }
  }
  return samples.length ? avg(samples) : 0;
}

function teamBowlingStrengthIndex(team) {
  const top = Array.isArray(team.topBowlers) ? team.topBowlers.slice(0, 4) : [];
  if (!top.length) {
    return {
      index: Number(team.stats?.wins || 0) ? 0.52 : 0.45,
      estEconomy: Number(team.stats?.strikeRate || 0) ? round(12 - Number(team.stats.strikeRate) / 20, 2) : 7.8
    };
  }
  const avgEcon = avg(top.map((b) => Number(b.economy || 0)).filter((x) => x > 0)) || 7.5;
  const wickets = top.reduce((sum, b) => sum + Number(b.wickets || 0), 0);
  const econScore = clamp((9.5 - avgEcon) / 5, 0, 1);
  const wicketScore = clamp(wickets / 120, 0, 1);
  return { index: round(0.62 * econScore + 0.38 * wicketScore, 3), estEconomy: round(avgEcon, 2) };
}

function battingConsistencyMetrics(player) {
  const scores = Array.isArray(player.stats?.recentScores) ? player.stats.recentScores.map((x) => Number(x || 0)) : [];
  if (!scores.length) {
    return { sample: 0, mean: 0, sd: 0, cv: 0, label: 'Low', p30: 0, p50: 0 };
  }
  const mean = avg(scores);
  const sd = stddev(scores);
  const cv = mean > 0 ? sd / mean : 1;
  const p30Sample = scores.filter((s) => s >= 30).length / scores.length;
  const p50Sample = scores.filter((s) => s >= 50).length / scores.length;
  const consistencyScore = clamp(1 - cv, 0, 1);
  return {
    sample: scores.length,
    mean: round(mean, 1),
    sd: round(sd, 1),
    cv: round(cv, 2),
    label: consistencyScore >= 0.7 ? 'High' : consistencyScore >= 0.45 ? 'Medium' : 'Low',
    p30: round(p30Sample, 2),
    p50: round(p50Sample, 2)
  };
}

function playerTrendSignal(player) {
  const scores = Array.isArray(player.stats?.recentScores) ? player.stats.recentScores.map((x) => Number(x || 0)) : [];
  if (scores.length < 2) return { slope: 0, trend: 'stable' };
  let xSum = 0;
  let ySum = 0;
  let xySum = 0;
  let xxSum = 0;
  for (let i = 0; i < scores.length; i += 1) {
    const x = i + 1;
    const y = scores[i];
    xSum += x;
    ySum += y;
    xySum += x * y;
    xxSum += x * x;
  }
  const n = scores.length;
  const denom = n * xxSum - xSum * xSum;
  const slope = denom ? (n * xySum - xSum * ySum) / denom : 0;
  return { slope: round(slope, 2), trend: slope > 3 ? 'rising' : slope < -3 ? 'cooling' : 'stable' };
}

function predictionBaseConfidence({ source, matches, recentSamples, opponentSamples = 0, hasOpponent = false }) {
  const matchScore = clamp((Number(matches || 0) - 3) / 20, 0, 1);
  const recentScore = clamp(Number(recentSamples || 0) / 5, 0, 1);
  const oppScore = hasOpponent ? clamp(Number(opponentSamples || 0) / 6, 0, 1) : 0.5;
  const sourceScore = source === 'csv' ? 0.9 : 0.55;
  return round(0.34 * matchScore + 0.24 * recentScore + 0.22 * oppScore + 0.2 * sourceScore, 2);
}

function buildPredictionMeta(confidence, factors = []) {
  return {
    confidence: round(confidence, 2),
    confidenceLabel: confidenceLabel(confidence),
    factors: factors.slice(0, 6)
  };
}

function buildPlayerPredictionPayload({ kb, playerWrapper, teamWrapper = null, statKind = 'runs' }) {
  const player = playerWrapper.entity;
  const source = playerWrapper.source;
  const stats = player.stats || {};
  const scores = Array.isArray(stats.recentScores) ? stats.recentScores.map((x) => Number(x || 0)) : [];
  const recentAvg = scores.length ? avg(scores) : Number(stats.average || 0);
  const trend = playerTrendSignal(player);
  const consistency = battingConsistencyMetrics(player);
  const opponent = teamWrapper ? teamWrapper.entity : null;
  const splitRows = kb.cache?.playerOpponentSplits?.get?.(String(player.id || '')) || [];
  const oppSplitCandidate = opponent
    ? (splitRows.find((s) => normalize(s.opponent) === normalize(opponent.name))
      || splitRows
        .map((s) => ({ s, score: scoreName(opponent.name, s.opponent, 'team', { stats: { matches: s.matches } }) }))
        .sort((a, b) => b.score - a.score)[0]?.s
      || null)
    : null;
  const oppSplit = (oppSplitCandidate && (!opponent || scoreName(opponent.name, oppSplitCandidate.opponent, 'team', { stats: { matches: oppSplitCandidate.matches } }) >= 0.6))
    ? oppSplitCandidate
    : null;

  const factors = [];
  let analytics = [];
  let summary = '';
  let insight = '';
  let confidence = predictionBaseConfidence({
    source,
    matches: stats.matches || 0,
    recentSamples: scores.length,
    opponentSamples: oppSplit ? oppSplit.matches : 0,
    hasOpponent: Boolean(opponent)
  });

  if (statKind === 'wickets') {
    const wicketsPerMatch = (Number(stats.wickets || 0) && Number(stats.matches || 0))
      ? Number(stats.wickets || 0) / Math.max(1, Number(stats.matches || 0))
      : 0;
    const oppRate = oppSplit ? (Number(oppSplit.bowling?.wickets || 0) / Math.max(1, Number(oppSplit.matches || 1))) : null;
    const econ = Number(stats.economy || 0);
    const econAdj = econ ? clamp((7.6 - econ) / 8, -0.15, 0.12) : -0.03;
    const lambda = Math.max(0.05, wicketsPerMatch * 0.65 + (oppRate ?? wicketsPerMatch) * 0.35 + econAdj);
    const p1 = poissonProbAtLeast(lambda, 1);
    const p2 = poissonProbAtLeast(lambda, 2);
    const p3 = poissonProbAtLeast(lambda, 3);

    summary = `${player.name} next-match wicket forecast${opponent ? ` vs ${opponent.name}` : ''}: expected wickets ${round(lambda, 2)} with ~${Math.round(p2 * 100)}% probability of 2+ wickets.`;
    analytics = [
      { label: 'Expected wickets', value: round(lambda, 2) },
      { label: 'P(1+ wickets)', value: `${Math.round(p1 * 100)}%` },
      { label: 'P(2+ wickets)', value: `${Math.round(p2 * 100)}%` },
      { label: 'P(3+ wickets)', value: `${Math.round(p3 * 100)}%` },
      { label: 'Wkts/match', value: round(wicketsPerMatch, 2) },
      { label: 'Economy', value: stats.economy || 0 }
    ];
    factors.push(`Career wicket rate: ${round(wicketsPerMatch, 2)} wickets/match`);
    if (oppSplit) factors.push(`Opponent split vs ${oppSplit.opponent}: ${oppSplit.bowling?.wickets || 0} wickets in ${oppSplit.matches || 0} matches`);
    if (!oppSplit && opponent) factors.push(`Opponent wicket split not available for ${opponent.name} in current dataset`);
    if (stats.economy) factors.push(`Bowling economy (${stats.economy}) is used as a control variable`);
    insight = 'This is a probability-based forecast derived from wicket rate, economy, and available opponent splits. It is not a certainty.';
    confidence = Math.max(0.42, confidence - 0.04);
  } else {
    const careerAvg = Number(stats.average || 0);
    const strikeRate = Number(stats.strikeRate || 0);
    const recentWeight = scores.length ? 0.58 : 0.35;
    let expectedRuns = recentAvg * recentWeight + careerAvg * (1 - recentWeight);
    expectedRuns += clamp(trend.slope * 0.7, -6, 8);
    if (oppSplit && Number(oppSplit.batting?.average || 0) > 0) {
      expectedRuns += clamp((Number(oppSplit.batting.average) - careerAvg) * 0.22, -10, 12);
    }
    expectedRuns = round(clamp(expectedRuns, 2, 140), 1);

    let projectedSR = strikeRate || (careerAvg ? clamp(85 + careerAvg * 0.8, 60, 170) : 100);
    projectedSR += clamp(trend.slope * 0.5, -6, 8);
    if (oppSplit && Number(oppSplit.batting?.strikeRate || 0) > 0) {
      projectedSR = projectedSR * 0.72 + Number(oppSplit.batting.strikeRate) * 0.28;
    }
    projectedSR = round(clamp(projectedSR, 50, 220), 1);

    const variability = Math.max(8, consistency.sd || 16);
    const p30 = clamp(logistic((expectedRuns - 30) / (variability * 0.65)), 0.05, 0.98);
    const p50 = clamp(logistic((expectedRuns - 50) / (variability * 0.75)), 0.02, 0.95);
    const p100 = clamp(logistic((expectedRuns - 100) / (variability * 0.95)), 0.001, 0.35);

    if (statKind === 'strike_rate') {
      summary = `${player.name} next-match strike-rate projection${opponent ? ` vs ${opponent.name}` : ''}: around ${projectedSR} (probabilistic estimate, not a certainty).`;
      analytics = [
        { label: 'Projected SR', value: projectedSR },
        { label: 'Current SR', value: strikeRate || 0 },
        { label: 'Expected runs', value: expectedRuns },
        { label: 'Trend', value: trend.trend },
        { label: 'Consistency', value: consistency.label }
      ];
      factors.push(`Current strike rate: ${strikeRate || 0}`);
      factors.push(`Recent trend: ${trend.trend} (slope ${trend.slope})`);
      if (oppSplit && Number(oppSplit.batting?.strikeRate || 0) > 0) factors.push(`Opponent SR split vs ${oppSplit.opponent}: ${oppSplit.batting.strikeRate}`);
      insight = 'Strike-rate projection blends long-term strike rate with recent form trend and opponent split context when available.';
    } else if (statKind === 'consistency') {
      summary = `${player.name} consistency outlook: ${consistency.label} consistency based on recent innings variance. Estimated next-match range is roughly ${Math.max(0, round(expectedRuns - variability, 0))}-${round(expectedRuns + variability, 0)} runs.`;
      analytics = [
        { label: 'Consistency', value: consistency.label },
        { label: 'Recent mean', value: consistency.mean },
        { label: 'Std dev', value: consistency.sd },
        { label: 'CV', value: consistency.cv },
        { label: 'P(30+)', value: `${Math.round(p30 * 100)}%` },
        { label: 'P(50+)', value: `${Math.round(p50 * 100)}%` }
      ];
      factors.push(`Recent-innings sample size: ${scores.length || 0}`);
      factors.push(`Dispersion (SD): ${consistency.sd}`);
      factors.push(`Coefficient of variation: ${consistency.cv}`);
      insight = 'Consistency analysis uses recent-score dispersion and produces a probability range, not a guaranteed score.';
    } else if (statKind === 'form') {
      const riseProb = clamp(0.5 + (trend.slope / 20) + ((recentAvg - careerAvg) / 120), 0.08, 0.92);
      summary = `${player.name} form trend prediction: ${trend.trend} with ~${Math.round(riseProb * 100)}% probability of maintaining or improving current batting form next match. Expected runs: ${expectedRuns}.`;
      analytics = [
        { label: 'Trend', value: trend.trend },
        { label: 'Slope', value: trend.slope },
        { label: 'Recent avg', value: round(recentAvg, 1) },
        { label: 'Career avg', value: careerAvg || 0 },
        { label: 'Expected runs', value: expectedRuns }
      ];
      factors.push(`Recent average vs baseline: ${round(recentAvg, 1)} vs ${careerAvg || 0}`);
      factors.push(`Trend slope over recent innings: ${trend.slope}`);
      factors.push(`Consistency rating: ${consistency.label}`);
      insight = 'Form trend is a directional signal based on recent-score momentum and baseline batting average.';
    } else {
      summary = `${player.name} next-match run projection${opponent ? ` vs ${opponent.name}` : ''}: expected runs ${expectedRuns} with ~${Math.round(p50 * 100)}% probability of 50+ and ~${Math.round(p30 * 100)}% probability of 30+.`;
      analytics = [
        { label: 'Expected runs', value: expectedRuns },
        { label: 'P(30+)', value: `${Math.round(p30 * 100)}%` },
        { label: 'P(50+)', value: `${Math.round(p50 * 100)}%` },
        { label: 'P(100+)', value: `${Math.round(p100 * 100)}%` },
        { label: 'Projected SR', value: projectedSR },
        { label: 'Consistency', value: consistency.label }
      ];
      factors.push(`Career average: ${careerAvg || 0}`);
      factors.push(`Recent batting average (last ${scores.length || 0}): ${round(recentAvg, 1)}`);
      factors.push(`Trend: ${trend.trend} (slope ${trend.slope})`);
      if (oppSplit) factors.push(`Opponent split vs ${oppSplit.opponent}: avg ${oppSplit.batting?.average || 0}, SR ${oppSplit.batting?.strikeRate || 0}`);
      if (!oppSplit && opponent) factors.push(`No reliable opponent batting split found for ${opponent.name} in current dataset`);
      insight = 'This probability forecast blends historical average, recent form trend, and opponent split context when available.';
    }
  }

  const meta = buildPredictionMeta(confidence, factors);
  return {
    type: 'prediction',
    predictionKind: `player_${statKind}`,
    player,
    team: opponent && teamWrapper ? teamView(teamWrapper) : undefined,
    summary,
    insight,
    analytics,
    confidence: meta.confidence,
    confidenceLabel: meta.confidenceLabel,
    factors: meta.factors,
    dataSource: sourceUnion(source, opponent ? teamWrapper.source : null),
    relatedMatches: playerRelated(playerWrapper)
  };
}

function buildMatchPredictionPayload({ kb, leftTeamWrapper, rightTeamWrapper, tournamentProgression = false }) {
  const a = teamView(leftTeamWrapper);
  const b = teamView(rightTeamWrapper);
  const formA = recentWinForm(a.recentMatches || [], a.name);
  const formB = recentWinForm(b.recentMatches || [], b.name);
  const winA = Number(a.stats?.winRate || 0) / 100;
  const winB = Number(b.stats?.winRate || 0) / 100;
  const avgScoreA = estimateTeamAvgScore(kb, leftTeamWrapper);
  const avgScoreB = estimateTeamAvgScore(kb, rightTeamWrapper);
  const bowlA = teamBowlingStrengthIndex(a);
  const bowlB = teamBowlingStrengthIndex(b);
  const h2h = headToHeadStats(kb, a.name, b.name);
  const h2hEdgeA = h2h.total ? (h2h.winsA - h2h.winsB) / h2h.total : 0;

  const scoreA =
    (winA - winB) * 1.9 +
    (formA.winRate - formB.winRate) * 1.7 +
    ((avgScoreA - avgScoreB) / 100) * 0.9 +
    (bowlA.index - bowlB.index) * 1.1 +
    h2hEdgeA * 0.45;
  const pA = clamp(logistic(scoreA), 0.08, 0.92);
  const pB = round(1 - pA, 2);

  const expectedTotalA = round(clamp(avgScoreA + (formA.winRate - 0.5) * 14 - (bowlB.estEconomy - 7.2) * 5, 80, 260), 0);
  const expectedTotalB = round(clamp(avgScoreB + (formB.winRate - 0.5) * 14 - (bowlA.estEconomy - 7.2) * 5, 80, 260), 0);
  const winner = pA >= 0.5 ? a : b;
  const winnerProb = Math.max(pA, pB);

  const confidence = predictionBaseConfidence({
    source: leftTeamWrapper.source === 'csv' && rightTeamWrapper.source === 'csv' ? 'csv' : 'seed',
    matches: Math.max(a.stats?.matches || 0, b.stats?.matches || 0),
    recentSamples: Math.max(formA.sample, formB.sample),
    opponentSamples: h2h.total,
    hasOpponent: true
  });

  const analyticsBase = [
    { label: `${a.name} win prob`, value: `${Math.round(pA * 100)}%` },
    { label: `${b.name} win prob`, value: `${Math.round(pB * 100)}%` },
    { label: `${a.name} total`, value: expectedTotalA },
    { label: `${b.name} total`, value: expectedTotalB },
    { label: `${a.name} bowl econ`, value: bowlA.estEconomy },
    { label: `${b.name} bowl econ`, value: bowlB.estEconomy }
  ];

  const factors = [
    `${a.name} win rate ${round(winA * 100, 1)}% vs ${b.name} ${round(winB * 100, 1)}%`,
    `Recent form index ${round(formA.winRate * 100, 0)} vs ${round(formB.winRate * 100, 0)}`,
    `Projected totals ${expectedTotalA} vs ${expectedTotalB}`,
    `Bowling control proxy (econ): ${a.name} ${bowlA.estEconomy}, ${b.name} ${bowlB.estEconomy}`
  ];
  if (h2h.total) factors.push(`Head-to-head in dataset: ${h2h.total} matches (${h2h.winsA}-${h2h.winsB})`);
  else factors.push('No head-to-head sample available in the current dataset');

  if (tournamentProgression) {
    const progressA = clamp(0.18 + pA * 0.4 + winA * 0.22 + formA.winRate * 0.2, 0.08, 0.9);
    const progressB = clamp(0.18 + pB * 0.4 + winB * 0.22 + formB.winRate * 0.2, 0.08, 0.9);
    const favored = progressA >= progressB ? a : b;
    const favoredProb = progressA >= progressB ? progressA : progressB;
    return {
      type: 'prediction',
      predictionKind: 'team_progression',
      teams: [a, b],
      summary: `Tournament progression likelihood (heuristic): ${favored.name} has the stronger progression outlook at ~${Math.round(favoredProb * 100)}% relative probability in this model.`,
      insight: 'This is not a live standings simulation. It is a comparative progression proxy using win rate, recent form, and projected match strength.',
      analytics: [
        { label: `${a.name} progression`, value: `${Math.round(progressA * 100)}%` },
        { label: `${b.name} progression`, value: `${Math.round(progressB * 100)}%` },
        ...analyticsBase.slice(2, 6)
      ],
      confidence: round(confidence, 2),
      confidenceLabel: confidenceLabel(confidence),
      factors,
      dataSource: sourceUnion(leftTeamWrapper.source, rightTeamWrapper.source)
    };
  }

  return {
    type: 'prediction',
    predictionKind: 'match_outcome',
    teams: [a, b],
    summary: `Match outcome prediction: ${winner.name} is favored with ~${Math.round(winnerProb * 100)}% win probability (probabilistic estimate, not a certainty).`,
    insight: `Projected team totals are ${a.name} ${expectedTotalA} and ${b.name} ${expectedTotalB}. Bowling effectiveness forecast uses top-bowler economy/wicket signals and team results as proxies.`,
    analytics: analyticsBase,
    confidence: round(confidence, 2),
    confidenceLabel: confidenceLabel(confidence),
    factors,
    dataSource: sourceUnion(leftTeamWrapper.source, rightTeamWrapper.source)
  };
}

function buildTopPredictedPerformerPayload(kb) {
  const ranked = kb.players
    .map((wrapper) => {
      const p = wrapper.entity;
      const s = p.stats || {};
      const recent = Array.isArray(s.recentScores) ? s.recentScores : [];
      const recentAvg = recent.length ? avg(recent) : Number(s.average || 0);
      const trend = playerTrendSignal(p);
      const consistency = battingConsistencyMetrics(p);
      const wicketsRate = (Number(s.wickets || 0) && Number(s.matches || 0))
        ? Number(s.wickets || 0) / Math.max(1, Number(s.matches || 0))
        : 0;
      const battingImpact = recentAvg * 0.62 + Number(s.average || 0) * 0.38;
      const bowlingImpact = wicketsRate * 22 + (Number(s.economy || 0) ? clamp((8.5 - Number(s.economy || 0)) * 3, -6, 8) : 0);
      const consistencyBonus = consistency.label === 'High' ? 4 : consistency.label === 'Medium' ? 2 : 0;
      const trendBonus = trend.trend === 'rising' ? 5 : trend.trend === 'cooling' ? -3 : 0;
      const impact = round(battingImpact + bowlingImpact + consistencyBonus + trendBonus, 1);
      const conf = predictionBaseConfidence({
        source: wrapper.source,
        matches: s.matches || 0,
        recentSamples: recent.length,
        hasOpponent: false
      });
      return { wrapper, impact, recentAvg: round(recentAvg, 1), wicketsRate: round(wicketsRate, 2), conf };
    })
    .sort((a, b) => (b.impact - a.impact) || (b.conf - a.conf) || a.wrapper.entity.name.localeCompare(b.wrapper.entity.name));

  const rows = ranked.slice(0, 8).map((row, i) => ({
    rank: i + 1,
    label: row.wrapper.entity.name,
    team: row.wrapper.entity.team || '',
    value: `Impact ${row.impact}`,
    meta: `Pred conf ${confidenceLabel(row.conf)} | Recent avg ${row.recentAvg} | Wkts/m ${row.wicketsRate}`
  }));

  const avgConf = ranked.length ? round(avg(ranked.slice(0, 8).map((r) => r.conf)), 2) : 0.5;
  return {
    type: 'prediction',
    predictionKind: 'top_predicted_performers',
    title: 'Top Predicted Performers (Next Appearance)',
    rows,
    summary: 'Top predicted performers based on historical output, recent form trend, all-round contribution, and consistency. This is a next-appearance ranking, not a live fixture schedule forecast.',
    insight: "If you know today's fixtures, combine this ranking with opponent-specific queries for better precision.",
    analytics: rows.slice(0, 3).map((r) => ({ label: `#${r.rank}`, value: r.label })),
    confidence: avgConf,
    confidenceLabel: confidenceLabel(avgConf),
    factors: [
      'Weighted recent batting average and baseline average',
      'All-round contribution (wicket rate + economy)',
      'Recent trend signal from last innings sample',
      'Consistency score from variance in recent scores'
    ],
    dataSource: kb.cache ? 'csv' : 'seed'
  };
}

function parsePredictionIntent(text = '') {
  const q = normalize(text);
  if (!q) return null;

  if (/\btop\b.*\bpredicted\b.*\bperformer/.test(q) || /\btop predicted performer\b/.test(q)) {
    return { kind: 'top_performer' };
  }

  let m = text.match(/^(?:who\s+will\s+win\s+)(.+?)\s+(?:vs|versus)\s+(.+?)\??$/i);
  if (m) return { kind: 'match_outcome', left: m[1].trim(), right: m[2].trim() };

  m = text.match(/^(.+?)\s+(?:vs|versus)\s+(.+?)\s+(?:prediction|win probability|forecast)\??$/i);
  if (m) return { kind: 'match_outcome', left: m[1].trim(), right: m[2].trim() };

  m = text.match(/^(.+?)\s+(?:vs|versus)\s+(.+?)\s+(?:tournament\s+progression\s+likelihood|tournament\s+progression|progression likelihood|qualification chances?)\??$/i);
  if (m) return { kind: 'team_progression_compare', left: m[1].trim(), right: m[2].trim() };

  m = text.match(/^(.+?)\s+(?:tournament\s+progression\s+likelihood|tournament\s+progression|progression likelihood|qualification chances?|advance chances?)\??$/i);
  if (m) return { kind: 'team_progression_single', team: m[1].trim() };

  m = text.match(/^(?:how many\s+)?runs\s+(?:might|will|can|could|should|is\s+expected\s+to)\s+(.+?)\s+(?:score|make)\s+next\s+match\??$/i)
    || text.match(/^(.+?)\s+next\s+match\s+runs?\s+(?:predictions?|forecasts?|projections?)\??$/i)
    || text.match(/^expected\s+runs?\s+for\s+(.+?)\s+next\s+match\??$/i);
  if (m) return { kind: 'player_runs_next', player: m[1].trim() };

  m = text.match(/^(?:how many\s+)?wickets\s+(?:might|will|can|could|should|is\s+expected\s+to)\s+(.+?)\s+(?:take|get)\s+next\s+match\??$/i)
    || text.match(/^(.+?)\s+next\s+match\s+wickets?\s+(?:predictions?|forecasts?|projections?)\??$/i)
    || text.match(/^expected\s+wickets?\s+for\s+(.+?)\s+next\s+match\??$/i);
  if (m) return { kind: 'player_wickets_next', player: m[1].trim() };

  m = text.match(/^(.+?)\s+(runs|wickets|strike rate|sr)\s+(?:predictions?|projections?|forecasts?)\s+(?:vs|versus|against)\s+(.+?)\??$/i);
  if (m) return { kind: `player_${normalize(m[2]).replace(' ', '_')}_next_vs`, player: m[1].trim(), team: m[3].trim() };

  m = text.match(/^(?:how many\s+)?(runs|wickets)\s+might\s+(.+?)\s+(?:score|take)\s+(?:vs|versus|against)\s+(.+?)\??$/i);
  if (m) return { kind: `player_${normalize(m[1])}_next_vs`, player: m[2].trim(), team: m[3].trim() };

  m = text.match(/^(.+?)\s+(?:form trend prediction|form prediction|trend prediction)\??$/i);
  if (m) return { kind: 'player_form_prediction', player: m[1].trim() };
  if (/^(?:form trend prediction|form prediction|trend prediction)\??$/i.test(text)) return { kind: 'player_form_prediction_context' };

  m = text.match(/^(.+?)\s+(?:strike rate projections?|strike rate predictions?|sr projections?|sr predictions?)\??$/i);
  if (m) return { kind: 'player_strike_rate_prediction', player: m[1].trim() };

  m = text.match(/^(.+?)\s+(?:consistency analysis|consistency forecast|consistency prediction)\??$/i);
  if (m) return { kind: 'player_consistency_prediction', player: m[1].trim() };

  return null;
}

function buildSingleTeamProgressionPayload(wrapper, kb) {
  const team = teamView(wrapper);
  const winRate = Number(team.stats?.winRate || 0) / 100;
  const form = recentWinForm(team.recentMatches || [], team.name);
  const avgScore = estimateTeamAvgScore(kb, wrapper);
  const bowl = teamBowlingStrengthIndex(team);
  const score = winRate * 0.42 + form.winRate * 0.34 + clamp(avgScore / 220, 0, 1) * 0.12 + bowl.index * 0.12;
  const prob = clamp(0.15 + score * 0.72, 0.08, 0.9);
  const confidence = predictionBaseConfidence({
    source: wrapper.source,
    matches: team.stats?.matches || 0,
    recentSamples: form.sample,
    hasOpponent: false
  });

  return {
    type: 'prediction',
    predictionKind: 'team_progression',
    team,
    summary: `${team.name} tournament progression likelihood (heuristic, dataset-based): ~${Math.round(prob * 100)}% relative progression probability in this model.`,
    insight: 'This is not a live tournament table simulation. It is a probability proxy derived from win rate, recent form, scoring strength, and bowling indicators.',
    analytics: [
      { label: 'Progression prob', value: `${Math.round(prob * 100)}%` },
      { label: 'Win rate', value: `${round(winRate * 100, 1)}%` },
      { label: 'Recent form', value: `${round(form.winRate * 100, 0)}%` },
      { label: 'Avg score', value: round(avgScore, 0) },
      { label: 'Bowling index', value: bowl.index }
    ],
    confidence: round(confidence, 2),
    confidenceLabel: confidenceLabel(confidence),
    factors: [
      `Win rate sample: ${round(winRate * 100, 1)}%`,
      `Recent form index from last ${form.sample || 0} matches: ${round(form.winRate * 100, 0)}%`,
      `Estimated scoring strength: ${round(avgScore, 0)}`,
      `Bowling control proxy: economy ${bowl.estEconomy}`
    ],
    dataSource: wrapper.source,
    recentMatches: Array.isArray(team.recentMatches) ? team.recentMatches.slice(0, 5) : []
  };
}

function buildPredictionQueryPayload({ kb, session, text, resolvePlayer, resolveTeam }) {
  const intent = parsePredictionIntent(text);
  if (!intent) return null;

  if (intent.kind === 'top_performer') return buildTopPredictedPerformerPayload(kb);

  if (intent.kind === 'match_outcome' || intent.kind === 'team_progression_compare') {
    const lt = resolveTeam(intent.left);
    const rt = resolveTeam(intent.right);
    if (lt.status === 'ambiguous' || lt.status === 'uncertain') return clarify('team', intent.left, lt);
    if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', intent.right, rt);
    if (lt.status !== 'resolved' || rt.status !== 'resolved') {
      return {
        type: 'clarification',
        summary: 'I detected a predictive match query but could not resolve both teams confidently.',
        entityType: 'team',
        suggestions: ['Who will win India vs Australia?', 'India vs England win probability'],
        dataSource: kb.cache ? 'csv' : 'seed'
      };
    }
    return buildMatchPredictionPayload({
      kb,
      leftTeamWrapper: lt.top,
      rightTeamWrapper: rt.top,
      tournamentProgression: intent.kind === 'team_progression_compare'
    });
  }

  if (intent.kind === 'team_progression_single') {
    const rt = resolveTeam(intent.team, true);
    if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', intent.team, rt);
    if (rt.status === 'resolved') return buildSingleTeamProgressionPayload(rt.top, kb);
    return {
      type: 'clarification',
      summary: `I could not resolve the team "${intent.team}" confidently for a progression prediction.`,
      entityType: 'team',
      suggestions: ['India tournament progression likelihood', 'Australia progression likelihood'],
      dataSource: kb.cache ? 'csv' : 'seed'
    };
  }

  const playerRef = intent.player || session.context.player;
  if (!playerRef) {
    return {
      type: 'clarification',
      summary: 'Please specify a player for the prediction query.',
      entityType: 'player',
      suggestions: ['How many runs might Virat Kohli score next match?', 'Bumrah wickets next match', 'Rohit Sharma consistency analysis'],
      dataSource: kb.cache ? 'csv' : 'seed'
    };
  }

  const rp = resolvePlayer(playerRef, true);
  if (rp.status === 'ambiguous' || rp.status === 'uncertain') return clarify('player', playerRef, rp);
  if (rp.status !== 'resolved') {
    return {
      type: 'clarification',
      summary: `I could not resolve "${playerRef}" confidently for a prediction query. Please use a more specific player name.`,
      entityType: 'player',
      suggestions: ['Virat Kohli', 'Rohit Sharma', 'Jasprit Bumrah'],
      dataSource: kb.cache ? 'csv' : 'seed'
    };
  }

  let teamWrapper = null;
  if (intent.team) {
    const rt = resolveTeam(intent.team, true);
    if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', intent.team, rt);
    if (rt.status === 'resolved') teamWrapper = rt.top;
    else {
      return {
        type: 'clarification',
        summary: `I could not resolve the opponent "${intent.team}" confidently for the prediction query.`,
        entityType: 'team',
        suggestions: ['Australia', 'India', 'Pakistan'],
        dataSource: kb.cache ? 'csv' : 'seed'
      };
    }
  }

  let statKind = 'runs';
  if (intent.kind.includes('wickets')) statKind = 'wickets';
  else if (intent.kind.includes('strike_rate') || intent.kind.includes('sr_')) statKind = 'strike_rate';
  else if (intent.kind.includes('consistency')) statKind = 'consistency';
  else if (intent.kind.includes('form')) statKind = 'form';

  return buildPlayerPredictionPayload({ kb, playerWrapper: rp.top, teamWrapper, statKind });
}

function parseLiveIntent(text = '') {
  const q = normalize(text);
  if (!q) return null;

  const isLiveish =
    /\blive\b/.test(q) ||
    /\bcurrent match\b/.test(q) ||
    /\bmatch momentum\b/.test(q) ||
    /\bmomentum\b/.test(q) ||
    /\bwho is batting now\b/.test(q) ||
    /\bwho s batting now\b/.test(q) ||
    /\brequired run rate\b/.test(q) ||
    /\brrr\b/.test(q) ||
    /\bball by ball\b/.test(q) ||
    /\bcommentary\b/.test(q);

  if (!isLiveish) return null;

  let mode = 'score';
  if (/\bwho is batting now\b|\bwho s batting now\b|\bbatting now\b/.test(q)) mode = 'batters';
  else if (/\bmomentum\b/.test(q)) mode = 'momentum';
  else if (/\brequired run rate\b|\brrr\b/.test(q)) mode = 'rrr';
  else if (/\bball by ball\b|\bcommentary\b/.test(q)) mode = 'commentary';
  else if (/\bstatus\b/.test(q)) mode = 'status';
  else if (/\blive score\b/.test(q)) mode = 'score';

  return { mode };
}

function formatLiveInnings(innings = []) {
  if (!Array.isArray(innings) || !innings.length) return 'Score unavailable';
  return innings
    .slice(0, 3)
    .map((inn) => `${inn.team || 'Team'} ${inn.runs ?? 0}/${inn.wickets ?? 0}${inn.overs != null ? ` (${inn.overs} ov)` : ''}`)
    .join(' | ');
}

function buildLiveHistoricalContext(kb, liveMatch, resolveTeam) {
  const wrappers = [];
  for (const teamName of liveMatch?.teams || []) {
    const result = resolveTeam(teamName);
    if (result.status === 'resolved') wrappers.push(result.top);
  }

  let contextInsight = '';
  if (wrappers.length >= 2) {
    const a = teamView(wrappers[0]);
    const b = teamView(wrappers[1]);
    const h2h = headToHeadStats(kb, a.name, b.name);
    contextInsight = `${a.name} historical sample win rate ${a.stats?.winRate || 0}% vs ${b.name} ${b.stats?.winRate || 0}%.`;
    if (h2h.total) {
      contextInsight += ` Indexed head-to-head sample: ${h2h.total} matches (${a.name} ${h2h.winsA}, ${b.name} ${h2h.winsB}).`;
    }
    return { teams: [a, b], insight: contextInsight };
  }

  if (wrappers.length === 1) {
    const t = teamView(wrappers[0]);
    contextInsight = `${t.name} historical sample: ${t.stats?.matches || 0} matches, ${t.stats?.wins || 0} wins, win rate ${t.stats?.winRate || 0}%.`;
    return { teams: [t], insight: contextInsight };
  }

  return { teams: [], insight: '' };
}

function buildLiveModeSummary(mode, liveSnapshot) {
  const match = liveSnapshot.match;
  const progress = liveSnapshot.progress || {};
  const current = progress.current || {};
  const inningsLine = formatLiveInnings(match?.innings || []);
  const statusText = String(match?.statusText || 'Status unavailable');
  const title = match?.shortTitle || 'Current match';

  if (mode === 'status') {
    return `Current match status: ${title} | ${statusText}. ${inningsLine}.`;
  }
  if (mode === 'batters') {
    const batters = (match?.batters || []).slice(0, 2);
    if (!batters.length) {
      return `Who is batting now: live batter details are not available from the current feed for ${title}. ${statusText}.`;
    }
    return `Who is batting now: ${batters.map((b) => `${b.name} ${b.runs ?? 0}${b.balls != null ? ` (${b.balls})` : ''}`).join(', ')}. ${title} | ${statusText}.`;
  }
  if (mode === 'momentum') {
    const m = liveSnapshot.momentum || {};
    return `Match momentum: ${m.label || 'Balanced'}${m.battingTeam ? ` (${m.battingTeam} batting)` : ''}. ${title} | ${statusText}.`;
  }
  if (mode === 'rrr') {
    if (current.requiredRunRate != null || current.currentRunRate != null || current.runsNeeded != null) {
      return `Required run-rate view: ${title} | CRR ${current.currentRunRate ?? 'N/A'} | RRR ${current.requiredRunRate ?? 'N/A'}${current.runsNeeded != null ? ` | Runs needed ${current.runsNeeded}` : ''}${current.ballsRemaining != null ? ` off ${current.ballsRemaining}` : ''}.`;
    }
    return `Required run-rate details are not available from the current live feed for ${title}. ${statusText}.`;
  }
  if (mode === 'commentary') {
    const latest = (match?.commentary || []).slice(0, 3);
    if (!latest.length) return `Ball-by-ball updates are not available from the current feed for ${title}. ${statusText}.`;
    return `Live commentary updates: ${title} | ${statusText}. Latest: ${latest.map((c) => c.text).join(' | ')}`;
  }

  return `Live score: ${title} | ${inningsLine}. ${statusText}.`;
}

async function buildLiveQueryPayload({ cache, session, text }) {
  const intent = parseLiveIntent(text);
  if (!intent) return null;

  const kb = mergeKnowledge(cache);
  const resolveTeam = (ref, allowContext = false) =>
    resolveEntity(kb.teams, String(ref || '').trim() || (allowContext ? session.context.team : ''), 'team');
  const liveSnapshot = await liveCricketService.getSnapshot({ query: text });

  if (!liveSnapshot?.available || !liveSnapshot.match) {
    const fallbackTeams = [];
    const vs = text.match(/(.+?)\s+(?:vs|versus)\s+(.+)/i);
    if (vs) {
      const left = resolveTeam(vs[1]);
      const right = resolveTeam(vs[2]);
      if (left.status === 'resolved') fallbackTeams.push(teamView(left.top));
      if (right.status === 'resolved') fallbackTeams.push(teamView(right.top));
    }

    const analytics = [
      { label: 'Live feed', value: 'Unavailable' },
      { label: 'Configured', value: liveSnapshot?.configured ? 'Yes' : 'No' }
    ];

    let insight = 'Live data is unavailable, so I am falling back to historical analytics and recent indexed/sample matches.';
    if (fallbackTeams.length >= 2) {
      const h2h = headToHeadStats(kb, fallbackTeams[0].name, fallbackTeams[1].name);
      insight = `${fallbackTeams[0].name} vs ${fallbackTeams[1].name}: historical sample win rates are ${fallbackTeams[0].stats?.winRate || 0}% vs ${fallbackTeams[1].stats?.winRate || 0}%.`;
      if (h2h.total) insight += ` Indexed head-to-head sample: ${h2h.total} matches (${fallbackTeams[0].name} ${h2h.winsA}, ${fallbackTeams[1].name} ${h2h.winsB}).`;
    }

    return {
      type: 'live',
      liveAvailable: false,
      liveMode: intent.mode,
      summary: `Live data unavailable: ${liveSnapshot?.message || 'No live feed response available right now.'}`,
      insight,
      analytics,
      live: {
        available: false,
        configured: Boolean(liveSnapshot?.configured),
        message: liveSnapshot?.message || null,
        fetchedAt: liveSnapshot?.fetchedAt || null
      },
      teams: fallbackTeams.length ? fallbackTeams : undefined,
      dataSource: sourceUnion(fallbackTeams.length ? 'seed' : null)
    };
  }

  const historicalContext = buildLiveHistoricalContext(kb, liveSnapshot.match, resolveTeam);
  const progress = liveSnapshot.progress || {};
  const current = progress.current || {};
  const momentum = liveSnapshot.momentum || {};
  const playerHighlights = liveSnapshot.playerHighlights || [];
  const analytics = [
    { label: 'Status', value: liveSnapshot.match.statusText || 'Live' },
    { label: 'CRR', value: current.currentRunRate ?? 'N/A' },
    { label: 'RRR', value: current.requiredRunRate ?? 'N/A' },
    { label: 'Runs needed', value: current.runsNeeded ?? 'N/A' },
    { label: 'Balls left', value: current.ballsRemaining ?? 'N/A' },
    { label: 'Momentum', value: momentum.label || 'Balanced' }
  ];

  const commentaryNote = (liveSnapshot.match.commentary || []).slice(0, 2).map((c) => c.text).join(' | ');
  let insight = `${momentum.explanation || 'Momentum is estimated from live scoring patterns.'}`;
  if (historicalContext.insight) insight += ` ${historicalContext.insight}`;
  if (commentaryNote && intent.mode !== 'commentary') insight += ` Latest events: ${commentaryNote}`;

  return {
    type: 'live',
    liveAvailable: true,
    liveMode: intent.mode,
    summary: buildLiveModeSummary(intent.mode, liveSnapshot),
    insight,
    analytics,
    live: {
      available: true,
      configured: true,
      fetchedAt: liveSnapshot.fetchedAt,
      stale: Boolean(liveSnapshot.stale),
      message: liveSnapshot.message || null,
      match: liveSnapshot.match,
      progress: liveSnapshot.progress,
      momentum: liveSnapshot.momentum,
      playerHighlights: liveSnapshot.playerHighlights,
      commentary: (liveSnapshot.match.commentary || []).slice(0, 8)
    },
    match: liveSnapshot.match,
    teams: historicalContext.teams.length ? historicalContext.teams : undefined,
    playerHighlights,
    commentary: (liveSnapshot.match.commentary || []).slice(0, 8),
    dataSource: sourceUnion('live', historicalContext.teams.length ? (cache ? 'csv' : 'seed') : null)
  };
}

function buildPayload(query, session, cache) {
  const kb = mergeKnowledge(cache);
  const text = String(query || '').trim();
  const q = normalize(text);
  if (!text) return { type: 'unsupported', summary: 'Please enter a player, team, match id, comparison, or leaderboard query.', suggestions: ['Virat Kohli', 'India', 'match 1082591', 'top run scorers'], dataSource: cache ? 'csv' : 'seed' };

  const lb = leaderboardIntent(text);
  if (lb) {
    const rows = kb.leaderboards?.[lb.key] || fallbackLeaderboardRows(lb.key, kb);
    return { type: 'leaderboard', title: lb.title, rows, summary: `${lb.title} from the ${kb.leaderboards?.[lb.key] ? 'indexed CSV' : 'available profiles'}.`, insight: rows[0] ? `${rows[0].label} leads this current view.` : 'No rows available yet.', dataSource: kb.leaderboards?.[lb.key] ? 'csv' : 'seed' };
  }

  const matchId = (text.match(/\bmatch\s+(\d{5,})\b/i) || text.match(/^\d{5,}$/))?.[1] || (text.match(/^\d{5,}$/) ? text : null);
  if (matchId) {
    const rec = kb.matchById.get(String(matchId));
    if (!rec) return { type: 'unsupported', summary: `I could not find match ${matchId} in the current data.`, suggestions: ['match 1082591', 'India recent matches'], dataSource: cache ? 'csv' : 'seed' };
    return matchPayload(rec, cache, `${(rec.match.teams || []).join(' vs ')} on ${rec.match.date || 'unknown date'} at ${rec.match.venue || 'unknown venue'}. ${rec.match.result || 'Result unavailable'}.`);
  }

  const resolvePlayer = (ref, allowContext = false) => resolveEntity(kb.players, String(ref || '').trim() || (allowContext ? session.context.player : ''), 'player');
  const resolveTeam = (ref, allowContext = false) => resolveEntity(kb.teams, String(ref || '').trim() || (allowContext ? session.context.team : ''), 'team');

  const predictionPayload = buildPredictionQueryPayload({ kb, session, text, resolvePlayer, resolveTeam });
  if (predictionPayload) return predictionPayload;

  let m = text.match(/^(?:last|latest)\s+(.+?)\s+match(?:\s+result)?\??$/i) || text.match(/^(.+?)\s+last\s+match(?:\s+result)?\??$/i);
  if (!m && /^(?:last|latest)\s+match(?:\s+result)?\??$/i.test(text) && session.context.team) m = [text, session.context.team];
  if (m) {
    const rt = resolveTeam(m[1], true);
    if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', m[1], rt);
    if (rt.status === 'resolved') {
      const recent = teamRecent(rt.top);
      if (!recent.length) return teamPayload(rt.top, `${rt.top.entity.name} is available, but no recent matches are indexed for this team yet.`);
      const first = recent[0];
      const rec = kb.matchById.get(String(first.id || '')) || { source: rt.top.source, match: first };
      return matchPayload(rec, cache, `Last ${rt.top.entity.name} match result: ${(rec.match.teams || []).join(' vs ')} on ${rec.match.date || 'unknown date'} - ${rec.match.result || 'Result unavailable'}.`);
    }
  }

  m = text.match(/^(.+?)\s+recent\s+matches\??$/i);
  if (!m && /^recent\s+matches\??$/i.test(text) && session.context.team) m = [text, session.context.team];
  if (m) {
    const rt = resolveTeam(m[1], true);
    if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', m[1], rt);
    if (rt.status === 'resolved') return teamPayload(rt.top, `${rt.top.entity.name} recent matches loaded from the ${rt.top.source === 'csv' ? 'indexed CSV' : 'sample feed'}.`, { insight: 'Review the fixtures below for recency and opposition quality before drawing form conclusions.' });
  }

  m = text.match(/^(.+?)\s+team\s+stats\??$/i);
  if (!m && /^team\s+stats\??$/i.test(text) && session.context.team) m = [text, session.context.team];
  if (m) {
    const rt = resolveTeam(m[1], true);
    if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', m[1], rt);
    if (rt.status === 'resolved') {
      const t = teamView(rt.top);
      return teamPayload(rt.top, `${t.name}: ${t.stats?.matches || 0} matches, ${t.stats?.wins || 0} wins, win rate ${t.stats?.winRate || 0}%.`);
    }
  }

  m = text.match(/^(.+?)\s+(runs|wickets|average|strike rate|sr|economy)\s+(?:vs|versus|against)\s+(.+?)\??$/i)
    || text.match(/^(?:how many\s+)?(runs|wickets)\s+did\s+(.+?)\s+(?:score|scored|take|took|get|got)\s+(?:vs|versus|against)\s+(.+?)\??$/i)
    || (session.context.player ? (text.match(/^(runs|wickets|average|strike rate|sr|economy)\s+(?:vs|versus|against)\s+(.+?)\??$/i) || null) : null);
  if (m) {
    const patternWithContext = m.length === 3 && session.context.player;
    const firstIsStat = /^(runs|wickets|average|strike rate|sr|economy)$/i.test(String(m[1] || ''));
    const statCapture = patternWithContext ? m[1] : (firstIsStat ? m[1] : m[2]);
    const playerRef = patternWithContext ? session.context.player : (firstIsStat ? m[2] : m[1]);
    const teamRef = patternWithContext ? m[2] : m[3];
    const statKey = normalize(statCapture).replace(' ', '_');
    const rp = resolvePlayer(playerRef, true);
    if (rp.status === 'ambiguous' || rp.status === 'uncertain') return clarify('player', playerRef, rp);
    const rt = resolveTeam(teamRef, true);
    if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', teamRef, rt);
    if (rp.status === 'resolved' && rt.status === 'resolved') {
      const splitRows = cache?.playerOpponentSplits?.get?.(String(rp.top.entity.id || '')) || [];
      const split = splitRows.find((s) => normalize(s.opponent) === normalize(rt.top.entity.name))
        || splitRows.sort((a, b) => scoreName(rt.top.entity.name, b.opponent, 'team', { stats: { matches: b.matches } }) - scoreName(rt.top.entity.name, a.opponent, 'team', { stats: { matches: a.matches } }))[0]
        || null;
      if (!split || scoreName(rt.top.entity.name, split.opponent, 'team', { stats: { matches: split.matches } }) < 0.6) {
        return playerPayload(rp.top, `I could not find an indexed ${rp.top.entity.name} vs ${rt.top.entity.name} split in the current CSV cache.`, { insight: 'This usually means the active dataset covers a different competition or the player/opponent pairing is absent.', analytics: [{ label: 'Opponent', value: rt.top.entity.name }, { label: 'Indexed split', value: 'Not available' }], dataSource: sourceUnion(rp.top.source, rt.top.source) });
      }
      const bat = split.batting || {};
      const bowl = split.bowling || {};
      let summary = `${rp.top.entity.name} vs ${split.opponent}: ${bat.runs || 0} runs and ${bowl.wickets || 0} wickets in ${split.matches || 0} indexed matches.`;
      if (statKey === 'wickets') summary = `${rp.top.entity.name} has ${bowl.wickets || 0} wickets vs ${split.opponent} in ${split.matches || 0} indexed matches (econ ${bowl.economy || 0}).`;
      if (statKey === 'runs') summary = `${rp.top.entity.name} has ${bat.runs || 0} runs vs ${split.opponent} in ${split.matches || 0} indexed matches (avg ${bat.average || 0}, SR ${bat.strikeRate || 0}).`;
      if (statKey === 'average') summary = `${rp.top.entity.name} batting average vs ${split.opponent}: ${bat.average || 0} across ${split.matches || 0} indexed matches.`;
      if (statKey === 'strike_rate' || statKey === 'sr') summary = `${rp.top.entity.name} strike rate vs ${split.opponent}: ${bat.strikeRate || 0} across ${split.matches || 0} indexed matches.`;
      if (statKey === 'economy') summary = `${rp.top.entity.name} economy vs ${split.opponent}: ${bowl.economy || 0} across ${split.matches || 0} indexed matches (${bowl.wickets || 0} wickets).`;
      return playerPayload(rp.top, summary, { insight: 'Opponent splits are aggregated from indexed meetings in the current CSV cache.', analytics: [{ label: 'Opponent', value: split.opponent }, { label: 'Matches', value: split.matches || 0 }, { label: 'Runs', value: bat.runs || 0 }, { label: 'Wickets', value: bowl.wickets || 0 }] , dataSource: sourceUnion('csv', rp.top.source, rt.top.source) });
    }
  }

  const cmp = text.match(/^(.+?)\s+(?:vs|versus)\s+(.+)$/i) || text.match(/^compare\s+(.+?)\s+(?:with|and)\s+(.+)$/i);
  if (cmp && !/\b(runs|wickets|average|strike rate|sr|economy)\s+(?:vs|versus|against)\b/i.test(q)) {
    const left = cmp[1].trim();
    const right = cmp[2].trim();
    const lp = resolvePlayer(left), rp = resolvePlayer(right);
    const lt = resolveTeam(left), rt = resolveTeam(right);
    const playerReady = lp.status === 'resolved' && rp.status === 'resolved';
    const teamReady = lt.status === 'resolved' && rt.status === 'resolved';
    if (teamReady && !playerReady) {
      const a = teamView(lt.top), b = teamView(rt.top);
      return { type: 'comparison', comparisonKind: 'team', teams: [a, b], summary: `${a.name} vs ${b.name}: ${((a.stats?.winRate || 0) === (b.stats?.winRate || 0)) ? 'win-rate sample is evenly matched' : `${(a.stats?.winRate || 0) > (b.stats?.winRate || 0) ? a.name : b.name} has the stronger win-rate sample`} (${a.stats?.winRate || 0}% vs ${b.stats?.winRate || 0}%).`, insight: 'Check recent matches to validate opposition quality and recency.', dataSource: sourceUnion(lt.top.source, rt.top.source) };
    }
    if (playerReady && !teamReady) {
      const a = lp.top.entity, b = rp.top.entity;
      return { type: 'comparison', comparisonKind: 'player', players: [a, b], summary: `${a.name} vs ${b.name}: ${(a.stats?.average || 0) === (b.stats?.average || 0) ? 'listed averages are close' : `${(a.stats?.average || 0) > (b.stats?.average || 0) ? a.name : b.name} leads on listed average`} and ${avg(a.stats?.recentScores || []) === avg(b.stats?.recentScores || []) ? 'recent batting samples are similar' : `${avg(a.stats?.recentScores || []) > avg(b.stats?.recentScores || []) ? a.name : b.name} has the stronger recent batting sample`}.`, insight: 'Use opponent or recent-form queries next for a more tactical comparison.', dataSource: sourceUnion(lp.top.source, rp.top.source) };
    }
    if (lp.status === 'ambiguous' || lp.status === 'uncertain') return clarify('player', left, lp);
    if (rp.status === 'ambiguous' || rp.status === 'uncertain') return clarify('player', right, rp);
    if (lt.status === 'ambiguous' || lt.status === 'uncertain') return clarify('team', left, lt);
    if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', right, rt);
    if (playerReady && teamReady) {
      const playerScore = lp.top.confidence + rp.top.confidence;
      const teamScore = lt.top.confidence + rt.top.confidence;
      if (playerScore >= teamScore + 0.08) {
        const a = lp.top.entity, b = rp.top.entity;
        return { type: 'comparison', comparisonKind: 'player', players: [a, b], summary: `${a.name} vs ${b.name}: comparison built from available profiles.`, insight: 'Ask for a specific stat or recent form to sharpen the comparison.', dataSource: sourceUnion(lp.top.source, rp.top.source) };
      }
      const a = teamView(lt.top), b = teamView(rt.top);
      return { type: 'comparison', comparisonKind: 'team', teams: [a, b], summary: `${a.name} vs ${b.name}: comparison built from available team profiles.`, insight: 'Ask for recent matches or last-match results to add fixture context.', dataSource: sourceUnion(lt.top.source, rt.top.source) };
    }

    return {
      type: 'clarification',
      summary: `I detected a comparison query ("${text}") but could not confidently resolve both sides. Please use more specific names.`,
      entityType: 'comparison',
      suggestions: ['Virat Kohli vs Rohit Sharma', 'India vs England', 'Compare Kohli with Babar Azam'],
      dataSource: cache ? 'csv' : 'seed'
    };
  }

  m = (session.context.player && /^(recent form|form|trend)\??$/i.test(text))
    ? [text, session.context.player]
    : text.match(/^(.+?)\s+(recent form|form|trend)\??$/i);
  if (m) {
    const rp = resolvePlayer(m[1], true);
    if (rp.status === 'ambiguous' || rp.status === 'uncertain') return clarify('player', m[1], rp);
    if (rp.status === 'resolved') {
      const p = rp.top.entity;
      const r = Array.isArray(p.stats?.recentScores) ? p.stats.recentScores : [];
      const rAvg = avg(r);
      return playerPayload(rp.top, r.length ? `${p.name} recent form: last ${r.length} innings average ${round(rAvg, 1)} (${r.join(', ')}). Listed baseline average is ${p.stats?.average || 0}.` : `${p.name}: recent batting form samples are not available in the current profile.`, { analytics: [{ label: 'Recent avg', value: round(rAvg, 1) }, { label: 'Career avg', value: p.stats?.average || 0 }, { label: 'Matches', value: p.stats?.matches || 0 }] });
    }
  }

  m = text.match(/^(?:how many\s+)?(runs|wickets)\s+did\s+(.+?)\s+(?:score|scored|take|took|get|got)\??$/i)
    || text.match(/^(.+?)\s+(runs|wickets|average|strike rate|sr|economy)\??$/i)
    || (session.context.player && /^(runs|wickets|average|strike rate|sr|economy)\??$/i.test(text) ? [text, session.context.player, text] : null);
  if (m) {
    const statFirst = m.length >= 3 && /^(runs|wickets)$/i.test(m[1]);
    const playerRef = statFirst ? m[2] : m[1];
    const statRaw = statFirst ? m[1] : m[2];
    const stat = normalize(statRaw).replace(' ', '_');
    const rp = resolvePlayer(playerRef, true);
    if (rp.status === 'ambiguous' || rp.status === 'uncertain') return clarify('player', playerRef, rp);
    if (rp.status === 'resolved') {
      const p = rp.top.entity;
      const s = p.stats || {};
      let summary = `${p.name}: ${s.matches || 0} matches, ${s.runs || 0} runs, ${s.wickets || 0} wickets.`;
      if (stat === 'runs') summary = `${p.name} has scored ${s.runs || 0} runs in ${s.matches || 0} indexed matches (avg ${s.average || 0}, SR ${s.strikeRate || 0}).`;
      if (stat === 'wickets') summary = `${p.name} has taken ${s.wickets || 0} wickets in ${s.matches || 0} indexed matches (econ ${s.economy || 0}).`;
      if (stat === 'average') summary = `${p.name}'s listed batting average is ${s.average || 0}.`;
      if (stat === 'strike_rate' || stat === 'sr') summary = `${p.name}'s listed strike rate is ${s.strikeRate || 0}.`;
      if (stat === 'economy') summary = `${p.name}'s listed economy rate is ${s.economy || 0}.`;
      return playerPayload(rp.top, summary, { analytics: [{ label: 'Runs', value: s.runs || 0 }, { label: 'Wickets', value: s.wickets || 0 }, { label: 'Average', value: s.average || 0 }, { label: 'SR', value: s.strikeRate || 0 }] });
    }
  }

  const teamHints = /\bteam\b/.test(q);
  if (teamHints) {
    const stripped = text.replace(/\bteam\b/gi, '').replace(/\bstats?\b/gi, '').trim();
    const rt = resolveTeam(stripped || text, true);
    if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', stripped || text, rt);
    if (rt.status === 'resolved') {
      const t = teamView(rt.top);
      return teamPayload(rt.top, `${t.name}: ${t.stats?.matches || 0} matches, ${t.stats?.wins || 0} wins, win rate ${t.stats?.winRate || 0}%.`);
    }
  }

  const rp = resolvePlayer(text);
  const rt = resolveTeam(text);
  if (rp.status === 'resolved' && rt.status === 'resolved') {
    if (rp.top.confidence > rt.top.confidence + 0.08) return playerPayload(rp.top, `${rp.top.entity.name} (${rp.top.entity.team || 'Unknown'}): ${rp.top.entity.stats?.runs || 0} runs, ${rp.top.entity.stats?.wickets || 0} wickets, avg ${rp.top.entity.stats?.average || 0}.`);
    if (rt.top.confidence > rp.top.confidence + 0.08) {
      const t = teamView(rt.top);
      return teamPayload(rt.top, `${t.name}: ${t.stats?.matches || 0} matches, ${t.stats?.wins || 0} wins, win rate ${t.stats?.winRate || 0}%.`);
    }
    return { type: 'clarification', summary: `I can match "${text}" to both a player and a team. Please choose one.`, entityType: 'player_or_team', options: [{ label: rp.top.entity.name, value: rp.top.entity.name, kind: 'player', source: rp.top.source, confidence: round(rp.top.confidence, 3), meta: `${rp.top.entity.team || 'Unknown'} | ${rp.top.entity.role || 'Cricketer'}` }, { label: rt.top.entity.name, value: rt.top.entity.name, kind: 'team', source: rt.top.source, confidence: round(rt.top.confidence, 3), meta: rt.top.entity.region || 'Team' }], dataSource: sourceUnion(rp.top.source, rt.top.source) };
  }
  if (rp.status === 'resolved') return playerPayload(rp.top, `${rp.top.entity.name} (${rp.top.entity.team || 'Unknown'}): ${rp.top.entity.stats?.runs || 0} runs, ${rp.top.entity.stats?.wickets || 0} wickets, avg ${rp.top.entity.stats?.average || 0}.`);
  if (rt.status === 'resolved') {
    const t = teamView(rt.top);
    return teamPayload(rt.top, `${t.name}: ${t.stats?.matches || 0} matches, ${t.stats?.wins || 0} wins, win rate ${t.stats?.winRate || 0}%.`);
  }
  if (rp.status === 'ambiguous' || rp.status === 'uncertain') return clarify('player', text, rp);
  if (rt.status === 'ambiguous' || rt.status === 'uncertain') return clarify('team', text, rt);

  const suggestions = [];
  if (session.context.player) suggestions.push(`${session.context.player} recent form`);
  if (session.context.team) suggestions.push(`Last ${session.context.team} match result`);
  suggestions.push(
    'Live score India vs Australia',
    'Match momentum',
    'Who will win India vs Australia?',
    'How many runs might Virat Kohli score next match?',
    'Top predicted performer today',
    'match 1082591'
  );
  return {
    type: 'unsupported',
    summary: 'I can answer historical analytics and predictive questions for players, teams, matches, comparisons, leaderboards, and form. Rephrase with a clear player/team name or a prediction request.',
    suggestions: [...new Set(suggestions)].slice(0, 6),
    dataSource: cache ? 'csv' : 'seed'
  };
}

async function attachSemantic(payload, query) {
  if (!payload || payload.type === 'indexing' || payload.type === 'clarification' || payload.type === 'live') return payload;
  const semantic = await chromaSearch.querySemantic(query, 4);
  if (semantic?.results?.length) {
    payload.semantic = semantic;
    payload.dataSource = sourceUnion(payload.dataSource, 'chroma');
  }
  return payload;
}

async function handleQuery({ type = 'auto', query, sessionId } = {}) {
  const cleaned = String(query || '').trim();
  const session = getSession(sessionId);
  if (!cleaned) return { success: false, sessionId: session.id, statusCode: 400, message: 'Please provide at least one keyword or player name.' };

  await csvAnalytics.waitUntilReady(1500);
  const cache = csvAnalytics.getCache();
  const csvPayload = csvAnalytics.buildQueryPayload(cleaned);

  let payload;
  if (!cache || csvPayload.type === 'indexing') {
    payload = await buildLiveQueryPayload({ cache: null, session, text: cleaned });
    if (!payload) payload = buildPayload(cleaned, session, null);
    if (!payload || payload.type === 'unsupported') {
      const semantic = await chromaSearch.querySemantic(cleaned, 4);
      if (semantic?.results?.length) {
        payload = { type: 'semantic', summary: 'Detailed CSV analytics are still loading. Showing smart matches from vector search so you can continue exploring.', status: csvPayload.status, semantic, insight: 'Retry shortly for exact CSV-backed aggregates once indexing completes.', dataSource: 'chroma' };
      } else {
        payload = csvPayload;
      }
    } else {
      payload.summary = `${payload.summary} (CSV indexing in progress; answering from available profiles.)`;
      payload.status = csvPayload.status;
      payload.dataSource = sourceUnion(payload.dataSource, 'seed');
      payload = await attachSemantic(payload, cleaned);
    }
  } else {
    payload = await buildLiveQueryPayload({ cache, session, text: cleaned });
    if (!payload) payload = buildPayload(cleaned, session, cache);
    if ((!payload || payload.type === 'unsupported') && csvPayload.type && csvPayload.type !== 'unsupported' && csvPayload.type !== 'indexing') {
      payload = { ...csvPayload, insight: 'This answer comes from the indexed CSV analytics engine.' };
    }
    payload = await attachSemantic(payload, cleaned);
  }

  if (type === 'player' && payload?.type === 'team') {
    payload = { type: 'clarification', summary: `You requested a player query, but "${cleaned}" resolved to a team. Please enter a player name.`, entityType: 'player', suggestions: ['Virat Kohli', 'Rohit Sharma', 'Jasprit Bumrah'], dataSource: payload.dataSource };
  }
  if (type === 'team' && payload?.type === 'player') {
    payload = { type: 'clarification', summary: `You requested a team query, but "${cleaned}" resolved to a player. Please enter a team name.`, entityType: 'team', suggestions: ['India', 'Australia', 'Pakistan'], dataSource: payload.dataSource };
  }

  if (payload?.type !== 'clarification') remember(session, cleaned, payload);
  return { success: true, sessionId: session.id, payload };
}

module.exports = { handleQuery };
