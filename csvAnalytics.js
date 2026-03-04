const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const CSV_PATH = path.join(__dirname, 'cleaned_balls_two_folders.csv');

const CREDITED_WICKET_KINDS = new Set(['bowled', 'caught', 'caught and bowled', 'lbw', 'stumped', 'hit wicket']);
const QUERY_INTENT_TOKENS = new Set([
  'about',
  'all',
  'analysis',
  'any',
  'average',
  'averages',
  'bat',
  'batting',
  'best',
  'bowler',
  'bowlers',
  'bowling',
  'compare',
  'comparison',
  'death',
  'detail',
  'details',
  'economy',
  'form',
  'for',
  'head',
  'headtohead',
  'h2h',
  'info',
  'information',
  'last',
  'match',
  'matches',
  'most',
  'of',
  'overs',
  'performance',
  'player',
  'players',
  'query',
  'rate',
  'recent',
  'record',
  'records',
  'runs',
  'scorer',
  'scorers',
  'show',
  'sr',
  'stats',
  'stat',
  'strike',
  'team',
  'teams',
  'the',
  'top',
  'versus',
  'vs',
  'wicket',
  'wickets',
  'with'
]);

const state = {
  status: 'idle',
  error: null,
  promise: null,
  cache: null,
  progress: {
    rowsProcessed: 0,
    startedAt: null,
    updatedAt: null
  }
};

function nowIso() {
  return new Date().toISOString();
}

function normalize(value = '') {
  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function slugify(value = '') {
  return normalize(value).replace(/\s+/g, '-');
}

function toInt(value) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function round(value, digits = 2) {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function safeDivide(a, b) {
  if (!b) return 0;
  return a / b;
}

function oversFromBalls(balls = 0) {
  const whole = Math.floor(balls / 6);
  const rem = balls % 6;
  return `${whole}.${rem}`;
}

function incrementCounter(map, key, amount = 1) {
  if (!key) return;
  map.set(key, (map.get(key) || 0) + amount);
}

function maxCounterKey(map) {
  let winner = 'Unknown';
  let max = -1;
  for (const [key, value] of map.entries()) {
    if (value > max) {
      max = value;
      winner = key;
    }
  }
  return winner;
}

function topEntries(items, limit, comparator) {
  return [...items].sort(comparator).slice(0, limit);
}

function compactQueryTokens(tokens = []) {
  const trimmed = tokens.filter(Boolean);
  const filtered = trimmed.filter((token) => !QUERY_INTENT_TOKENS.has(token));
  return filtered.length ? filtered : trimmed;
}

function rankNameMatches(items, query, nameAccessor) {
  const raw = normalize(query);
  if (!raw) return [];
  const rawTokens = raw.split(' ').filter(Boolean);
  const qTokens = compactQueryTokens(rawTokens);
  const q = qTokens.join(' ') || raw;

  return items
    .map((item) => {
      const name = String(nameAccessor(item) || '');
      const n = normalize(name);
      const nTokens = n.split(' ').filter(Boolean);
      let score = -1;
      if (n === q || n === raw) score = 1000;
      else if (n.startsWith(q)) score = 820;
      else if (q && n.includes(q)) score = 640;

      const exactTokenHits = qTokens.filter((t) => nTokens.includes(t)).length;
      const prefixTokenHits = qTokens.filter((t) => !nTokens.includes(t) && nTokens.some((nt) => nt.startsWith(t))).length;
      const reversePrefixHits = qTokens.filter((t) => !nTokens.includes(t) && nTokens.some((nt) => t.startsWith(nt))).length;
      if (exactTokenHits || prefixTokenHits || reversePrefixHits) {
        score = Math.max(score, 420 + exactTokenHits * 45 + prefixTokenHits * 18 + reversePrefixHits * 8);
      }

      const queryFirst = qTokens[0] || '';
      const queryLast = qTokens[qTokens.length - 1] || '';
      const nameFirst = nTokens[0] || '';
      const nameLast = nTokens[nTokens.length - 1] || '';

      // Prefer exact surname + first-name/initial alignment when query has extra descriptive words.
      const queryHasNameLast = Boolean(nameLast && qTokens.includes(nameLast));
      const queryHasFullFirst = Boolean(nameFirst && qTokens.includes(nameFirst));
      const queryHasFirstInitial = Boolean(nameFirst && qTokens.some((t) => t[0] && t[0] === nameFirst[0]));

      if (qTokens.length >= 2 && nTokens.length >= 2 && queryHasNameLast && queryHasFirstInitial) {
        score = Math.max(score, 760 + (queryHasFullFirst ? 40 : 0));
      } else if (
        qTokens.length >= 2 &&
        nTokens.length >= 2 &&
        queryLast &&
        nameLast &&
        queryLast === nameLast &&
        queryFirst[0] &&
        nameFirst[0] &&
        queryFirst[0] === nameFirst[0]
      ) {
        score = Math.max(score, 750);
      }

      // Prefer candidates whose significant name tokens cover more of the filtered query.
      const coverage = qTokens.length ? (exactTokenHits + prefixTokenHits * 0.6 + reversePrefixHits * 0.3) / qTokens.length : 0;
      if (coverage > 0) {
        score += coverage;
      }

      return {
        item,
        name,
        score,
        firstTokenLen: (nTokens[0] || '').length,
        exactTokenHits,
        nameTokenCount: nTokens.length
      };
    })
    .filter((x) => x.score >= 0)
    .sort(
      (a, b) =>
        (b.score - a.score) ||
        (b.exactTokenHits - a.exactTokenHits) ||
        (b.firstTokenLen - a.firstTokenLen) ||
        (a.nameTokenCount - b.nameTokenCount) ||
        a.name.localeCompare(b.name)
    )
    .map((x) => x.item);
}

function createPlayerAggregate(name) {
  return {
    name,
    matches: new Set(),
    teamsCount: new Map(),
    batting: { runs: 0, balls: 0, dismissals: 0, fours: 0, sixes: 0 },
    bowling: { balls: 0, runsConceded: 0, wickets: 0, dots: 0 },
    recentBatting: new Map(), // matchId -> { matchId, date, team, runs, balls }
    matchBreakdown: new Map() // matchId -> { matchId, date, battingByTeam: Map, bowlingVsTeam: Map }
  };
}

function createTeamAggregate(name) {
  return {
    name,
    matches: new Set(),
    seasons: new Set(),
    venues: new Map(),
    wins: 0,
    losses: 0,
    noResult: 0,
    batting: { runs: 0, legalBalls: 0, wicketsLost: 0 },
    batterStats: new Map(),
    recentMatches: []
  };
}

function createMatchAggregate(row) {
  return {
    id: String(row.match_id || '').trim(),
    date: row.match_date || '',
    season: row.season || '',
    matchType: row.match_type || '',
    venue: row.venue || '',
    city: row.city || '',
    teams: new Set(),
    winner: row.match_winner || '',
    innings: new Map(), // inning -> { inning, battingTeam, runs, wickets, legalBalls }
    batters: new Map(), // name -> { name, team, runs, balls }
    bowlers: new Map(), // name -> { name, wickets, runsConceded, balls }
    rows: 0
  };
}

function getOrCreate(map, key, factory) {
  let value = map.get(key);
  if (!value) {
    value = factory();
    map.set(key, value);
  }
  return value;
}

function pushRecent(list, item, limit = 5) {
  list.push(item);
  list.sort((a, b) => (b.date || '').localeCompare(a.date || '') || String(b.id || '').localeCompare(String(a.id || '')));
  if (list.length > limit) list.length = limit;
}

function inferRole(stats) {
  if ((stats.runs || 0) >= 800 && (stats.wickets || 0) >= 20) return 'All-rounder';
  if ((stats.wickets || 0) >= 25 && (stats.economy || 99) > 0) return 'Bowler';
  if ((stats.runs || 0) >= 500) return 'Batter';
  return 'Cricketer';
}

function inferSpecialties(stats) {
  const tags = [];
  if ((stats.strikeRate || 0) >= 130) tags.push('Aggressive batting');
  if ((stats.average || 0) >= 35 && (stats.runs || 0) >= 300) tags.push('Consistency');
  if ((stats.wickets || 0) >= 20) tags.push('Wicket threat');
  if ((stats.economy || 99) <= 7 && (stats.wickets || 0) >= 10) tags.push('Economy control');
  if ((stats.fours || 0) >= 50) tags.push('Boundary scoring');
  if (!tags.length) tags.push('Regular contributor');
  return tags.slice(0, 3);
}

function finalizeData({ playerAggs, teamAggs, matchAggs, meta }) {
  const matches = [...matchAggs.values()]
    .map((match) => {
      const innings = topEntries(match.innings.values(), 10, (a, b) => a.inning - b.inning).map((i) => ({
        inning: i.inning,
        battingTeam: i.battingTeam,
        runs: i.runs,
        wickets: i.wickets,
        overs: oversFromBalls(i.legalBalls)
      }));

      const topBatters = topEntries(match.batters.values(), 4, (a, b) => (b.runs - a.runs) || (a.balls - b.balls) || a.name.localeCompare(b.name))
        .map((p) => ({ name: p.name, team: p.team, runs: p.runs, balls: p.balls }));
      const topBowlers = topEntries(match.bowlers.values(), 4, (a, b) => (b.wickets - a.wickets) || (a.runsConceded - b.runsConceded) || a.name.localeCompare(b.name))
        .map((p) => ({
          name: p.name,
          wickets: p.wickets,
          runsConceded: p.runsConceded,
          overs: oversFromBalls(p.balls),
          economy: round(safeDivide(p.runsConceded * 6, p.balls), 2)
        }));

      return {
        id: match.id,
        date: match.date,
        season: match.season,
        matchType: match.matchType,
        venue: match.venue,
        city: match.city,
        teams: [...match.teams].filter(Boolean).sort(),
        winner: match.winner,
        result: match.winner ? `${match.winner} won` : 'Result unavailable',
        innings,
        topBatters,
        topBowlers,
        highlights: [
          ...topBatters.slice(0, 2).map((x) => `${x.name} ${x.runs}`),
          ...topBowlers.slice(0, 2).map((x) => `${x.name} ${x.wickets}/${x.runsConceded}`)
        ].slice(0, 4)
      };
    })
    .sort((a, b) => (b.date || '').localeCompare(a.date || '') || String(b.id).localeCompare(String(a.id)));

  const matchMap = new Map(matches.map((m) => [String(m.id), m]));
  const playerOpponentSplits = new Map();

  const players = [...playerAggs.values()].map((player) => {
    const matchesCount = player.matches.size;
    const recentBatting = [...player.recentBatting.values()]
      .sort((a, b) => (b.date || '').localeCompare(a.date || '') || String(b.matchId).localeCompare(String(a.matchId)))
      .slice(0, 5);

    const stats = {
      matches: matchesCount,
      runs: player.batting.runs,
      wickets: player.bowling.wickets,
      average: player.batting.dismissals ? round(player.batting.runs / player.batting.dismissals, 2) : (player.batting.runs ? round(player.batting.runs, 2) : 0),
      strikeRate: player.batting.balls ? round((player.batting.runs * 100) / player.batting.balls, 2) : 0,
      economy: player.bowling.balls ? round((player.bowling.runsConceded * 6) / player.bowling.balls, 2) : 0,
      bowlingAverage: player.bowling.wickets ? round(player.bowling.runsConceded / player.bowling.wickets, 2) : 0,
      bowlingStrikeRate: player.bowling.wickets ? round(player.bowling.balls / player.bowling.wickets, 2) : 0,
      ballsFaced: player.batting.balls,
      ballsBowled: player.bowling.balls,
      wicketsCredits: player.bowling.wickets,
      fours: player.batting.fours,
      sixes: player.batting.sixes,
      dotBalls: player.bowling.dots,
      recentScores: recentBatting.map((x) => x.runs)
    };

    const primaryTeam = maxCounterKey(player.teamsCount);
    const recentMatches = recentBatting.map((appearance) => {
      const match = matchMap.get(String(appearance.matchId));
      return {
        id: String(appearance.matchId),
        date: appearance.date,
        venue: match?.venue || '',
        teams: match?.teams || [],
        result: match?.result || 'Result unavailable',
        highlights: match?.highlights || []
      };
    });

    const opponentSplitMap = new Map();
    for (const matchStats of player.matchBreakdown.values()) {
      const match = matchMap.get(String(matchStats.matchId));
      const matchTeams = Array.isArray(match?.teams) ? match.teams : [];
      const otherTeamFor = (teamName) =>
        matchTeams.find((teamNameCandidate) => String(teamNameCandidate || '') !== String(teamName || '')) || 'Unknown';

      for (const [battingTeam, battingStats] of matchStats.battingByTeam.entries()) {
        const opponent = otherTeamFor(battingTeam);
        const split = getOrCreate(opponentSplitMap, opponent, () => ({
          opponent,
          matches: new Set(),
          batting: { runs: 0, balls: 0, dismissals: 0, fours: 0, sixes: 0 },
          bowling: { balls: 0, runsConceded: 0, wickets: 0, dots: 0 }
        }));
        split.matches.add(String(matchStats.matchId));
        split.batting.runs += battingStats.runs || 0;
        split.batting.balls += battingStats.balls || 0;
        split.batting.dismissals += battingStats.dismissals || 0;
        split.batting.fours += battingStats.fours || 0;
        split.batting.sixes += battingStats.sixes || 0;
      }

      for (const [opponent, bowlingStats] of matchStats.bowlingVsTeam.entries()) {
        const split = getOrCreate(opponentSplitMap, opponent, () => ({
          opponent,
          matches: new Set(),
          batting: { runs: 0, balls: 0, dismissals: 0, fours: 0, sixes: 0 },
          bowling: { balls: 0, runsConceded: 0, wickets: 0, dots: 0 }
        }));
        split.matches.add(String(matchStats.matchId));
        split.bowling.balls += bowlingStats.balls || 0;
        split.bowling.runsConceded += bowlingStats.runsConceded || 0;
        split.bowling.wickets += bowlingStats.wickets || 0;
        split.bowling.dots += bowlingStats.dots || 0;
      }
    }

    const opponentSplits = [...opponentSplitMap.values()]
      .map((split) => ({
        opponent: split.opponent,
        matches: split.matches.size,
        batting: {
          runs: split.batting.runs,
          balls: split.batting.balls,
          dismissals: split.batting.dismissals,
          average: split.batting.dismissals ? round(split.batting.runs / split.batting.dismissals, 2) : split.batting.runs,
          strikeRate: split.batting.balls ? round((split.batting.runs * 100) / split.batting.balls, 2) : 0,
          fours: split.batting.fours,
          sixes: split.batting.sixes
        },
        bowling: {
          balls: split.bowling.balls,
          wickets: split.bowling.wickets,
          runsConceded: split.bowling.runsConceded,
          economy: split.bowling.balls ? round((split.bowling.runsConceded * 6) / split.bowling.balls, 2) : 0,
          average: split.bowling.wickets ? round(split.bowling.runsConceded / split.bowling.wickets, 2) : 0,
          strikeRate: split.bowling.wickets ? round(split.bowling.balls / split.bowling.wickets, 2) : 0,
          dots: split.bowling.dots
        }
      }))
      .sort((a, b) => (b.matches - a.matches) || a.opponent.localeCompare(b.opponent));

    playerOpponentSplits.set(slugify(player.name), opponentSplits);

    return {
      id: slugify(player.name),
      name: player.name,
      team: primaryTeam,
      role: inferRole(stats),
      stats,
      specialties: inferSpecialties(stats),
      recentMatches
    };
  });

  const playersByTeam = new Map();
  for (const player of players) {
    if (!playersByTeam.has(player.team)) playersByTeam.set(player.team, []);
    playersByTeam.get(player.team).push(player);
  }

  const teams = [...teamAggs.values()].map((team) => {
    const matchCount = team.matches.size;
    const topBatters = topEntries(team.batterStats.entries(), 5, (a, b) => (b[1].runs - a[1].runs) || a[0].localeCompare(b[0]))
      .map(([name, s]) => ({ name, runs: s.runs, strikeRate: s.balls ? round((s.runs * 100) / s.balls, 1) : 0 }));
    const topBowlers = topEntries((playersByTeam.get(team.name) || []).filter((p) => (p.stats.wickets || 0) > 0), 5, (a, b) =>
      (b.stats.wickets - a.stats.wickets) || (a.stats.economy - b.stats.economy) || a.name.localeCompare(b.name)
    ).map((p) => ({ name: p.name, wickets: p.stats.wickets, economy: p.stats.economy }));

    return {
      id: slugify(team.name),
      name: team.name,
      region: 'CSV Indexed',
      captain: 'N/A',
      coach: 'N/A',
      stats: {
        matches: matchCount,
        wins: team.wins,
        losses: team.losses,
        noResult: team.noResult,
        winRate: matchCount ? round((team.wins * 100) / matchCount, 1) : 0,
        runs: team.batting.runs,
        averageScore: matchCount ? round(team.batting.runs / matchCount, 1) : 0,
        strikeRate: team.batting.legalBalls ? round((team.batting.runs * 100) / team.batting.legalBalls, 1) : 0,
        wicketsLost: team.batting.wicketsLost
      },
      venues: topEntries(team.venues.entries(), 5, (a, b) => b[1] - a[1]).map(([venue, count]) => ({ venue, matches: count })),
      topBatters,
      topBowlers,
      recentMatches: team.recentMatches
    };
  });

  const leaderboards = {
    runs: topEntries(players.filter((p) => (p.stats.runs || 0) > 0), 10, (a, b) => (b.stats.runs - a.stats.runs) || a.name.localeCompare(b.name))
      .map((p, i) => ({ rank: i + 1, label: p.name, team: p.team, value: `${p.stats.runs} runs`, meta: `Avg ${p.stats.average} | SR ${p.stats.strikeRate}` })),
    wickets: topEntries(players.filter((p) => (p.stats.wickets || 0) > 0), 10, (a, b) => (b.stats.wickets - a.stats.wickets) || a.name.localeCompare(b.name))
      .map((p, i) => ({ rank: i + 1, label: p.name, team: p.team, value: `${p.stats.wickets} wickets`, meta: `Econ ${p.stats.economy}` })),
    averages: topEntries(
      players.filter((p) => (p.stats.runs || 0) > 0 && (p.stats.matches || 0) >= 8 && (p.stats.ballsFaced || 0) >= 60),
      10,
      (a, b) => (b.stats.average - a.stats.average) || (b.stats.runs - a.stats.runs) || a.name.localeCompare(b.name)
    ).map((p, i) => ({
      rank: i + 1,
      label: p.name,
      team: p.team,
      value: `Avg ${p.stats.average}`,
      meta: `${p.stats.runs} runs | ${p.stats.matches} matches | SR ${p.stats.strikeRate}`
    })),
    teamWins: topEntries(teams, 10, (a, b) => (b.stats.wins - a.stats.wins) || a.name.localeCompare(b.name))
      .map((t, i) => ({ rank: i + 1, label: t.name, team: t.name, value: `${t.stats.wins} wins`, meta: `${t.stats.matches} matches | ${t.stats.winRate}%` }))
  };

  return {
    meta: {
      rows: meta.rows,
      matches: matches.length,
      players: players.length,
      teams: teams.length,
      seasons: meta.seasons.size,
      venues: meta.venues.size,
      minDate: meta.minDate,
      maxDate: meta.maxDate
    },
    matches,
    players,
    teams,
    leaderboards,
    playerOpponentSplits
  };
}

function buildIndex() {
  if (!fs.existsSync(CSV_PATH)) {
    return Promise.reject(new Error(`CSV file not found: ${CSV_PATH}`));
  }

  const playerAggs = new Map();
  const teamAggs = new Map();
  const matchAggs = new Map();
  const meta = { rows: 0, seasons: new Set(), venues: new Set(), minDate: '', maxDate: '' };

  state.progress.startedAt = nowIso();
  state.progress.updatedAt = nowIso();
  state.progress.rowsProcessed = 0;

  return new Promise((resolve, reject) => {
    fs.createReadStream(CSV_PATH)
      .pipe(csv())
      .on('data', (row) => {
        meta.rows += 1;
        state.progress.rowsProcessed = meta.rows;
        if (meta.rows % 25000 === 0) state.progress.updatedAt = nowIso();

        const matchId = String(row.match_id || '').trim();
        if (!matchId) return;
        const date = String(row.match_date || '');
        if (date) {
          if (!meta.minDate || date < meta.minDate) meta.minDate = date;
          if (!meta.maxDate || date > meta.maxDate) meta.maxDate = date;
        }
        if (row.season) meta.seasons.add(String(row.season));
        if (row.venue) meta.venues.add(String(row.venue));

        const match = getOrCreate(matchAggs, matchId, () => createMatchAggregate(row));
        match.rows += 1;
        match.date = row.match_date || match.date;
        match.season = row.season || match.season;
        match.matchType = row.match_type || match.matchType;
        match.venue = row.venue || match.venue;
        match.city = row.city || match.city;
        if (row.batting_team) match.teams.add(row.batting_team);
        if (row.match_winner) match.winner = row.match_winner;

        const battingTeam = String(row.batting_team || '').trim() || 'Unknown';
        const inning = toInt(row.inning);
        const runsBatter = toInt(row.runs_batter);
        const runsTotal = toInt(row.runs_total);
        const wides = toInt(row.extras_wides);
        const noBalls = toInt(row.extras_noballs);
        const byes = toInt(row.extras_byes);
        const legByes = toInt(row.extras_legbyes);
        const wicketCount = toInt(row.wicket_count);
        const wicketKind = String(row.wicket_kind || '').trim().toLowerCase();
        const wicketPlayerOut = String(row.wicket_player_out || '').trim();
        const nonBoundary = toInt(row.non_boundary);
        const legalBall = wides === 0 && noBalls === 0;

        const inningAgg = getOrCreate(match.innings, inning, () => ({ inning, battingTeam, runs: 0, wickets: 0, legalBalls: 0 }));
        inningAgg.battingTeam = battingTeam;
        inningAgg.runs += runsTotal;
        inningAgg.wickets += wicketCount;
        if (legalBall) inningAgg.legalBalls += 1;

        const team = getOrCreate(teamAggs, battingTeam, () => createTeamAggregate(battingTeam));
        team.matches.add(matchId);
        if (row.season) team.seasons.add(String(row.season));
        if (row.venue) incrementCounter(team.venues, row.venue);
        team.batting.runs += runsTotal;
        team.batting.wicketsLost += wicketCount;
        if (legalBall) team.batting.legalBalls += 1;

        const batterName = String(row.batter || '').trim();
        if (batterName) {
          const player = getOrCreate(playerAggs, batterName, () => createPlayerAggregate(batterName));
          player.matches.add(matchId);
          incrementCounter(player.teamsCount, battingTeam);
          player.batting.runs += runsBatter;
          if (legalBall) player.batting.balls += 1;
          if (wicketPlayerOut && wicketPlayerOut === batterName) player.batting.dismissals += 1;
          if (runsBatter === 4 && nonBoundary !== 1) player.batting.fours += 1;
          if (runsBatter === 6) player.batting.sixes += 1;

          const prev = player.recentBatting.get(matchId) || { matchId, date, team: battingTeam, runs: 0, balls: 0 };
          prev.date = date || prev.date;
          prev.team = battingTeam || prev.team;
          prev.runs += runsBatter;
          if (legalBall) prev.balls += 1;
          player.recentBatting.set(matchId, prev);

          const playerMatch = getOrCreate(player.matchBreakdown, matchId, () => ({
            matchId,
            date,
            battingByTeam: new Map(),
            bowlingVsTeam: new Map()
          }));
          playerMatch.date = date || playerMatch.date;
          const battingSplit = getOrCreate(playerMatch.battingByTeam, battingTeam, () => ({
            runs: 0,
            balls: 0,
            dismissals: 0,
            fours: 0,
            sixes: 0
          }));
          battingSplit.runs += runsBatter;
          if (legalBall) battingSplit.balls += 1;
          if (wicketPlayerOut && wicketPlayerOut === batterName) battingSplit.dismissals += 1;
          if (runsBatter === 4 && nonBoundary !== 1) battingSplit.fours += 1;
          if (runsBatter === 6) battingSplit.sixes += 1;

          const teamBatter = getOrCreate(team.batterStats, batterName, () => ({ runs: 0, balls: 0 }));
          teamBatter.runs += runsBatter;
          if (legalBall) teamBatter.balls += 1;

          const matchBatter = getOrCreate(match.batters, batterName, () => ({ name: batterName, team: battingTeam, runs: 0, balls: 0 }));
          matchBatter.team = battingTeam;
          matchBatter.runs += runsBatter;
          if (legalBall) matchBatter.balls += 1;
        }

        const bowlerName = String(row.bowler || '').trim();
        if (bowlerName) {
          const player = getOrCreate(playerAggs, bowlerName, () => createPlayerAggregate(bowlerName));
          player.matches.add(matchId);
          const bowlerRunsConceded = runsBatter + wides + noBalls;
          player.bowling.runsConceded += bowlerRunsConceded;
          if (legalBall) player.bowling.balls += 1;
          if (legalBall && bowlerRunsConceded === 0 && byes === 0 && legByes === 0) player.bowling.dots += 1;
          if (wicketCount > 0 && wicketPlayerOut && CREDITED_WICKET_KINDS.has(wicketKind)) player.bowling.wickets += 1;

          const playerMatch = getOrCreate(player.matchBreakdown, matchId, () => ({
            matchId,
            date,
            battingByTeam: new Map(),
            bowlingVsTeam: new Map()
          }));
          playerMatch.date = date || playerMatch.date;
          const bowlingSplit = getOrCreate(playerMatch.bowlingVsTeam, battingTeam, () => ({
            balls: 0,
            runsConceded: 0,
            wickets: 0,
            dots: 0
          }));
          bowlingSplit.runsConceded += bowlerRunsConceded;
          if (legalBall) bowlingSplit.balls += 1;
          if (legalBall && bowlerRunsConceded === 0 && byes === 0 && legByes === 0) bowlingSplit.dots += 1;
          if (wicketCount > 0 && wicketPlayerOut && CREDITED_WICKET_KINDS.has(wicketKind)) bowlingSplit.wickets += 1;

          const matchBowler = getOrCreate(match.bowlers, bowlerName, () => ({ name: bowlerName, wickets: 0, runsConceded: 0, balls: 0 }));
          matchBowler.runsConceded += bowlerRunsConceded;
          if (legalBall) matchBowler.balls += 1;
          if (wicketCount > 0 && wicketPlayerOut && CREDITED_WICKET_KINDS.has(wicketKind)) matchBowler.wickets += 1;
        }
      })
      .on('end', () => {
        const finalized = finalizeData({ playerAggs, teamAggs, matchAggs, meta });

        const matchById = new Map(finalized.matches.map((m) => [String(m.id), m]));
        const finalizedTeamMap = new Map(finalized.teams.map((t) => [t.name, t]));

        for (const team of finalized.teams) {
          for (const match of finalized.matches) {
            if (!match.teams.includes(team.name)) continue;
            pushRecent(team.recentMatches, {
              id: match.id,
              date: match.date,
              venue: match.venue,
              teams: match.teams,
              result: match.result,
              highlights: match.highlights
            });
          }
          team.stats.wins = 0;
          team.stats.losses = 0;
          team.stats.noResult = 0;
        }

        for (const match of finalized.matches) {
          for (const teamName of match.teams) {
            const team = finalizedTeamMap.get(teamName);
            if (!team) continue;
            if (match.winner) {
              if (match.winner === team.name) team.stats.wins += 1;
              else team.stats.losses += 1;
            } else {
              team.stats.noResult += 1;
            }
          }
        }
        for (const team of finalized.teams) {
          team.stats.winRate = team.stats.matches ? round((team.stats.wins * 100) / team.stats.matches, 1) : 0;
        }

        finalized.matchById = matchById;
        resolve(finalized);
      })
      .on('error', reject);
  });
}

function startIndexing() {
  if (state.cache) {
    state.status = 'ready';
    return Promise.resolve(state.cache);
  }
  if (state.promise) return state.promise;

  state.status = 'loading';
  state.error = null;
  state.progress.startedAt = nowIso();
  state.progress.updatedAt = nowIso();

  state.promise = buildIndex()
    .then((cache) => {
      state.cache = cache;
      state.status = 'ready';
      state.error = null;
      state.progress.updatedAt = nowIso();
      state.promise = null;
      return cache;
    })
    .catch((error) => {
      state.status = 'error';
      state.error = error;
      state.promise = null;
      throw error;
    });

  return state.promise;
}

async function waitUntilReady(timeoutMs = 2000) {
  if (state.cache) return state.cache;
  if (!state.promise) startIndexing();
  if (!state.promise) return null;

  const timeout = new Promise((resolve) => setTimeout(() => resolve(null), timeoutMs));
  try {
    return await Promise.race([state.promise, timeout]);
  } catch (_) {
    return null;
  }
}

function getStatus() {
  const payload = {
    status: state.status,
    rowsProcessed: state.progress.rowsProcessed,
    startedAt: state.progress.startedAt,
    updatedAt: state.progress.updatedAt
  };
  if (state.cache?.meta) payload.meta = state.cache.meta;
  if (state.error) payload.error = state.error.message;
  return payload;
}

function getCache() {
  return state.cache;
}

function parseComparison(query) {
  const text = String(query || '').trim();
  if (!text) return null;
  const vs = text.match(/^(.+?)\s+vs\s+(.+)$/i);
  if (vs) return { left: vs[1].trim(), right: vs[2].trim() };
  const compare = text.match(/^compare\s+(.+?)\s+(?:with|and)\s+(.+)$/i);
  if (compare) return { left: compare[1].trim(), right: compare[2].trim() };
  return null;
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

function buildPlayerSummary(player) {
  const stats = player.stats || {};
  const bits = [];
  if (stats.runs) bits.push(`${stats.runs} runs`);
  if (stats.wickets) bits.push(`${stats.wickets} wickets`);
  if (stats.average) bits.push(`avg ${stats.average}`);
  if (stats.strikeRate) bits.push(`SR ${stats.strikeRate}`);
  if (stats.economy && stats.wickets) bits.push(`econ ${stats.economy}`);
  return `${player.name} (${player.team}) in ${stats.matches || 0} indexed matches: ${bits.join(' | ')}.`;
}

function buildTeamSummary(team) {
  const s = team.stats || {};
  return `${team.name}: ${s.matches || 0} indexed matches, ${s.wins || 0} wins, win rate ${s.winRate || 0}%, total runs ${s.runs || 0}.`;
}

function buildQueryPayload(query) {
  if (!state.cache) {
    return {
      type: 'indexing',
      summary: `CSV index is ${state.status === 'loading' ? 'building' : 'starting'}... processed ${state.progress.rowsProcessed.toLocaleString('en-US')} rows.`,
      status: getStatus()
    };
  }

  const text = String(query || '').trim();
  if (!text) {
    return {
      type: 'unsupported',
      summary: 'Ask about a player, team, match id, or leaderboard query like "top run scorers".'
    };
  }

  const comparison = parseComparison(text);
  if (comparison) {
    const left = rankNameMatches(state.cache.players, comparison.left, (p) => p.name)[0];
    const right = rankNameMatches(state.cache.players, comparison.right, (p) => p.name)[0];
    if (left && right) {
      return {
        type: 'comparison',
        summary: `${left.name} vs ${right.name}: ${left.name} has ${left.stats.runs} runs and ${left.stats.wickets} wickets; ${right.name} has ${right.stats.runs} runs and ${right.stats.wickets} wickets in the indexed CSV.`,
        players: [left, right],
        dataSource: 'csv'
      };
    }
  }

  const leaderboard = leaderboardIntent(text);
  if (leaderboard && state.cache.leaderboards[leaderboard.key]) {
    return {
      type: 'leaderboard',
      summary: `${leaderboard.title} from the indexed CSV.`,
      title: leaderboard.title,
      rows: state.cache.leaderboards[leaderboard.key],
      dataSource: 'csv'
    };
  }

  const matchIdMatch = text.match(/\bmatch\s+(\d{5,})\b/i) || text.match(/^\d{5,}$/);
  if (matchIdMatch) {
    const match = state.cache.matchById.get(String(matchIdMatch[1] || matchIdMatch[0]));
    if (match) {
      return {
        type: 'match',
        summary: `${match.teams.join(' vs ')} on ${match.date} at ${match.venue}. ${match.result}.`,
        match,
        dataSource: 'csv'
      };
    }
  }

  const player = rankNameMatches(state.cache.players, text, (p) => p.name)[0];
  if (player) {
    return {
      type: 'player',
      summary: buildPlayerSummary(player),
      player,
      relatedMatches: player.recentMatches || [],
      dataSource: 'csv'
    };
  }

  const team = rankNameMatches(state.cache.teams, text, (t) => t.name)[0];
  if (team) {
    return {
      type: 'team',
      summary: buildTeamSummary(team),
      team,
      recentMatches: team.recentMatches || [],
      dataSource: 'csv'
    };
  }

  return {
    type: 'unsupported',
    summary:
      'Try a player name, team name, match id, or queries like "top run scorers", "most wickets", or "most team wins".',
    suggestions: ['Virat Kohli', 'India', 'match 1082591', 'top run scorers', 'best averages']
  };
}

function summarizeForDashboard(fallbackSummary) {
  if (!state.cache?.meta) return fallbackSummary;
  return {
    matches: state.cache.meta.matches,
    players: state.cache.meta.players,
    teams: state.cache.meta.teams,
    metrics: fallbackSummary.metrics,
    insights: fallbackSummary.insights,
    rows: state.cache.meta.rows,
    seasons: state.cache.meta.seasons
  };
}

function toDashboardPlayers(limit = 6) {
  if (!state.cache?.players) return null;
  const topRuns = topEntries(state.cache.players.filter((p) => (p.stats.runs || 0) > 0), 8, (a, b) => (b.stats.runs - a.stats.runs) || a.name.localeCompare(b.name));
  const topWickets = topEntries(state.cache.players.filter((p) => (p.stats.wickets || 0) > 0), 8, (a, b) => (b.stats.wickets - a.stats.wickets) || a.name.localeCompare(b.name));
  const merged = new Map();
  for (const p of [...topRuns, ...topWickets]) merged.set(p.id, p);
  return [...merged.values()].slice(0, limit);
}

function toDashboardMatches(limit = 5) {
  if (!state.cache?.matches) return null;
  return state.cache.matches.slice(0, limit).map((m) => ({
    id: m.id,
    date: m.date,
    venue: m.venue,
    teams: m.teams,
    result: m.result,
    highlights: m.highlights || []
  }));
}

function toDashboardMetrics(fallbackMetrics = []) {
  if (!state.cache?.meta) return fallbackMetrics;
  const core = [
    {
      id: 'rows-indexed',
      label: 'Indexed delivery rows',
      value: state.cache.meta.rows.toLocaleString('en-US'),
      detail: 'Ball-by-ball rows parsed from your CSV dataset.'
    },
    {
      id: 'matches-indexed-csv',
      label: 'Indexed matches',
      value: state.cache.meta.matches.toLocaleString('en-US'),
      detail: 'Unique match ids from the CSV.'
    },
    {
      id: 'players-indexed-csv',
      label: 'Indexed players',
      value: state.cache.meta.players.toLocaleString('en-US'),
      detail: 'Player aggregates computed from deliveries.'
    },
    {
      id: 'teams-indexed-csv',
      label: 'Indexed teams',
      value: state.cache.meta.teams.toLocaleString('en-US'),
      detail: 'Teams inferred from batting innings.'
    },
    {
      id: 'seasons-covered',
      label: 'Seasons covered',
      value: state.cache.meta.seasons.toLocaleString('en-US'),
      detail: `${state.cache.meta.minDate || 'N/A'} to ${state.cache.meta.maxDate || 'N/A'}`
    },
    {
      id: 'venues-covered',
      label: 'Venues covered',
      value: state.cache.meta.venues.toLocaleString('en-US'),
      detail: 'Distinct venues in the CSV feed.'
    }
  ];
  return [...core, ...fallbackMetrics.slice(0, Math.max(0, 12 - core.length))];
}

module.exports = {
  startIndexing,
  waitUntilReady,
  getStatus,
  getCache,
  buildQueryPayload,
  summarizeForDashboard,
  toDashboardPlayers,
  toDashboardMatches,
  toDashboardMetrics
};
