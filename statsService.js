const datasetStore = require('./datasetStore');
const { GLOSSARY, NOT_AVAILABLE_MESSAGE } = require('./constants');
const { normalizeText, round } = require('./textUtils');

function clampStatValue(value, min, max) {
  if (!Number.isFinite(value)) return false;
  return value >= min && value <= max;
}

function sanitizePlayerStats(stats) {
  if (!stats) return null;
  if (!clampStatValue(stats.runs, 0, Number.MAX_SAFE_INTEGER)) return null;
  if (!clampStatValue(stats.matches, 0, Number.MAX_SAFE_INTEGER)) return null;
  if (!clampStatValue(stats.innings, 0, Number.MAX_SAFE_INTEGER)) return null;
  if (stats.matches < stats.innings) return null;
  if (!clampStatValue(stats.strike_rate, 0, 300)) return null;
  if (!clampStatValue(stats.average, 0, 200)) return null;
  if (!clampStatValue(stats.economy, 0, 20)) return null;
  return stats;
}

function sanitizeTeamStats(stats) {
  if (!stats) return null;
  if (!clampStatValue(stats.matches, 0, Number.MAX_SAFE_INTEGER)) return null;
  if (!clampStatValue(stats.win_rate, 0, 100)) return null;
  if (!clampStatValue(stats.strike_rate, 0, 300)) return null;
  return stats;
}

function unavailableResult() {
  return {
    answer: NOT_AVAILABLE_MESSAGE,
    data: {},
    followups: [
      'Try "Virat Kohli stats"',
      'Try "Top run scorers 2019"',
      'Try "India vs Australia head to head"'
    ]
  };
}

function defaultFollowups(action) {
  if (action === 'player_stats' || action === 'player_season_stats') {
    return [
      'Compare this player with another player',
      'Show this player in another season',
      'Show this player in ODI or T20 format'
    ];
  }
  if (action === 'team_stats' || action === 'head_to_head') {
    return [
      'Show this team in another season',
      'Show head to head against another team',
      'Show recent matches for this team'
    ];
  }
  if (action === 'top_players') {
    return [
      'Show top runs for another season',
      'Show most wickets for a season',
      'Show best strike rate with minimum balls'
    ];
  }
  if (action === 'match_summary') {
    return [
      'Show player stats from this match',
      'Show team head to head',
      'Show recent matches for a team'
    ];
  }
  return [
    'Show player stats',
    'Show top run scorers',
    'Show team head to head'
  ];
}

function metricLabel(metric = '') {
  const normalized = normalizeText(metric);
  if (normalized.includes('wicket')) return 'wickets';
  if (normalized.includes('strike')) return 'strike rate';
  if (normalized.includes('economy')) return 'economy';
  return 'runs';
}

function resolveTopMetric(metric = '') {
  const normalized = normalizeText(metric);
  if (!normalized) return 'runs';
  if (normalized.includes('wicket')) return 'wickets';
  if (normalized.includes('strike') || normalized === 'sr') return 'strike_rate';
  if (normalized.includes('economy')) return 'economy';
  return 'runs';
}

function buildPlayerAnswer(player, stats, contextText = '') {
  return `${player.name}${contextText}: ${stats.matches} matches, ${stats.runs} runs, ${stats.wickets} wickets, average ${stats.average}, strike rate ${stats.strike_rate}, economy ${stats.economy}.`;
}

function executePlayerStats({ playerId, filters = {}, action = 'player_stats' }) {
  const summary = datasetStore.getPlayerSummary(playerId, filters);
  if (!summary) return unavailableResult();
  const safe = sanitizePlayerStats(summary.stats);
  if (!safe) {
    return {
      answer: 'Stats not available.',
      data: { type: 'player', player: { id: summary.id, name: summary.name } },
      followups: defaultFollowups(action)
    };
  }

  const contextBits = [];
  if (filters.season) contextBits.push(` in ${filters.season}`);
  if (filters.format) contextBits.push(` (${filters.format})`);
  if (filters.venue) contextBits.push(` at ${filters.venue}`);
  const contextText = contextBits.join('');

  return {
    answer: buildPlayerAnswer(summary, safe, contextText),
    data: {
      type: 'player_stats',
      player: {
        id: summary.id,
        name: summary.name,
        team: summary.team
      },
      stats: safe,
      recent_matches: safe.recent_matches
    },
    followups: defaultFollowups(action)
  };
}

function executeTeamStats({ teamId, filters = {}, action = 'team_stats' }) {
  const summary = datasetStore.getTeamSummary(teamId, filters);
  if (!summary) return unavailableResult();
  const safe = sanitizeTeamStats(summary.stats);
  if (!safe) {
    return {
      answer: 'Stats not available.',
      data: { type: 'team', team: { id: summary.id, name: summary.name } },
      followups: defaultFollowups(action)
    };
  }

  const contextBits = [];
  if (filters.season) contextBits.push(` in ${filters.season}`);
  if (filters.format) contextBits.push(` (${filters.format})`);
  if (filters.venue) contextBits.push(` at ${filters.venue}`);
  const contextText = contextBits.join('');

  return {
    answer: `${summary.name}${contextText}: ${safe.matches} matches, ${safe.wins} wins, win rate ${safe.win_rate}%, average score ${safe.average_score}.`,
    data: {
      type: 'team_stats',
      team: { id: summary.id, name: summary.name },
      stats: safe
    },
    followups: defaultFollowups(action)
  };
}

function executeMatchSummary({ matchId, team1, team2, season, date }) {
  let match = null;
  if (matchId) {
    match = datasetStore.getMatchById(matchId);
  } else if (team1 || team2) {
    match = datasetStore.findMatchByTeams({ team1, team2, season, date });
  }
  if (!match) return unavailableResult();

  const topBatters = (match.top_batters || [])
    .slice(0, 3)
    .map((row) => `${row.name} ${row.runs}`)
    .join(', ');
  const topBowlers = (match.top_bowlers || [])
    .slice(0, 3)
    .map((row) => `${row.name} ${row.wickets}/${row.runs_conceded}`)
    .join(', ');

  return {
    answer: `${match.result}. Top batters: ${topBatters || 'not available'}. Top bowlers: ${topBowlers || 'not available'}.`,
    data: {
      type: 'match_summary',
      match
    },
    followups: defaultFollowups('match_summary')
  };
}

function executeComparePlayers({ playerId1, playerId2, filters = {} }) {
  const comparison = datasetStore.comparePlayers(playerId1, playerId2, filters);
  if (!comparison) return unavailableResult();

  const leftStats = sanitizePlayerStats(comparison.left.stats);
  const rightStats = sanitizePlayerStats(comparison.right.stats);
  if (!leftStats || !rightStats) {
    return {
      answer: 'Stats not available.',
      data: { type: 'compare_players' },
      followups: defaultFollowups('player_stats')
    };
  }

  const runLeader =
    leftStats.runs === rightStats.runs
      ? 'Both are level on runs'
      : leftStats.runs > rightStats.runs
        ? `${comparison.left.name} has more runs`
        : `${comparison.right.name} has more runs`;
  const wicketLeader =
    leftStats.wickets === rightStats.wickets
      ? 'Both are level on wickets'
      : leftStats.wickets > rightStats.wickets
        ? `${comparison.left.name} has more wickets`
        : `${comparison.right.name} has more wickets`;

  return {
    answer: `${runLeader}. ${wicketLeader}.`,
    data: {
      type: 'compare_players',
      left: {
        id: comparison.left.id,
        name: comparison.left.name,
        team: comparison.left.team,
        stats: leftStats
      },
      right: {
        id: comparison.right.id,
        name: comparison.right.name,
        team: comparison.right.team,
        stats: rightStats
      }
    },
    followups: defaultFollowups('player_stats')
  };
}

function executeCompareTeams({ teamId1, teamId2, filters = {} }) {
  const comparison = datasetStore.compareTeams(teamId1, teamId2, filters);
  if (!comparison) return unavailableResult();

  const leftStats = sanitizeTeamStats(comparison.left.stats);
  const rightStats = sanitizeTeamStats(comparison.right.stats);
  if (!leftStats || !rightStats) {
    return {
      answer: 'Stats not available.',
      data: { type: 'compare_teams' },
      followups: defaultFollowups('team_stats')
    };
  }

  const winLeader =
    leftStats.wins === rightStats.wins
      ? 'Both teams have the same wins'
      : leftStats.wins > rightStats.wins
        ? `${comparison.left.name} has more wins`
        : `${comparison.right.name} has more wins`;

  return {
    answer: `${winLeader}.`,
    data: {
      type: 'compare_teams',
      left: {
        id: comparison.left.id,
        name: comparison.left.name,
        stats: leftStats
      },
      right: {
        id: comparison.right.id,
        name: comparison.right.name,
        stats: rightStats
      }
    },
    followups: defaultFollowups('team_stats')
  };
}

function executeHeadToHead({ team1Name, team2Name, filters = {} }) {
  const result = datasetStore.computeHeadToHead(team1Name, team2Name, filters);
  if (!result || result.matches === 0) return unavailableResult();

  return {
    answer: `${team1Name} vs ${team2Name}: ${result.matches} matches, ${team1Name} won ${result.wins_team_a}, ${team2Name} won ${result.wins_team_b}, no result ${result.no_result}.`,
    data: {
      type: 'head_to_head',
      team1: team1Name,
      team2: team2Name,
      stats: result
    },
    followups: defaultFollowups('head_to_head')
  };
}

function executeTopList({ entities = {} }) {
  const metric = resolveTopMetric(entities.metric || entities.list_type || '');
  const rows = datasetStore.listTopPlayers({
    metric,
    season: entities.season || '',
    format: entities.format || '',
    limit: Number(entities.limit || 10),
    minBalls: Number(entities.min_balls || 200),
    minOvers: Number(entities.min_overs || 20)
  });
  if (!rows || !rows.length) return unavailableResult();

  const first = rows[0];
  const label = metricLabel(metric);
  return {
    answer: `Top ${label}: ${first.player} is number 1 with ${first.value}.`,
    data: {
      type: 'top_players',
      metric,
      rows
    },
    followups: defaultFollowups('top_players')
  };
}

function executeVenueStats({ venue, playerId, teamId, season, format }) {
  const result = datasetStore.getVenueStats({
    venue,
    playerId,
    teamId,
    season,
    format
  });
  if (!result) return unavailableResult();

  if (result.type === 'player') {
    const safe = sanitizePlayerStats(result.stats);
    if (!safe) {
      return {
        answer: 'Stats not available.',
        data: { type: 'venue_stats', venue, player: result.player },
        followups: defaultFollowups('player_stats')
      };
    }
    return {
      answer: `${result.player.name} at ${venue}: ${safe.matches} matches, ${safe.runs} runs, ${safe.wickets} wickets, average ${safe.average}.`,
      data: {
        type: 'venue_stats',
        venue,
        player: result.player,
        stats: safe
      },
      followups: defaultFollowups('player_stats')
    };
  }

  if (result.type === 'team') {
    const safe = sanitizeTeamStats(result.stats);
    if (!safe) {
      return {
        answer: 'Stats not available.',
        data: { type: 'venue_stats', venue, team: result.team },
        followups: defaultFollowups('team_stats')
      };
    }
    return {
      answer: `${result.team.name} at ${venue}: ${safe.matches} matches, ${safe.wins} wins, win rate ${safe.win_rate}%.`,
      data: {
        type: 'venue_stats',
        venue,
        team: result.team,
        stats: safe
      },
      followups: defaultFollowups('team_stats')
    };
  }

  return {
    answer: `${venue}: ${result.matches} matches found.`,
    data: {
      type: 'venue_stats',
      venue,
      matches: result.matches,
      recent_matches: result.recent_matches
    },
    followups: defaultFollowups('match_summary')
  };
}

function executeGlossary({ term = '' }) {
  const clean = normalizeText(term);
  const key = Object.keys(GLOSSARY).find((item) => clean.includes(normalizeText(item)));
  if (!key) return unavailableResult();

  return {
    answer: GLOSSARY[key],
    data: {
      type: 'glossary',
      term: key
    },
    followups: [
      `Ask for ${key} of a player`,
      'Ask top players using this metric',
      'Ask team stats'
    ]
  };
}

function executeAction(action, params = {}) {
  if (action === 'player_stats') return executePlayerStats({ ...params, action: 'player_stats' });
  if (action === 'player_season_stats') {
    return executePlayerStats({ ...params, action: 'player_season_stats' });
  }
  if (action === 'team_stats') return executeTeamStats({ ...params, action: 'team_stats' });
  if (action === 'match_summary') return executeMatchSummary(params);
  if (action === 'compare_players') return executeComparePlayers(params);
  if (action === 'head_to_head') return executeHeadToHead(params);
  if (action === 'top_players') return executeTopList(params);
  if (action === 'glossary') return executeGlossary(params);
  return unavailableResult();
}

module.exports = {
  executeAction,
  unavailableResult,
  sanitizePlayerStats,
  sanitizeTeamStats
};
