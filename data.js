const players = [
  {
    id: 'p1',
    name: 'Rohit Sharma',
    team: 'India',
    role: 'Top-order Batsman',
    stats: {
      matches: 240,
      runs: 10422,
      average: 46.4,
      strikeRate: 88.9,
      centuries: 30,
      fifties: 45,
      recentScores: [95, 120, 44, 7, 112]
    },
    specialties: ['Power play pacing', 'Middle-overs acceleration']
  },
  {
    id: 'p2',
    name: 'Jasprit Bumrah',
    team: 'India',
    role: 'Fast Bowler',
    stats: {
      matches: 111,
      wickets: 176,
      average: 22.7,
      economy: 4.6,
      strikeRate: 37,
      recentScores: [0, 1, 0, 0, 1]
    },
    specialties: ['Death bowling', 'Reverse swing']
  },
  {
    id: 'p3',
    name: 'Kane Williamson',
    team: 'New Zealand',
    role: 'Top-order Batsman',
    stats: {
      matches: 180,
      runs: 8512,
      average: 53.2,
      strikeRate: 87.4,
      centuries: 29,
      fifties: 57,
      recentScores: [76, 53, 91, 34, 67]
    },
    specialties: ['Anchor innings', 'Switch-hit adaptability']
  },
  {
    id: 'p4',
    name: 'Ellyse Perry',
    team: 'Australia',
    role: 'All-rounder',
    stats: {
      matches: 245,
      runs: 7386,
      wickets: 173,
      average: 44.1,
      strikeRate: 88.7,
      economy: 4.1,
      recentScores: [82, 18, 46, 5, 90]
    },
    specialties: ['Seam bowling', 'Situational finishing']
  },
  {
    id: 'p5',
    name: 'Babar Azam',
    team: 'Pakistan',
    role: 'Middle-order Batsman',
    stats: {
      matches: 186,
      runs: 7554,
      average: 48.5,
      strikeRate: 88.2,
      centuries: 24,
      fifties: 41,
      recentScores: [102, 66, 47, 13, 78]
    },
    specialties: ['Classical technique', 'Second-innings control']
  },
  {
    id: 'p6',
    name: 'Trent Boult',
    team: 'New Zealand',
    role: 'Left-arm Fast Bowler',
    stats: {
      matches: 150,
      wickets: 213,
      average: 26.3,
      economy: 4.6,
      strikeRate: 30,
      recentScores: [0, 1, 0, 0, 2]
    },
    specialties: ['Swing bowling', 'New-ball aggression']
  }
];

const matches = [
  {
    id: 'match-1',
    date: '2025-01-05',
    tournament: 'ICC World Test Championship',
    stage: 'Final',
    teams: ['India', 'Australia'],
    venue: 'Melbourne Cricket Ground',
    winner: 'India',
    result: 'India won by 42 runs',
    totalRuns: 854,
    summary:
      'Rohit Sharma and K.S. Bharat stitched innings-defining partnerships, while Jasprit Bumrah dismissed the top order in the fourth innings.',
    highlights: ['Rohit Sharma 120', 'Jasprit Bumrah 4/56']
  },
  {
    id: 'match-2',
    date: '2025-01-12',
    tournament: 'Commonwealth Cup',
    stage: 'Semi-final',
    teams: ['Pakistan', 'England'],
    venue: 'Wankhede Stadium',
    winner: 'Pakistan',
    result: 'Pakistan won by 6 wickets',
    totalRuns: 412,
    summary:
      "Babar Azam's classy 102* guided Pakistan past 410, while Shaheen Afridi kept England from recovering in the final overs.",
    highlights: ['Babar Azam 102*', 'Shaheen Afridi 3/52']
  },
  {
    id: 'match-3',
    date: '2024-12-29',
    tournament: 'Asia Cup',
    stage: 'Group Stage',
    teams: ['India', 'Sri Lanka'],
    venue: 'Dubai International Stadium',
    winner: 'India',
    result: 'India won by 78 runs',
    totalRuns: 499,
    summary:
      'Virat Kohli and Shubman Gill piled on 210+ in the opening chase, with Kuldeep Yadav finishing off Sri Lanka cleanly.',
    highlights: ['Shubman Gill 135', 'Kuldeep Yadav 4/43']
  },
  {
    id: 'match-4',
    date: '2024-11-20',
    tournament: 'The Ashes',
    stage: 'Second Test',
    teams: ['England', 'Australia'],
    venue: "Lord's",
    winner: 'Australia',
    result: 'Australia won by 5 wickets',
    totalRuns: 533,
    summary: 'Marnus Labuschagne and Travis Head held the chase together after the Barmy Army fought valiantly in the first dig.',
    highlights: ['Travis Head 88', 'Pat Cummins 5/67']
  },
  {
    id: 'match-5',
    date: '2024-10-10',
    tournament: "Women's Championship Series",
    stage: 'Final',
    teams: ['Australia', 'India'],
    venue: 'Adelaide Oval',
    winner: 'Australia',
    result: 'Australia won by 3 runs',
    totalRuns: 318,
    summary: 'Ellyse Perry allround brilliance and the pace battery got across the line in a nail-biter.',
    highlights: ['Ellyse Perry 91', 'Darcie Brown 3/34']
  },
  {
    id: 'match-6',
    date: '2024-09-15',
    tournament: 'T20 Global League',
    stage: 'Group Stage',
    teams: ['South Africa', 'India'],
    venue: 'Newlands',
    winner: 'South Africa',
    result: 'South Africa won by 18 runs',
    totalRuns: 372,
    summary: 'Quinton de Kock and Aiden Markram torched the powerplay, leaving Hardik Pandya and co. with little to chase.',
    highlights: ['Aiden Markram 76', 'Kagiso Rabada 4/29']
  },
  {
    id: 'match-7',
    date: '2024-08-09',
    tournament: 'Bilateral ODI Series',
    stage: 'Third ODI',
    teams: ['New Zealand', 'Pakistan'],
    venue: 'Eden Park',
    winner: 'Pakistan',
    result: 'Pakistan won by 12 runs',
    totalRuns: 381,
    summary: 'Babar Azam anchored 90+ in a pressure chase while Naseem Shah delivered crucial strikes.',
    highlights: ['Babar Azam 94', 'Naseem Shah 3/41']
  },
  {
    id: 'match-8',
    date: '2024-07-01',
    tournament: 'Challenger Trophy',
    stage: 'Final',
    teams: ['India', 'England'],
    venue: 'Eden Gardens',
    winner: 'England',
    result: 'England won by 4 wickets',
    totalRuns: 430,
    summary: "Jos Buttler's finishing display chased down India's 430, with Adil Rashid stopping the tail.",
    highlights: ['Jos Buttler 102*', 'Adil Rashid 3/48']
  }
];

const teams = [
  { id: 'india', name: 'India', coach: 'Rahul Dravid', captain: 'Rohit Sharma', region: 'Asia' },
  { id: 'australia', name: 'Australia', coach: 'Andrew McDonald', captain: 'Pat Cummins', region: 'Oceania' },
  { id: 'england', name: 'England', coach: 'Brendon McCullum', captain: 'Ben Stokes', region: 'Europe' },
  { id: 'pakistan', name: 'Pakistan', coach: 'Grant Bradburn', captain: 'Babar Azam', region: 'Asia' },
  { id: 'new-zealand', name: 'New Zealand', coach: 'Gary Stead', captain: 'Kane Williamson', region: 'Oceania' },
  { id: 'south-africa', name: 'South Africa', coach: 'Shukri Conrad', captain: 'Temba Bavuma', region: 'Africa' }
];

const insights = [
  {
    id: 'insight-1',
    theme: 'Top-order stability',
    note:
      'Rohit Sharma, Kane Williamson, and Babar Azam are anchoring innings with an aggregate average above 47 in the sample dataset.'
  },
  {
    id: 'insight-2',
    theme: 'New-ball dominance',
    note:
      'The sample matches show Jasprit Bumrah, Trent Boult, and Shaheen Afridi consistently making early breakthroughs before the powerplay.'
  },
  {
    id: 'insight-3',
    theme: 'All-round match cogs',
    note:
      'Ellyse Perry and Shakib Al Hasan (future profiling) stay on the radar for dual contributions that tilt the balance toward their side.'
  }
];

const totalRuns = matches.reduce((sum, match) => sum + match.totalRuns, 0);
const averageRuns = (totalRuns / matches.length).toFixed(1);
const topRunPlayer = [...players]
  .filter((player) => typeof player.stats.runs === 'number')
  .sort((a, b) => b.stats.runs - a.stats.runs)[0];
const topFormPlayer = [...players]
  .filter((player) => Array.isArray(player.stats.recentScores))
  .sort(
    (a, b) =>
      avgRecent(a.stats.recentScores) - avgRecent(b.stats.recentScores)
  )
  .slice(-1)[0];

function avgRecent(scores = []) {
  if (!scores.length) return 0;
  return scores.reduce((sum, s) => sum + s, 0) / scores.length;
}

const coreMetrics = [
  {
    id: 'matches-indexed',
    label: 'Indexed match records',
    value: matches.length,
    detail: 'Record set seeded with representative fixtures.'
  },
  {
    id: 'player-profiles',
    label: 'Player profiles tracked',
    value: players.length,
    detail: 'Roster contains batters, bowlers, and all-rounders.'
  },
  {
    id: 'team-coverage',
    label: 'Team + tournament coverage',
    value: teams.length,
    detail: 'National teams that appear in the match feed.'
  },
  {
    id: 'avg-runs',
    label: 'Average runs per match',
    value: averageRuns,
    detail: 'Demonstrates how the analytics engine summarizes aggregate scoring.'
  },
  {
    id: 'top-batsman',
    label: 'Top run-getter (sample)',
    value: topRunPlayer ? `${topRunPlayer.name} • ${topRunPlayer.stats.runs}` : 'Data building',
    detail: 'From the selective player pool.'
  },
  {
    id: 'current-form',
    label: 'Best recent average',
    value:
      topFormPlayer && topFormPlayer.stats.average
        ? `${topFormPlayer.name} • ${topFormPlayer.stats.average.toFixed(1)}`
        : 'Awaiting activity',
    detail: 'Uses last five innings to score form.'
  }
];

const placeholderMetrics = Array.from({ length: 46 }, (_, index) => ({
  id: `metric-boost-${index + 1}`,
  label: `Metric slot ${index + 1}`,
  value: 'awaiting data expansion',
  detail: 'Ready to calculate once additional indices arrive.'
}));

const metrics = [...coreMetrics, ...placeholderMetrics];

function findPlayerByName(query) {
  const normalized = String(query || '').toLowerCase();
  if (!normalized) {
    return null;
  }

  return (
    players.find((player) => player.name.toLowerCase() === normalized) ||
    players.find((player) => player.name.toLowerCase().includes(normalized)) ||
    null
  );
}

function findTeamByName(query) {
  const normalized = String(query || '').toLowerCase().trim();
  if (!normalized) {
    return null;
  }

  return (
    teams.find((team) => team.name.toLowerCase() === normalized) ||
    teams.find((team) => team.name.toLowerCase().includes(normalized)) ||
    null
  );
}

function getRecentMatchesForTeam(teamName, limit = 3) {
  return matches
    .filter((match) => match.teams.some((team) => team.toLowerCase() === String(teamName || '').toLowerCase()))
    .slice(0, limit)
    .map((match) => ({
      id: match.id,
      date: match.date,
      venue: match.venue,
      tournament: match.tournament,
      teams: match.teams,
      result: match.result,
      highlights: match.highlights
    }));
}

function getRelatedMatchesForPlayer(playerName, limit = 3) {
  const normalized = String(playerName || '').toLowerCase().trim();
  if (!normalized) {
    return [];
  }

  return matches
    .filter((match) => {
      const summary = String(match.summary || '').toLowerCase();
      const highlights = (match.highlights || []).join(' ').toLowerCase();
      return summary.includes(normalized) || highlights.includes(normalized);
    })
    .slice(0, limit)
    .map((match) => ({
      id: match.id,
      date: match.date,
      tournament: match.tournament,
      venue: match.venue,
      teams: match.teams,
      result: match.result,
      highlights: match.highlights
    }));
}

function getMatchHighlights(limit = 3) {
  return matches.slice(0, limit).map((match) => ({
    id: match.id,
    date: match.date,
    venue: match.venue,
    teams: match.teams,
    result: match.result,
    highlights: match.highlights
  }));
}

module.exports = {
  players,
  matches,
  teams,
  insights,
  metrics,
  findPlayerByName,
  findTeamByName,
  getRecentMatchesForTeam,
  getRelatedMatchesForPlayer,
  getMatchHighlights
};
