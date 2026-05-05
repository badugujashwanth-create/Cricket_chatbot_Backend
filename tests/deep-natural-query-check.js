require('../loadEnv');

const { processQuery } = require('../queryService');

const QUERY_TIMEOUT_MS = 45000;

const CASES = [
  { question: 'who is best batsman', category: 'ranking', generalRanking: true, mustNotBeTeamStats: true },
  { question: 'best batsmen', category: 'ranking', generalRanking: true, mustNotBeTeamStats: true },
  { question: 'top 10 batsmen', category: 'ranking', generalRanking: true, mustNotBeTeamStats: true },
  { question: 'greatest batsman in cricket', category: 'ranking', generalRanking: true },
  { question: 'best bowler', category: 'ranking', generalRanking: true },
  { question: 'top cricket players', category: 'ranking', generalRanking: true },
  { question: 'best batsman in India', category: 'ranking' },
  { question: 'best batsman in Australia team', category: 'ranking' },

  { question: 'who is virat', category: 'player_identity' },
  { question: 'who is kohli', category: 'player_identity' },
  { question: 'who is dhoni', category: 'player_identity' },
  { question: 'who is msd', category: 'player_identity' },
  { question: 'who is rohit', category: 'player_identity' },
  { question: 'who is sachin', category: 'player_identity' },
  { question: 'who is babar', category: 'player_identity' },
  { question: 'who is bumrah', category: 'player_identity' },

  { question: 'which team does virat play for', category: 'player_team', viratTeam: true },
  { question: 'what team does kohli play for', category: 'player_team', viratTeam: true },
  { question: 'which team did virat play for last', category: 'player_team', viratTeam: true },
  { question: 'virat latest team', category: 'player_team', viratTeam: true },
  { question: 'kohli latest match team', category: 'player_team', viratTeam: true },
  { question: 'what team does dhoni play for', category: 'player_team' },
  { question: 'what team does rohit play for', category: 'player_team' },

  { question: 'australia team stats', category: 'team_stats', explicitTeamStats: true },
  { question: 'india win rate', category: 'team_stats', explicitTeamStats: true },
  { question: 'pakistan team stats', category: 'team_stats', explicitTeamStats: true },
  { question: 'england win rate', category: 'team_stats', explicitTeamStats: true },
  { question: 'australia last match', category: 'team_stats' },
  { question: 'india last match', category: 'team_stats' },

  { question: 'what is lbw', category: 'rules' },
  { question: 'what is dls', category: 'rules' },
  { question: 'what is powerplay', category: 'rules' },
  { question: 'what is free hit', category: 'rules' },
  { question: 'what is yorker', category: 'rules' },
  { question: 'what is economy rate', category: 'rules' },

  { question: 'live score', category: 'live' },
  { question: 'today match', category: 'live' },
  { question: 'current match', category: 'live' },
  { question: 'india match today', category: 'live' },

  { question: 'unknownxyz player stats', category: 'fallback', unknown: true },
  { question: 'random abc cricket team stats', category: 'fallback', unknown: true },
  { question: 'tell me about fakeplayer123', category: 'fallback', unknown: true },
  { question: 'best batsman in fakecountry', category: 'fallback', unknown: true, generalRanking: true },
  { question: 'which team does unknownplayer play for', category: 'fallback', unknown: true }
];

const COUNTRY_LEAK_PATTERNS = [
  /Australia,\s+officially/i,
  /Australia\s+is\s+a\s+country/i,
  /Commonwealth\s+of\s+Australia/i,
  /\bland\s+area\b/i,
  /sixth-largest\s+country/i,
  /\bTasmania\b/i,
  /mainland\s+of\s+the\s+Australian\s+continent/i,
  /megadiverse\s+country/i
];

function withTimeout(promise, ms, label) {
  let timer = null;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label}: timed out after ${ms}ms`)), ms);
  });

  return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timer));
}

function getAction(response = {}) {
  return String(response.extra?.action || '').trim();
}

function getSearchableText(response = {}) {
  const pieces = [
    response.summary,
    response.title,
    response.type,
    response.extra?.action,
    response.extra?.team_description,
    response.extra?.player_description,
    response.extra?.insights,
    response.extra?.detected_entities,
    response.extra?.entities
  ];

  return pieces
    .map((value) => {
      if (value === null || value === undefined) return '';
      if (typeof value === 'string') return value;
      return JSON.stringify(value);
    })
    .join(' ');
}

function hasRandomTeamStats(text = '') {
  return (
    /Australia\s+has\s+\d+\s+wins/i.test(text) ||
    /India\s+has\s+\d+\s+wins/i.test(text) ||
    /Pakistan\s+has\s+\d+\s+wins/i.test(text) ||
    /England\s+has\s+\d+\s+wins/i.test(text)
  );
}

function validateCase(testCase, outcome, durationMs) {
  const response = outcome.response || {};
  const summary = String(response.summary || '').trim();
  const action = getAction(response);
  const searchableText = getSearchableText(response);
  const failures = [];

  if (outcome.statusCode !== 200) {
    failures.push(`expected status 200, got ${outcome.statusCode}`);
  }

  if (!summary) {
    failures.push('empty answer text');
  }

  for (const pattern of COUNTRY_LEAK_PATTERNS) {
    if (pattern.test(searchableText)) {
      failures.push(`country encyclopedia leak matched ${pattern}`);
    }
  }

  if (testCase.mustNotBeTeamStats && action === 'team_stats') {
    failures.push(`general ranking routed to team_stats`);
  }

  if (testCase.generalRanking) {
    if (action === 'team_stats' || action === 'player_stats') {
      failures.push(`general ranking routed to ${action}`);
    }
    if (/Australia\s+team\s+stats/i.test(searchableText)) {
      failures.push('general ranking returned Australia team stats label');
    }
    if (/Australia\s+has\s+751\s+wins/i.test(searchableText)) {
      failures.push('general ranking returned Australia has 751 wins');
    }
  }

  if (testCase.viratTeam && /Fortune\s+Barishal/i.test(searchableText)) {
    failures.push('Virat team query returned Fortune Barishal');
  }

  if (testCase.unknown && hasRandomTeamStats(searchableText)) {
    failures.push('unknown/fake query returned random team stats');
  }

  return {
    question: testCase.question,
    category: testCase.category,
    statusCode: outcome.statusCode,
    action,
    summary,
    durationMs,
    passed: failures.length === 0,
    failures
  };
}

async function runOne(testCase) {
  const start = Date.now();
  const outcome = await withTimeout(
    processQuery({ question: testCase.question }),
    QUERY_TIMEOUT_MS,
    testCase.question
  );
  const durationMs = Date.now() - start;
  return validateCase(testCase, outcome, durationMs);
}

async function run() {
  const results = [];

  for (const testCase of CASES) {
    const start = Date.now();
    try {
      results.push(await runOne(testCase));
    } catch (error) {
      results.push({
        question: testCase.question,
        category: testCase.category,
        statusCode: 0,
        action: '',
        summary: '',
        durationMs: Date.now() - start,
        passed: false,
        failures: [String(error.message || error)]
      });
    }
  }

  const total = results.length;
  const failedRows = results.filter((row) => !row.passed);
  const passed = total - failedRows.length;
  const averageMs = total
    ? results.reduce((sum, row) => sum + Number(row.durationMs || 0), 0) / total
    : 0;
  const slowest = [...results]
    .sort((left, right) => Number(right.durationMs || 0) - Number(left.durationMs || 0))
    .slice(0, 5);

  console.log(`Deep natural query check`);
  console.log(`Total queries: ${total}`);
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failedRows.length}`);
  console.log(`Average response time: ${averageMs.toFixed(0)}ms`);
  console.log(`Slowest 5 queries:`);
  for (const row of slowest) {
    console.log(`- ${row.durationMs}ms | ${row.question} | action=${row.action || 'n/a'}`);
  }

  if (failedRows.length) {
    console.log(`Failed queries:`);
    for (const row of failedRows) {
      console.log(`- ${row.question}: ${row.failures.join('; ')}`);
      if (row.summary) {
        console.log(`  Summary: ${row.summary}`);
      }
    }
    process.exitCode = 1;
  } else {
    console.log(`Failed queries: none`);
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
