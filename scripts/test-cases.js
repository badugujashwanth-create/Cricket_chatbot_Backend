require('../loadEnv');

const { routeQuestion } = require('../llamaRouter');
const { processQuery } = require('../queryService');
const { queryVectorDb } = require('../chromaService');

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function runCase({
  question,
  expectedAction = '',
  expectedExtraAction = '',
  expectedSummaryPattern = null
}) {
  const route = await routeQuestion(question, {});
  const result = await processQuery({ question });
  const response = result.response || {};

  assert(result.statusCode === 200, `${question}: expected 200, got ${result.statusCode}`);
  assert(typeof response.summary === 'string' && response.summary.trim(), `${question}: missing summary`);
  assert(response.extra && typeof response.extra === 'object', `${question}: missing extra payload`);

  if (expectedAction) {
    assert(route.action === expectedAction, `${question}: expected route ${expectedAction}, got ${route.action}`);
  }

  if (expectedExtraAction) {
    assert(
      String(response.extra.action || '').trim() === expectedExtraAction,
      `${question}: expected extra.action ${expectedExtraAction}, got ${response.extra.action}`
    );
  }

  if (expectedSummaryPattern) {
    assert(
      expectedSummaryPattern.test(String(response.summary || '')),
      `${question}: summary did not match ${expectedSummaryPattern}`
    );
  }

  return { route, result, response };
}

async function main() {
  const liveCases = [
    { question: 'live score', expectedAction: 'live_update', expectedExtraAction: 'live_update' },
    { question: 'current match', expectedAction: 'live_update', expectedExtraAction: 'live_update' },
    { question: 'today match schedule', expectedAction: 'live_update', expectedExtraAction: 'live_update' }
  ];

  const archiveCases = [
    {
      question: 'virat kohli stats',
      expectedAction: 'player_stats',
      expectedExtraAction: 'player_stats',
      expectedSummaryPattern: /virat|kohli/i
    },
    {
      question: 'ms dhoni odi average',
      expectedAction: 'player_stats',
      expectedExtraAction: 'player_stats',
      expectedSummaryPattern: /dhoni|average/i
    },
    {
      question: 'india team summary',
      expectedAction: 'team_stats',
      expectedExtraAction: 'team_stats',
      expectedSummaryPattern: /india|matches|wins/i
    }
  ];

  const knowledgeCases = [
    {
      question: 'what is lbw',
      expectedAction: 'general_knowledge',
      expectedExtraAction: 'general_knowledge',
      expectedSummaryPattern: /leg before wicket|lbw/i
    },
    {
      question: 'what is powerplay',
      expectedAction: 'general_knowledge',
      expectedExtraAction: 'general_knowledge',
      expectedSummaryPattern: /powerplay|fielding restriction/i
    },
    {
      question: 'what is free hit in cricket',
      expectedAction: 'general_knowledge',
      expectedExtraAction: 'general_knowledge',
      expectedSummaryPattern: /free hit|no-ball|no ball/i
    },
    {
      question: 'difference between odi and t20',
      expectedAction: 'general_knowledge',
      expectedExtraAction: 'general_knowledge',
      expectedSummaryPattern: /odi|t20|overs/i
    },
    {
      question: 'what is yorker ball',
      expectedAction: 'general_knowledge',
      expectedExtraAction: 'general_knowledge',
      expectedSummaryPattern: /yorker|full delivery/i
    }
  ];

  const historyCases = [
    {
      question: 'who won wc 2011',
      expectedAction: 'general_knowledge',
      expectedExtraAction: 'general_knowledge',
      expectedSummaryPattern: /india|2011/i
    },
    {
      question: 'highest individual score in odi',
      expectedAction: 'record_lookup',
      expectedExtraAction: 'record_lookup',
      expectedSummaryPattern: /264|rohit/i
    },
    {
      question: 'fastest century in odi',
      expectedAction: 'record_lookup',
      expectedExtraAction: 'record_lookup',
      expectedSummaryPattern: /ab de villiers|31 balls/i
    }
  ];

  const analysisCases = [
    {
      question: 'virat vs babar in odi',
      expectedAction: 'compare_players',
      expectedExtraAction: 'compare_players'
    },
    {
      question: 'who may win today match',
      expectedAction: 'subjective_analysis',
      expectedExtraAction: 'subjective_analysis'
    },
    {
      question: 'best fantasy captain today',
      expectedAction: 'subjective_analysis',
      expectedExtraAction: 'subjective_analysis'
    }
  ];

  const typoCase = await runCase({
    question: 'virat t20 sr',
    expectedAction: 'player_stats',
    expectedExtraAction: 'player_stats'
  });
  assert(/virat|kohli|strike rate|sr/i.test(typoCase.response.summary), 'virat t20 sr: weak typo handling');

  for (const testCase of [
    ...liveCases,
    ...archiveCases,
    ...knowledgeCases,
    ...historyCases,
    ...analysisCases
  ]) {
    await runCase(testCase);
  }

  const unknownPlayer = await runCase({
    question: 'zzzxxyy unknown player stats',
    expectedAction: 'player_stats'
  });
  assert(unknownPlayer.response.summary.trim(), 'unknown player path returned empty summary');

  const degradedVector = await queryVectorDb('Virat Kohli', {
    dbDir: 'Z:\\definitely-missing-chroma-db'
  });
  assert(degradedVector && degradedVector.available === false, 'missing Chroma path should degrade cleanly');
  assert(typeof degradedVector.warning === 'string' && degradedVector.warning.trim(), 'missing Chroma path should return a warning');

  console.log('Smoke coverage passed.');
}

main().catch((error) => {
  console.error('Test run failed:', error.message);
  process.exitCode = 1;
});
