require('./loadEnv');

const { routeQuestion } = require('./llamaRouter');
const { processQuery } = require('./queryService');

const testQueries = [
  // Player-specific stats
  'What are the total runs scored by Virat Kohli in Test cricket?',
  'How many wickets has Jasprit Bumrah taken against Australia?',
  "What is Virat Kohli's batting average in ODIs?",
  "What is Jasprit Bumrah's economy rate in death overs?",
  'How many centuries does Rohit Sharma have in T20 Internationals?',

  // Live/Current Match
  'What is the current live score of the India vs Australia match?',
  'Show live score',
  'Current match updates',

  // Schedule and Toss
  'Who won the toss in the recent India vs Pakistan match?',
  'When is the next India match scheduled?',
  'What matches are scheduled today?',

  // Series and Tournament Stats
  'Who are the top 5 run-scorers in the current IPL season?',
  'What was the result of the last India vs England series?',

  // Records and History
  'Who holds the record for the fastest fifty in T20 history?',
  'What was the highest partnership for the 7th wicket in a World Cup final?',

  // Cricket Rules and Concepts
  'Can you explain the LBW rule?',
  'What is the Duckworth-Lewis-Stern (DLS) method?',
  'What is powerplay?',
  'What is a free hit?',
  'What is a yorker?',

  // Team Stats
  'India team summary',
  'Australia team statistics',

  // Comparisons
  'Compare Virat Kohli vs Rohit Sharma in Test cricket',
  'Who is better: Babar Azam or Virat Kohli?',

  // Rankings
  'Who are the current ICC rankings for Test bowlers?',
  'Top batsmen in ICC rankings',

  // Player Info
  'Virat Kohli profile',
  'Who is MS Dhoni?'
];

async function runTest(question, index) {
  try {
    console.log(`\n[${index + 1}/${testQueries.length}] Testing: "${question}"`);

    const route = await routeQuestion(question, {});
    console.log(`  [PASS] Route: ${route.action || 'unknown'}`);

    const result = await processQuery({ question });

    if (result.statusCode === 200) {
      const response = result.response || {};
      const summary = response.summary || 'No summary';
      const truncated = summary.length > 150 ? summary.substring(0, 150) + '...' : summary;
      console.log(`  [PASS] Status: 200 OK`);
      console.log(`  [PASS] Summary: "${truncated}"`);
      console.log(`  [PASS] Extra action: ${response.extra?.action || 'none'}`);
      return { success: true, question, route: route.action, statusCode: result.statusCode };
    } else {
      console.log(`  [FAIL] Status: ${result.statusCode}`);
      console.log(`  [FAIL] Error: ${JSON.stringify(result.response)}`);
      return { success: false, question, route: route.action, statusCode: result.statusCode };
    }
  } catch (error) {
    console.log(`  [FAIL] Exception: ${error.message}`);
    return { success: false, question, error: error.message };
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('COMPREHENSIVE CRICKET CHATBOT TEST SUITE');
  console.log('='.repeat(70));

  const results = [];

  for (let i = 0; i < testQueries.length; i++) {
    const result = await runTest(testQueries[i], i);
    results.push(result);
  }

  console.log('\n' + '='.repeat(70));
  console.log('TEST SUMMARY');
  console.log('='.repeat(70));

  const passed = results.filter(r => r.success).length;
  const failed = results.filter(r => !r.success).length;

  console.log(`Total Tests: ${results.length}`);
  console.log(`Passed: ${passed} [PASS]`);
  console.log(`Failed: ${failed} [FAIL]`);
  console.log(`Success Rate: ${((passed / results.length) * 100).toFixed(2)}%`);

  if (failed > 0) {
    console.log('\nFailed Queries:');
    results.filter(r => !r.success).forEach((r, i) => {
      console.log(`  ${i + 1}. "${r.question}"`);
      if (r.error) console.log(`     Error: ${r.error}`);
    });
  }

  console.log('\nRoutes Distribution:');
  const routeCount = {};
  results.forEach(r => {
    if (r.route) {
      routeCount[r.route] = (routeCount[r.route] || 0) + 1;
    }
  });
  Object.entries(routeCount).forEach(([route, count]) => {
    console.log(`  ${route}: ${count}`);
  });
}

main().catch(error => {
  console.error('Test suite failed:', error);
  process.exitCode = 1;
});
