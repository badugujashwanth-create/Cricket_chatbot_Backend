require('./loadEnv');

const { routeQuestion } = require('./llamaRouter');
const { processQuery } = require('./queryService');

const testQueries = [
  // Basic queries that should work fast
  { query: 'What is LBW?', category: 'Rule' },
  { query: 'What is powerplay?', category: 'Rule' },
  { query: 'Who won world cup 2011?', category: 'History' },
  { query: 'Virat Kohli stats', category: 'Player' },
  { query: 'Live score', category: 'Live' },
  { query: 'India team summary', category: 'Team' },
];

async function runTest(testCase, index) {
  try {
    const { query, category } = testCase;
    console.log(`\n[${index + 1}/${testQueries.length}] [${category}] "${query}"`);

    const route = await routeQuestion(query, {});
    console.log(`  Route: ${route.action}`);

    const result = await processQuery({ question: query });

    if (result.statusCode === 200) {
      const response = result.response || {};
      const summary = response.summary || 'No summary';
      const truncated = summary.length > 100 ? summary.substring(0, 100) + '...' : summary;
      console.log(`  [PASS] ${truncated}`);
      return { success: true, query, route: route.action };
    } else {
      console.log(`  [FAIL] Status: ${result.statusCode}`);
      return { success: false, query, route: route.action };
    }
  } catch (error) {
    console.log(`  [FAIL] Error: ${error.message.substring(0, 80)}`);
    return { success: false, query, error: error.message };
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('QUICK CRICKET CHATBOT TEST - SAMPLE QUERIES');
  console.log('='.repeat(70));

  const results = [];

  for (let i = 0; i < testQueries.length; i++) {
    const result = await runTest(testQueries[i], i);
    results.push(result);
  }

  console.log('\n' + '='.repeat(70));
  console.log('RESULTS');
  console.log('='.repeat(70));

  const passed = results.filter(r => r.success).length;
  console.log(`Passed: ${passed}/${results.length}`);
  console.log(`Success Rate: ${((passed / results.length) * 100).toFixed(0)}%`);
}

main().catch(error => {
  console.error('Test failed:', error);
  process.exitCode = 1;
});
