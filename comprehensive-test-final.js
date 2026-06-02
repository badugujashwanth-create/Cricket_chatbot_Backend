require('./loadEnv');

const { routeQuestion } = require('./llamaRouter');
const { processQuery } = require('./queryService');

const testQueries = [
  // Player-specific stats
  { query: 'What are the total runs scored by Virat Kohli in Test cricket?', timeout: 15, category: 'Player' },
  { query: 'How many wickets has Jasprit Bumrah taken against Australia?', timeout: 15, category: 'Player' },
  { query: "What is Virat Kohli's batting average in ODIs?", timeout: 8, category: 'Player' },
  { query: "What is Jasprit Bumrah's economy rate in death overs?", timeout: 8, category: 'Rule' },
  { query: 'How many centuries does Rohit Sharma have in T20 Internationals?', timeout: 15, category: 'Player' },

  // Live/Current Match
  { query: 'What is the current live score of the India vs Australia match?', timeout: 10, category: 'Live' },
  { query: 'Show live score', timeout: 10, category: 'Live' },
  { query: 'Current match updates', timeout: 10, category: 'Live' },

  // Schedule and Toss
  { query: 'Who won the toss in the recent India vs Pakistan match?', timeout: 8, category: 'Match' },
  { query: 'When is the next India match scheduled?', timeout: 8, category: 'Schedule' },
  { query: 'What matches are scheduled today?', timeout: 8, category: 'Schedule' },

  // Series and Tournament Stats
  { query: 'Who are the top 5 run-scorers in the current IPL season?', timeout: 10, category: 'Top' },
  { query: 'What was the result of the last India vs England series?', timeout: 8, category: 'Series' },

  // Records and History
  { query: 'Who holds the record for the fastest fifty in T20 history?', timeout: 8, category: 'Record' },
  { query: 'What was the highest partnership for the 7th wicket in a World Cup final?', timeout: 8, category: 'Record' },

  // Cricket Rules and Concepts
  { query: 'Can you explain the LBW rule?', timeout: 5, category: 'Rule' },
  { query: 'What is the Duckworth-Lewis-Stern (DLS) method?', timeout: 5, category: 'Rule' },
  { query: 'What is powerplay?', timeout: 5, category: 'Rule' },
  { query: 'What is a free hit?', timeout: 5, category: 'Rule' },
  { query: 'What is a yorker?', timeout: 5, category: 'Rule' },

  // Team Stats (KNOWN TO HANG)
  { query: 'India team summary', timeout: 20, category: 'Team' },
  { query: 'Australia team statistics', timeout: 20, category: 'Team' },

  // Comparisons
  { query: 'Compare Virat Kohli vs Rohit Sharma in Test cricket', timeout: 10, category: 'Compare' },
  { query: 'Who is better: Babar Azam or Virat Kohli?', timeout: 10, category: 'Compare' },

  // Rankings
  { query: 'Who are the current ICC rankings for Test bowlers?', timeout: 8, category: 'Ranking' },
  { query: 'Top batsmen in ICC rankings', timeout: 8, category: 'Ranking' },

  // Player Info
  { query: 'Virat Kohli profile', timeout: 8, category: 'Profile' },
  { query: 'Who is MS Dhoni?', timeout: 8, category: 'Profile' }
];

async function runTestWithTimeout(query, timeoutSec) {
  return new Promise((resolve) => {
    let completed = false;
    const timeoutMs = timeoutSec * 1000;

    const promise = (async () => {
      try {
        const result = await processQuery({ question: query });
        completed = true;
        return { success: true, statusCode: result.statusCode, summary: result.response?.summary || 'No summary' };
      } catch (error) {
        completed = true;
        return { success: false, error: error.message };
      }
    })();

    const timeout = setTimeout(() => {
      if (!completed) {
        resolve({ success: false, timeout: true, message: `Timeout after ${timeoutSec}s` });
      }
    }, timeoutMs);

    promise.then((result) => {
      clearTimeout(timeout);
      resolve(result);
    });
  });
}

async function main() {
  console.log('='.repeat(80));
  console.log('COMPREHENSIVE CRICKET CHATBOT TEST SUITE');
  console.log('='.repeat(80));
  console.log(`Total Test Cases: ${testQueries.length}\n`);

  const results = [];
  let completedCount = 0;

  for (let i = 0; i < testQueries.length; i++) {
    const testCase = testQueries[i];
    process.stdout.write(`[${String(i + 1).padStart(2)}/${testQueries.length}] [${testCase.category.padEnd(7)}] Testing: "${testCase.query.substring(0, 50)}"`);

    const result = await runTestWithTimeout(testCase.query, testCase.timeout);

    if (result.timeout) {
      console.log(' [FAIL] TIMEOUT');
      results.push({ ...testCase, ...result, success: false });
    } else if (result.success && result.statusCode === 200) {
      const summary = String(result.summary || '').substring(0, 50);
      console.log(` [PASS] ${summary}...`);
      results.push({ ...testCase, ...result, success: true });
      completedCount++;
    } else {
      console.log(` [FAIL] ERROR`);
      results.push({ ...testCase, ...result, success: false });
    }
  }

  console.log('\n' + '='.repeat(80));
  console.log('TEST RESULTS SUMMARY');
  console.log('='.repeat(80));

  const passed = results.filter(r => r.success).length;
  const failed = results.filter(r => !r.success).length;
  const timeouts = results.filter(r => r.timeout).length;

  console.log(`Total: ${results.length} | Passed: ${passed} [PASS] | Failed: ${failed} [FAIL] | Timeout: ${timeouts} [TIMEOUT]`);
  console.log(`Success Rate: ${((passed / results.length) * 100).toFixed(1)}%\n`);

  // Group by category
  const byCategory = {};
  results.forEach(r => {
    if (!byCategory[r.category]) byCategory[r.category] = { pass: 0, fail: 0, timeout: 0 };
    if (r.success) byCategory[r.category].pass++;
    else if (r.timeout) byCategory[r.category].timeout++;
    else byCategory[r.category].fail++;
  });

  console.log('Results by Category:');
  Object.entries(byCategory).forEach(([cat, counts]) => {
    const total = counts.pass + counts.fail + counts.timeout;
    console.log(`  ${cat.padEnd(12)} : ${counts.pass}/${total} [PASS] | ${counts.fail} [FAIL] | ${counts.timeout} [TIMEOUT]`);
  });

  // Failed cases
  const failedCases = results.filter(r => !r.success);
  if (failedCases.length > 0) {
    console.log('\n' + '='.repeat(80));
    console.log('FAILED/TIMEOUT CASES:');
    console.log('='.repeat(80));
    failedCases.forEach((r, i) => {
      console.log(`${i + 1}. "${r.query}"`);
      if (r.timeout) console.log(`   [TIMEOUT] TIMEOUT after ${r.timeout}`);
      if (r.error) console.log(`   Error: ${r.error}`);
    });
  }

  console.log('\n' + '='.repeat(80));
  console.log('RECOMMENDATIONS:');
  console.log('='.repeat(80));
  if (timeouts > 0) {
    console.log(`- ${timeouts} query(ies) timed out - likely Chroma DB performance issue`);
    console.log('  -> Consider: 1) Reducing TEAM_LIMIT in vectorIndexService.js');
    console.log('              2) Implementing connection pooling for database');
    console.log('              3) Adding caching for frequently queried data');
  }
  if (passed === results.length) {
    console.log('- [PASS] All tests passed! The chatbot is ready for production.');
  } else {
    console.log(`- Review the ${failed} failed cases and implement fixes.`);
  }
}

main().catch(error => {
  console.error('Test suite failed:', error);
  process.exitCode = 1;
});
