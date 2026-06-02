require('./loadEnv');

const { processQuery } = require('./queryService');

const slowQueries = [
  { query: 'What are the total runs scored by Virat Kohli in Test cricket?', timeout: 20 },
  { query: 'How many wickets has Jasprit Bumrah taken against Australia?', timeout: 20 },
  { query: 'How many centuries does Rohit Sharma have in T20 Internationals?', timeout: 20 }
];

async function testWithLongTimeout(query, timeoutSec) {
  return new Promise((resolve) => {
    let completed = false;
    const timeoutMs = timeoutSec * 1000;
    console.log(`\nTesting with ${timeoutSec}s timeout: "${query}"`);
    console.time('Query');

    const promise = (async () => {
      try {
        const result = await processQuery({ question: query });
        completed = true;
        return { success: result.statusCode === 200, result };
      } catch (error) {
        completed = true;
        return { success: false, error: error.message };
      }
    })();

    const timeout = setTimeout(() => {
      if (!completed) {
        console.timeEnd('Query');
        console.log('  [FAIL] TIMEOUT - no response after', timeoutSec, 'seconds');
        resolve({ timeout: true });
      }
    }, timeoutMs);

    promise.then((result) => {
      if (completed) {
        clearTimeout(timeout);
        console.timeEnd('Query');
        if (result.success) {
          const summary = String(result.result.response?.summary || '').substring(0, 80);
          console.log('  [PASS] SUCCESS:', summary + '...');
        } else {
          console.log('  [FAIL] ERROR:', result.error);
        }
        resolve(result);
      }
    });
  });
}

async function main() {
  console.log('='.repeat(80));
  console.log('EXTENDED TIMEOUT TEST FOR SLOW QUERIES');
  console.log('='.repeat(80));

  let passed = 0;
  for (const { query, timeout } of slowQueries) {
    const result = await testWithLongTimeout(query, timeout);
    if (result.success) passed++;
  }

  console.log('\n' + '='.repeat(80));
  console.log(`Results: ${passed}/${slowQueries.length} passed`);
}

main().catch(console.error);
