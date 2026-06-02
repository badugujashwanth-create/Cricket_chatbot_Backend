require('./loadEnv');

const { processQuery } = require('./queryService');

const queries = [
  'Live score',
  'India team summary',
];

async function testQuery(query, timeout = 15000) {
  console.log(`\nTesting: "${query}"`);
  console.time(`Query: ${query}`);

  let completed = false;
  let result = null;
  let error = null;

  const promise = (async () => {
    try {
      result = await processQuery({ question: query });
      completed = true;
    } catch (err) {
      error = err;
      completed = true;
    }
  })();

  const timeoutPromise = new Promise((resolve) => {
    setTimeout(() => {
      if (!completed) {
        console.timeEnd(`Query: ${query}`);
        console.log('  [FAIL] TIMEOUT after', timeout, 'ms');
        process.exit(1);
      } else {
        resolve();
      }
    }, timeout);
  });

  await Promise.race([promise, timeoutPromise]);

  if (error) {
    console.log('  [FAIL] Error:', error.message);
  } else if (result) {
    console.timeEnd(`Query: ${query}`);
    console.log('  [PASS] Status:', result.statusCode);
    const response = result.response || {};
    const summary = response.summary || 'No summary';
    console.log('  [PASS] Summary:', summary.substring(0, 80) + '...');
  }
}

async function main() {
  console.log('='.repeat(70));
  console.log('DIAGNOSTIC TEST - TIMEOUT DETECTION');
  console.log('='.repeat(70));

  for (const query of queries) {
    await testQuery(query, 10000);
  }
}

main().catch(error => {
  console.error('Diagnostic failed:', error);
  process.exitCode = 1;
});
