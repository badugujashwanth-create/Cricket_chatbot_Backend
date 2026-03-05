const NOT_AVAILABLE = 'I can only answer using the data available in this project.';

const baseUrl = (process.env.TEST_BASE_URL || 'http://localhost:3000').replace(/\/+$/, '');

async function askQuestion(question) {
  const response = await fetch(`${baseUrl}/api/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ question })
  });
  const payload = await response.json();
  return { status: response.status, payload };
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function assertCommonShape(result, question) {
  const payload = result.payload || {};
  assert(typeof payload.answer === 'string', `${question}: missing answer`);
  assert(typeof payload.data === 'object' && payload.data !== null, `${question}: missing data`);
  assert(Array.isArray(payload.followups), `${question}: missing followups`);
}

async function run() {
  const cases = [
    'Virat Kohli ODI average',
    'Virat Kohli runs in 2019',
    'Top run scorers 2020',
    'Most wickets 2021',
    'India vs Australia head to head',
    'Compare Rohit Sharma vs Virat Kohli',
    'What is strike rate?'
  ];

  for (const question of cases) {
    const result = await askQuestion(question);
    assert(result.status >= 200 && result.status < 300, `${question}: request failed with ${result.status}`);
    assertCommonShape(result, question);
  }

  const unsupported = await askQuestion('Who is best captain ever?');
  assertCommonShape(unsupported, 'Who is best captain ever?');
  assert(
    unsupported.payload.answer === NOT_AVAILABLE,
    'Who is best captain ever?: expected not available message'
  );

  console.log('All required test cases executed.');
}

run().catch((error) => {
  console.error('Test run failed:', error.message);
  process.exitCode = 1;
});
