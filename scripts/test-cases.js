const NOT_AVAILABLE = 'I can help with cricket questions only.';

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

function parseSseEvents(raw = '') {
  return String(raw || '')
    .split(/\r?\n\r?\n/)
    .map((block) => block.trim())
    .filter(Boolean)
    .map((block) => {
      const lines = block
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line && !line.startsWith(':'));
      const event = lines.find((line) => line.startsWith('event:'));
      const data = lines
        .filter((line) => line.startsWith('data:'))
        .map((line) => line.slice(5).trim())
        .join('\n');
      return {
        event: event ? event.slice(6).trim() : 'message',
        data
      };
    });
}

async function askQuestionStream(question) {
  const response = await fetch(`${baseUrl}/api/query/stream?question=${encodeURIComponent(question)}`);
  const raw = await response.text();
  const events = parseSseEvents(raw);
  const resultEvent = events.find((event) => event.event === 'answer');

  return {
    status: response.status,
    events,
    payload: resultEvent ? JSON.parse(resultEvent.data || '{}') : null
  };
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function assertCommonShape(result, question) {
  const payload = result.payload || {};
  assert(typeof payload.summary === 'string', `${question}: missing summary`);
  assert(typeof payload.details === 'object' && payload.details !== null, `${question}: missing details`);
  assert(Array.isArray(payload.suggestions), `${question}: missing suggestions`);
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

  const streamed = await askQuestionStream('Virat Kohli ODI average');
  assert(streamed.status >= 200 && streamed.status < 300, 'streamed query: request failed');
  assert(streamed.events.some((event) => event.event === 'status'), 'streamed query: missing status event');
  assert(streamed.events.some((event) => event.event === 'answer'), 'streamed query: missing answer event');
  assertCommonShape(streamed, 'streamed query');

  const unsupported = await askQuestion('Who is best captain ever?');
  assertCommonShape(unsupported, 'Who is best captain ever?');
  assert(
    unsupported.payload.summary === NOT_AVAILABLE,
    'Who is best captain ever?: expected not available message'
  );

  console.log('All required test cases executed.');
}

run().catch((error) => {
  console.error('Test run failed:', error.message);
  process.exitCode = 1;
});
