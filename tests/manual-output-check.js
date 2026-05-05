require('../loadEnv');

const { processQuery } = require('../queryService');

const QUERIES = [
  'who is best batsman',
  'best batsmen',
  'which team did virat play for last',
  'who is virat',
  'australia team stats',
  'what is lbw'
];

async function run() {
  for (const question of QUERIES) {
    const outcome = await processQuery({ question });
    const response = outcome.response || {};
    const extra = response.extra && typeof response.extra === 'object' ? response.extra : {};

    console.log('='.repeat(72));
    console.log(`Query: ${question}`);
    console.log(`Status: ${outcome.statusCode}`);
    console.log(`Type: ${response.type || ''}`);
    console.log(`Action: ${extra.action || ''}`);
    console.log(`Summary: ${String(response.summary || '').trim()}`);
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
