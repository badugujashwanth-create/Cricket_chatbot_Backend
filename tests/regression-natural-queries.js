require('../loadEnv');

const { processQuery } = require('../queryService');

const COUNTRY_LEAK_MARKERS = [
  'Australia, officially',
  'Australia is a country',
  'Commonwealth of Australia',
  'land area',
  'sixth-largest country',
  'Tasmania',
  'megadiverse country',
  'mainland of the Australian continent',
  'country in Oceania'
];

const QUERIES = [
  'who is best batsman',
  'best batsmen',
  'top 10 batsmen',
  'greatest batsman in cricket',
  'best batsman in India',
  'best batsman in Australia team',
  'who is virat',
  'who is kohli',
  'which team does virat play for',
  'what team does kohli play for',
  'which team did virat play for last',
  'australia team stats',
  'india win rate',
  'what is lbw',
  'live score',
  'unknownxyz player stats'
];

function assert(cond, msg) {
  if (!cond) {
    throw new Error(msg);
  }
}

async function run() {
  let passed = 0;
  let failed = 0;
  const failures = [];

  for (const question of QUERIES) {
    try {
      const outcome = await processQuery({ question });
      const res = outcome.response || {};
      const summary = String(res.summary || '').trim();
      const extra = res.extra && typeof res.extra === 'object' ? res.extra : {};
      const action = String(extra.action || '').trim();
      const qLower = question.toLowerCase();

      assert(outcome.statusCode === 200, `${question}: expected status 200, got ${outcome.statusCode}`);
      assert(summary.length > 0, `${question}: empty summary`);

      for (const marker of COUNTRY_LEAK_MARKERS) {
        assert(
          !summary.toLowerCase().includes(marker),
          `${question}: summary must not contain country-encyclopedia marker "${marker}"`
        );
      }

      if (qLower === 'who is best batsman' || qLower === 'best batsmen' || qLower === 'top 10 batsmen') {
        assert(
          !/Australia\s+has\s+\d+\s+wins/i.test(summary),
          `${question}: must not return Australia win-count team stats`
        );
        assert(!/Australia\s+team\s+stats/i.test(summary), `${question}: must not look like Australia team stats`);
        assert(
          !/Australia,\s+officially/i.test(summary) && !/Australia\s+is\s+a\s+country/i.test(summary),
          `${question}: must not include Australia country article text`
        );
        assert(action !== 'team_stats', `${question}: must not be classified as team_stats (got ${action})`);
        assert(/Sachin\s+Tendulkar/i.test(summary), `${question}: expected general batsmen guidance in summary`);
      }

      if (qLower === 'greatest batsman in cricket') {
        assert(/Sachin\s+Tendulkar/i.test(summary), `${question}: expected legendary names in summary`);
        assert(action !== 'team_stats', `${question}: must not be team_stats`);
      }

      if (qLower === 'which team did virat play for last') {
        assert(!/Fortune\s+Barishal/i.test(summary), `${question}: must not return Fortune Barishal`);
        assert(!/Australia\s+has\s+\d+\s+wins/i.test(summary), `${question}: must not return unrelated team stats`);
        assert(/Virat\s+Kohli/i.test(summary), `${question}: expected Virat Kohli in answer`);
        assert(/India/i.test(summary) && /Royal\s+Challengers/i.test(summary), `${question}: expected India and RCB mention`);
      }

      if (qLower === 'unknownxyz player stats') {
        assert(!/Australia\s+has\s+\d+\s+wins/i.test(summary), `${question}: must not invent Australia stats`);
        assert(!/India\s+has\s+\d+\s+wins/i.test(summary), `${question}: must not invent India stats`);
        assert(action !== 'team_stats', `${question}: unknown player must not become team_stats`);
      }

      if (qLower === 'best batsman in india') {
        assert(/Sachin\s+Tendulkar/i.test(summary), `${question}: expected India batting names`);
        assert(!/Australia,\s+officially/i.test(summary), `${question}: no Australia country article`);
      }

      if (qLower === 'best batsman in australia team') {
        assert(/Bradman|Ponting|Smith/i.test(summary), `${question}: expected Australia batting names`);
        assert(!/Australia\s+is\s+a\s+country/i.test(summary), `${question}: no country article`);
      }

      if (qLower === 'live score') {
        assert(action === 'live_update', `${question}: expected live_update action, got ${action}`);
      }

      passed += 1;
    } catch (err) {
      failed += 1;
      failures.push({ question, message: String(err.message || err) });
    }
  }

  console.log(`Regression natural queries: ${passed} passed, ${failed} failed (total ${QUERIES.length})`);
  if (failures.length) {
    for (const row of failures) {
      console.error(`FAIL: "${row.question}" -> ${row.message}`);
    }
    process.exitCode = 1;
  }
}

run().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
