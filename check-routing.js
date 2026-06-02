require('./loadEnv');

const { routeQuestion } = require('./llamaRouter');

const slowQueries = [
  'What are the total runs scored by Virat Kohli in Test cricket?',
  'How many wickets has Jasprit Bumrah taken against Australia?',
  'How many centuries does Rohit Sharma have in T20 Internationals?'
];

async function checkRouting() {
  console.log('Checking routing for slow queries...\n');

  for (const query of slowQueries) {
    console.log(`Query: "${query}"`);
    console.time(`Route check`);
    const route = await routeQuestion(query, {});
    console.timeEnd(`Route check`);
    console.log(`  Action: ${route.action}`);
    console.log(`  Player: ${route.player}`);
    console.log(`  Format: ${route.format}`);
    console.log(`  Opponent: ${route.opponent}`);
    console.log();
  }
}

checkRouting().catch(console.error);
