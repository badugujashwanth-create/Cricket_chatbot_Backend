require('./loadEnv');

const path = require('path');
const fs = require('fs');
const { routeQuestion } = require('./llamaRouter');
const { processQuery } = require('./queryService');
const { queryVectorDb } = require('./chromaService');

console.log('\n=== COMPREHENSIVE SYSTEM TEST ===\n');

let testsPassed = 0;
let testsFailed = 0;

async function test(name, fn) {
  try {
    process.stdout.write(`[OK] ${name}...`);
    await fn();
    console.log(' PASS');
    testsPassed++;
  } catch (error) {
    console.log(' FAIL');
    console.log(`  Error: ${error.message}`);
    testsFailed++;
  }
}

async function runTests() {
  await test('Environment loader ran', () => {
    if (!process.env || typeof process.env !== 'object') {
      throw new Error('process.env is unavailable');
    }
  });

  await test('Vector database accessible', async () => {
    const result = await queryVectorDb('Virat Kohli', { k: 3 });
    if (!result) throw new Error('Vector DB returned null');
  });

  await test('Route: player_stats for known player', async () => {
    const route = await routeQuestion('virat kohli stats');
    if (route.action !== 'player_stats') {
      throw new Error(`Expected player_stats, got ${route.action}`);
    }
  });

  await test('Route: team_squad for squad query', async () => {
    const route = await routeQuestion('india squad');
    if (route.action !== 'team_squad') {
      throw new Error(`Expected team_squad, got ${route.action}`);
    }
  });

  await test('Route: general_knowledge for rules', async () => {
    const route = await routeQuestion('what is lbw');
    if (route.action !== 'general_knowledge') {
      throw new Error(`Expected general_knowledge, got ${route.action}`);
    }
  });

  await test('Query processing: player stats', async () => {
    const result = await processQuery({ question: 'virat kohli batting average' });
    if (!result || !result.response) {
      throw new Error('No response received');
    }
    if (!result.response.summary || !result.response.summary.trim()) {
      throw new Error('Empty summary in response');
    }
  });

  await test('Query processing: knowledge question', async () => {
    const result = await processQuery({ question: 'what is a boundary' });
    if (!result || !result.response) {
      throw new Error('No response received');
    }
    if (!result.response.summary || !result.response.summary.trim()) {
      throw new Error('Empty summary in response');
    }
  });

  await test('Entity type detection', async () => {
    const { detectEntityType } = require('./queryParser');
    const playerType = detectEntityType({ name: 'Virat Kohli', role: 'batsman' });
    const teamType = detectEntityType({ name: 'India', is_team: true });

    if (playerType !== 'PLAYER') throw new Error(`Expected PLAYER, got ${playerType}`);
    if (teamType !== 'TEAM') throw new Error(`Expected TEAM, got ${teamType}`);
  });

  await test('Route: live_update for current match', async () => {
    const route = await routeQuestion('current match score');
    if (route.action !== 'live_update') {
      throw new Error(`Expected live_update, got ${route.action}`);
    }
  });

  await test('Route: player_stats for unknown player', async () => {
    const route = await routeQuestion('zzzxxyy unknown player stats');
    if (route.action !== 'player_stats') {
      throw new Error(`Expected player_stats, got ${route.action}`);
    }
  });

  await test('Knowledge data files exist', () => {
    const dataDir = path.join(__dirname, 'data');
    const requiredFiles = [
      'cricket_rules.json',
      'cricket_terms.json',
      'cricket_history.json',
      'cricket_records.json',
      'worldcup_winners.json',
      'equipment_and_training.json'
    ];

    for (const file of requiredFiles) {
      const filePath = path.join(dataDir, file);
      if (!fs.existsSync(filePath)) {
        throw new Error(`Missing data file: ${file}`);
      }
      const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      if (!content || Object.keys(content).length === 0) {
        throw new Error(`Data file is empty: ${file}`);
      }
    }
  });

  await test('No duplicate entries in knowledge files', () => {
    const dataDir = path.join(__dirname, 'data');
    const files = fs.readdirSync(dataDir).filter((file) => file.endsWith('.json'));

    for (const file of files) {
      const content = JSON.parse(fs.readFileSync(path.join(dataDir, file), 'utf8'));
      const seen = new Set();
      for (const key in content) {
        if (seen.has(key.toLowerCase())) {
          throw new Error(`Duplicate key in ${file}: ${key}`);
        }
        seen.add(key.toLowerCase());
      }
    }
  });

  await test('All services import correctly', () => {
    try {
      require('./chromaService');
      require('./cricApiService');
      require('./knowledgeService');
      require('./queryParser');
      require('./queryService');
      require('./llamaRouter');
    } catch (error) {
      throw new Error(`Service import failed: ${error.message}`);
    }
  });

  await test('SQLite archive database exists', () => {
    const dbPath = path.join(__dirname, 'cricket_archive.sqlite3');
    if (!fs.existsSync(dbPath)) {
      throw new Error('cricket_archive.sqlite3 not found');
    }
  });

  console.log('\n=== TEST RESULTS ===');
  console.log(`Passed: ${testsPassed}`);
  console.log(`Failed: ${testsFailed}`);
  console.log(`Total: ${testsPassed + testsFailed}\n`);

  if (testsFailed === 0) {
    console.log('[OK] All tests passed. System ready for review.\n');
    process.exit(0);
  }

  console.log(`[FAIL] ${testsFailed} test(s) failed. Please fix before review.\n`);
  process.exit(1);
}

runTests().catch((error) => {
  console.error('Test suite error:', error);
  process.exit(1);
});
