const path = require('path');

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function loadModuleWithMocks(targetRelativePath, mocks) {
  const targetPath = require.resolve(targetRelativePath);
  const previousEntries = new Map();

  previousEntries.set(targetPath, require.cache[targetPath]);
  delete require.cache[targetPath];

  for (const [mockRelativePath, exports] of Object.entries(mocks)) {
    const resolved = require.resolve(mockRelativePath);
    previousEntries.set(resolved, require.cache[resolved]);
    require.cache[resolved] = {
      id: resolved,
      filename: resolved,
      loaded: true,
      exports
    };
  }

  try {
    return require(targetRelativePath);
  } finally {
    delete require.cache[targetPath];
    for (const [resolved, previous] of previousEntries.entries()) {
      if (previous) {
        require.cache[resolved] = previous;
      } else {
        delete require.cache[resolved];
      }
    }
  }
}

function runEntityResolverChecks() {
  const fakeIndex = {
    players: [
      { id: 'virat-kohli', name: 'Virat Kohli' },
      { id: 'virat-singh', name: 'Virat Singh' },
      { id: 'rohit-sharma', name: 'Rohit Sharma' }
    ],
    teams: [
      { id: 'rcb', name: 'Royal Challengers Bengaluru' },
      { id: 'srh', name: 'Sunrisers Hyderabad' }
    ],
    venues: []
  };

  const { resolvePlayer, resolveTeam } = loadModuleWithMocks('../entityResolver', {
    '../datasetStore': {
      getEntityIndex: () => fakeIndex
    }
  });

  const exactPlayer = resolvePlayer('virat kohli');
  assert(exactPlayer.status === 'resolved', 'Exact player name should resolve.');
  assert(
    exactPlayer.item.name === 'Virat Kohli',
    'Resolved player output should preserve the exact database name.'
  );

  const ambiguousPlayer = resolvePlayer('Virat');
  assert(
    ambiguousPlayer.status === 'clarify',
    'Ambiguous low-confidence player queries should request clarification.'
  );

  const aliasTeam = resolveTeam('RCB');
  assert(aliasTeam.status === 'resolved', 'Known team alias should resolve.');
  assert(
    aliasTeam.item.name === 'Royal Challengers Bengaluru',
    'Resolved team output should preserve the exact database name.'
  );
}

async function runQueryServiceChecks() {
  let executeCalled = false;

  const { processQuery } = loadModuleWithMocks('../queryService', {
    '../datasetStore': {
      waitUntilReady: async () => ({ ready: true })
    },
    '../llamaRouter': {
      routeQuestion: async () => ({
        action: 'player_stats',
        player: 'Virat'
      })
    },
    '../entityResolver': {
      resolvePlayer: () => ({
        status: 'clarify',
        query: 'Virat',
        choices: ['Virat Kohli', 'Virat Singh']
      }),
      resolveTeam: () => ({
        status: 'not_found'
      })
    },
    '../statsService': {
      executeAction: () => {
        executeCalled = true;
        return {
          answer: 'unexpected',
          data: {},
          followups: []
        };
      },
      unavailableResult: () => ({
        answer: 'unexpected',
        data: {},
        followups: []
      })
    }
  });

  const outcome = await processQuery({
    question: 'Virat stats'
  });

  assert(executeCalled === false, 'Low-confidence name resolution must not execute a guessed action.');
  assert(
    /could not confidently verify the player name/i.test(outcome.response.answer),
    'Clarification response should explain that the player name could not be verified confidently.'
  );
  assert(
    outcome.response.data?.type === 'name_resolution',
    'Clarification response should expose name-resolution metadata.'
  );
  assert(
    outcome.response.followups?.[0] === 'Use exact player name: Virat Kohli',
    'Clarification followups should preserve exact verified database names.'
  );
}

async function run() {
  runEntityResolverChecks();
  await runQueryServiceChecks();
  console.log('Name-resolution checks passed.');
}

run().catch((error) => {
  console.error('Name-resolution test failed:', error.message);
  process.exitCode = 1;
});
