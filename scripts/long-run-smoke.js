#!/usr/bin/env node

const DEFAULT_BASE_URL = process.env.SMOKE_BASE_URL || 'http://localhost:3000';
const DEFAULT_DURATION_MINUTES = Number(process.env.SMOKE_DURATION_MINUTES || 180);
const DEFAULT_INTERVAL_SECONDS = Number(process.env.SMOKE_INTERVAL_SECONDS || 300);

function parseArgs(argv = []) {
  const args = {
    baseUrl: DEFAULT_BASE_URL,
    durationMinutes: DEFAULT_DURATION_MINUTES,
    intervalSeconds: DEFAULT_INTERVAL_SECONDS
  };

  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    const next = argv[index + 1];
    if (current === '--base-url' && next) {
      args.baseUrl = next;
      index += 1;
    } else if (current === '--duration-minutes' && next) {
      args.durationMinutes = Number(next) || DEFAULT_DURATION_MINUTES;
      index += 1;
    } else if (current === '--interval-seconds' && next) {
      args.intervalSeconds = Number(next) || DEFAULT_INTERVAL_SECONDS;
      index += 1;
    }
  }

  return args;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function safeNow() {
  return new Date().toISOString();
}

async function requestJson(url, init = {}) {
  const startedAt = Date.now();
  const response = await fetch(url, init);
  const elapsedMs = Date.now() - startedAt;
  const text = await response.text();
  let body = null;
  try {
    body = JSON.parse(text);
  } catch (_) {
    body = { raw: text };
  }
  return {
    ok: response.ok,
    status: response.status,
    elapsedMs,
    body
  };
}

async function runIteration(baseUrl, iteration) {
  const samples = [
    {
      name: 'status',
      run: () => requestJson(`${baseUrl}/api/status`)
    },
    {
      name: 'player_stats',
      run: () =>
        requestJson(`${baseUrl}/api/query`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question: 'Virat Kohli stats' })
        })
    },
    {
      name: 'comparison',
      run: () =>
        requestJson(`${baseUrl}/api/query`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question: 'India vs Australia' })
        })
    },
    {
      name: 'record',
      run: () =>
        requestJson(`${baseUrl}/api/query`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question: 'most sixes in IPL' })
        })
    },
    {
      name: 'live_scores',
      run: () => requestJson(`${baseUrl}/api/cricapi/live-scores?includeRecent=true&limit=2`)
    }
  ];

  for (const sample of samples) {
    try {
      const result = await sample.run();
      const summary =
        result.body?.summary ||
        result.body?.message ||
        result.body?.status ||
        result.body?.type ||
        '';
      console.log(
        JSON.stringify({
          ts: safeNow(),
          iteration,
          sample: sample.name,
          ok: result.ok,
          status: result.status,
          elapsed_ms: result.elapsedMs,
          summary: String(summary).slice(0, 240)
        })
      );
    } catch (error) {
      console.log(
        JSON.stringify({
          ts: safeNow(),
          iteration,
          sample: sample.name,
          ok: false,
          status: 0,
          elapsed_ms: 0,
          error: error?.message || 'request_failed'
        })
      );
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const totalIterations = Math.max(1, Math.ceil((args.durationMinutes * 60) / args.intervalSeconds));

  console.log(
    JSON.stringify({
      ts: safeNow(),
      event: 'start',
      base_url: args.baseUrl,
      duration_minutes: args.durationMinutes,
      interval_seconds: args.intervalSeconds,
      iterations: totalIterations
    })
  );

  for (let iteration = 1; iteration <= totalIterations; iteration += 1) {
    await runIteration(args.baseUrl, iteration);
    if (iteration < totalIterations) {
      await sleep(args.intervalSeconds * 1000);
    }
  }

  console.log(
    JSON.stringify({
      ts: safeNow(),
      event: 'done'
    })
  );
}

main().catch((error) => {
  console.error(
    JSON.stringify({
      ts: safeNow(),
      event: 'fatal',
      error: error?.message || 'unknown_error'
    })
  );
  process.exitCode = 1;
});
