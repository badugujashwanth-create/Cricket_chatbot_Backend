const { ROUTER_SCHEMA, SUPPORTED_ACTIONS } = require('./constants');
const { callLlama } = require('./llamaClient');
const { normalizeText } = require('./textUtils');
const { cleanEntitySegment, parseVsSides } = require('./queryParser');

const YEAR_REGEX = /\b(19\d{2}|20\d{2})\b/;
const MATCH_ID_REGEX = /\b(\d{5,})\b/;

function validateType(value, type) {
  if (type === 'string') return typeof value === 'string';
  if (type === 'number') return typeof value === 'number' && Number.isFinite(value);
  if (type === 'object') return value && typeof value === 'object' && !Array.isArray(value);
  if (type === 'array') return Array.isArray(value);
  if (type === 'boolean') return typeof value === 'boolean';
  return true;
}

function validateAgainstSchema(schema, value) {
  if (!schema) return { valid: true, errors: [] };
  const errors = [];

  function visit(currentSchema, currentValue, path) {
    if (!currentSchema) return true;

    if (currentSchema.anyOf) {
      let validOption = false;
      for (const option of currentSchema.anyOf) {
        const optionErrors = [];
        const pushRef = errors.push;
        errors.push = (...args) => optionErrors.push(...args);
        const ok = visit(option, currentValue, path);
        errors.push = pushRef;
        if (ok && optionErrors.length === 0) {
          validOption = true;
          break;
        }
      }
      if (!validOption) {
        errors.push(`${path} does not match allowed value types`);
        return false;
      }
      return true;
    }

    if (currentSchema.type && !validateType(currentValue, currentSchema.type)) {
      errors.push(`${path} must be ${currentSchema.type}`);
      return false;
    }

    if (currentSchema.enum && !currentSchema.enum.includes(currentValue)) {
      errors.push(`${path} must be one of ${currentSchema.enum.join(', ')}`);
      return false;
    }

    if (currentSchema.type === 'object') {
      const required = currentSchema.required || [];
      for (const key of required) {
        if (
          currentValue === null ||
          currentValue === undefined ||
          !Object.prototype.hasOwnProperty.call(currentValue, key)
        ) {
          errors.push(`${path}.${key} is required`);
          return false;
        }
      }

      const properties = currentSchema.properties || {};
      for (const [prop, propSchema] of Object.entries(properties)) {
        if (
          currentValue &&
          Object.prototype.hasOwnProperty.call(currentValue, prop) &&
          currentValue[prop] !== undefined
        ) {
          visit(propSchema, currentValue[prop], `${path}.${prop}`);
        }
      }
    }

    return true;
  }

  visit(schema, value, '$');
  return { valid: errors.length === 0, errors };
}

function extractJsonFromText(text = '') {
  const source = String(text || '').trim();
  if (!source) return null;

  try {
    return JSON.parse(source);
  } catch (_) {
    // Continue with relaxed extraction.
  }

  const first = source.indexOf('{');
  const last = source.lastIndexOf('}');
  if (first >= 0 && last > first) {
    try {
      return JSON.parse(source.slice(first, last + 1));
    } catch (_) {
      return null;
    }
  }
  return null;
}

function detectFormat(question = '') {
  const text = normalizeText(question);
  if (/\bodi\b|\bone day\b/.test(text)) return 'ODI';
  if (/\bt20\b|\bipl\b/.test(text)) return 'T20';
  if (/\btest\b/.test(text)) return 'Test';
  return '';
}

function detectTopMetric(question = '') {
  const text = normalizeText(question);
  if (/\bwickets?\b/.test(text)) return 'wickets';
  if (/\bstrike rate\b|\bsr\b/.test(text)) return 'strike_rate';
  if (/\beconomy\b/.test(text)) return 'economy';
  return 'runs';
}

function extractSeason(question = '') {
  const match = String(question || '').match(YEAR_REGEX);
  return match ? match[1] : '';
}

function extractMatchId(question = '') {
  const match = String(question || '').match(MATCH_ID_REGEX);
  return match ? match[1] : '';
}

function fallbackRoute(question = '') {
  const raw = String(question || '').trim();
  const q = normalizeText(raw);
  const season = extractSeason(raw);
  const format = detectFormat(raw);
  const vs = parseVsSides(raw);

  if (!raw) return { action: 'not_supported' };

  if (/\b(captain|coach|history|best ever|greatest of all time|goat)\b/.test(q)) {
    return { action: 'not_supported' };
  }

  if (/\b(live|current|ongoing|today|latest|schedule|scheduled match(?:es)?|fixture|fixtures|upcoming|next match|next game|tomorrow|when is|who is playing today)\b/.test(q)) {
    return { action: 'not_supported', season, format };
  }

  if (/\b(meaning|define|definition|explain|what is)\b/.test(q)) {
    const term =
      (q.match(/\b(strike rate|economy|average|run rate|wicket)\b/) || [])[1] || '';
    if (term) {
      return { action: 'glossary', term };
    }
  }

  if (/\b(top|most|highest|best)\b/.test(q) && /\b(run|runs|wickets?|strike rate|sr|economy)\b/.test(q)) {
    return {
      action: 'top_players',
      metric: detectTopMetric(raw),
      season,
      format
    };
  }

  if (/\bhead to head\b|\bh2h\b/.test(q)) {
    return {
      action: 'head_to_head',
      team1: vs?.left || '',
      team2: vs?.right || '',
      season,
      format
    };
  }

  if (/\bcompare\b/.test(q) || /\bvs\b|\bversus\b/.test(q)) {
    return {
      action: 'compare_players',
      player1: vs?.left || '',
      player2: vs?.right || '',
      season,
      format
    };
  }

  if (/\bmatch\b|\bscorecard\b|\bsummary\b/.test(q)) {
    return {
      action: 'match_summary',
      match_id: extractMatchId(raw),
      team1: vs?.left || '',
      team2: vs?.right || '',
      season,
      format
    };
  }

  if (/\bteam\b/.test(q)) {
    return { action: 'team_stats', team: raw, season, format };
  }

  if (season) {
    return { action: 'player_season_stats', player: raw, season, format };
  }

  return { action: 'player_stats', player: raw, format };
}

function normalizeAction(action = '') {
  const raw = String(action || '').trim();
  if (!raw) return 'not_supported';
  if (raw === 'top_list') return 'top_players';
  if (raw === 'compare_teams') return 'head_to_head';
  if (raw === 'clarify' || raw === 'venue_stats') return 'not_supported';
  return SUPPORTED_ACTIONS.includes(raw) ? raw : 'not_supported';
}

function normalizeRoute(raw = {}) {
  const entities = raw?.entities && typeof raw.entities === 'object' ? raw.entities : {};
  const merged = { ...entities, ...raw };
  delete merged.entities;

  const route = {
    action: normalizeAction(merged.action),
    player: cleanEntitySegment(merged.player || ''),
    player1: cleanEntitySegment(merged.player1 || ''),
    player2: cleanEntitySegment(merged.player2 || ''),
    team: cleanEntitySegment(merged.team || ''),
    team1: cleanEntitySegment(merged.team1 || ''),
    team2: cleanEntitySegment(merged.team2 || ''),
    match_id: merged.match_id || '',
    season: merged.season || '',
    format: merged.format || '',
    date: merged.date || '',
    metric: merged.metric || merged.list_type || '',
    term: merged.term || '',
    limit: merged.limit || '',
    min_balls: merged.min_balls || '',
    min_overs: merged.min_overs || ''
  };

  return route;
}

async function callRouterModel(messages) {
  return callLlama(messages, {
    temperature: 0,
    purpose: 'router'
  });
}

function buildSystemPrompt() {
  return [
    'You are a cricket query router.',
    'Return ONLY one JSON object. No markdown. No explanation.',
    'Allowed actions:',
    'player_stats, player_season_stats, team_stats, match_summary, compare_players, head_to_head, top_players, glossary, not_supported',
    'Use these optional keys when relevant:',
    'player, player1, player2, team, team1, team2, season, format, match_id, date, metric, term, limit, min_balls, min_overs',
    'Rules:',
    '1) If a player career/cumulative stat is asked, use player_stats.',
    '2) If a player stat is constrained by year/season, use player_season_stats and include season.',
    '3) Compare two players -> compare_players with player1 and player2.',
    '4) Team-vs-team record -> head_to_head with team1 and team2.',
    '5) Rankings/top lists -> top_players with metric.',
    '6) Stat term meaning -> glossary with term.',
    '7) If not answerable from structured cricket data, use not_supported.'
  ].join('\n');
}

function buildUserPrompt(question, context = {}) {
  return JSON.stringify(
    {
      question,
      context: {
        player: context.player || context.player_name || '',
        team: context.team || context.team_name || '',
        season: context.season || '',
        format: context.format || '',
        action: context.action || ''
      }
    },
    null,
    2
  );
}

async function routeQuestion(question, context = {}) {
  const messages = [
    { role: 'system', content: buildSystemPrompt() },
    { role: 'user', content: buildUserPrompt(question, context) }
  ];

  try {
    const content = await callRouterModel(messages);
    let parsed = extractJsonFromText(content);
    let normalized = normalizeRoute(parsed || {});
    let validation = validateAgainstSchema(ROUTER_SCHEMA, normalized);

    if (!validation.valid) {
      const retryContent = await callRouterModel([
        ...messages,
        { role: 'user', content: 'Return valid JSON only.' }
      ]);
      parsed = extractJsonFromText(retryContent);
      normalized = normalizeRoute(parsed || {});
      validation = validateAgainstSchema(ROUTER_SCHEMA, normalized);
    }

    if (validation.valid) {
      return normalized;
    }
  } catch (_) {
    // Fall back to local heuristics when llama.cpp is unavailable or returns invalid JSON.
  }

  return fallbackRoute(question);
}

module.exports = {
  routeQuestion,
  validateAgainstSchema,
  extractJsonFromText
};
