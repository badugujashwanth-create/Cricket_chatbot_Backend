const { ROUTER_SCHEMA, SUPPORTED_ACTIONS, DATA_SOURCES } = require('./constants');
const { callLlama } = require('./llamaClient');
const { normalizeText } = require('./textUtils');
const { cleanEntitySegment, parseVsSides } = require('./queryParser');

const YEAR_REGEX = /\b(19\d{2}|20\d{2})\b/;
const MATCH_ID_REGEX = /\b(\d{5,})\b/;

const ROUTER_DATA_SOURCES = Object.values(DATA_SOURCES);

const LIVE_DATA_SIGNAL_REGEX =
  /\b(live|current|ongoing|now|today|latest|yesterday|recent|scorecard|schedule|scheduled match(?:es)?|fixture|fixtures|upcoming|next match|next game|tomorrow|who is playing today)\b/;

const VECTOR_DATA_SIGNAL_REGEX =
  /\b(all[- ]time|career|overall|historical|history|archive|archived|record|records|venue|ground|head to head|h2h|compare|season|centur(?:y|ies)|fift(?:y|ies)|chennai|mumbai|delhi|kolkata|bangalore)\b/;

const CRICBUZZ_SIGNAL_REGEX =
  /\b(player|players|profile|career|form|batting|bowling|strike rate|economy|average|runs|wickets|bio|biography|playing style|role)\b/;

const OPENAI_FALLBACK_SIGNAL_REGEX =
  /\b(explain|why|how does|how do|opinion|predict|prediction|chance|chances|likely|should|could|would|analysis|insight|strategy|fantasy|dream11|goat|best ever|better captain|stronger team)\b/;

const KNOWLEDGE_SIGNAL_REGEX =
  /\b(lbw|powerplay|free hit|no ball|noball|wide ball|drs|umpire signals?|fielding restrictions?|yorker|googly|doosra|maiden over|hat[- ]trick|difference between odi and t20|test vs odi vs t20|format difference|world cup|wc 20\d{2}|history of cricket|cricket history|legendary players?|rivalr(?:y|ies)|famous match(?:es)?|longest six|equipment|protective gear|cricket bat|practice routine|coaching drills?|batting drills?|bowling drills?|fielding drills?)\b/;

const RULES_SIGNAL_REGEX =
  /\b(lbw|powerplay|free hit|no ball|noball|wide ball|drs|umpire signals?|fielding restrictions?)\b/;

const TERMINOLOGY_SIGNAL_REGEX =
  /\b(yorker|googly|doosra|maiden over|hat[- ]trick)\b/;

const FORMAT_COMPARISON_SIGNAL_REGEX =
  /\b(difference between .*odi.*t20|difference between .*t20.*odi|test vs odi vs t20|odi vs t20|test vs t20|format difference|overs breakdown|overs in each format)\b/;

const HISTORY_SIGNAL_REGEX =
  /\b(world cup|wc\b|history of cricket|cricket history|legendary players?|rivalr(?:y|ies)|famous match(?:es)?|longest six|youngest debutant)\b/;

const EQUIPMENT_SIGNAL_REGEX =
  /\b(equipment|protective gear|cricket bat|practice routine|coaching drills?|batting drills?|bowling drills?|fielding drills?)\b/;

const PREDICTION_SIGNAL_REGEX =
  /\b(predict|prediction|who may win|who will win|win probability|likely winner|fantasy|dream11|strategy)\b/;

const TEAM_COMPARISON_SIGNAL_REGEX =
  /\b(team comparison|which team is better|which team better|stronger team|better team|batting lineup|bowling attack)\b/;

const POINTS_TABLE_SIGNAL_REGEX = /\b(points table|standings|table standings)\b/;

const RANKING_SIGNAL_REGEX = /\b(rankings?|icc rankings?|team rankings?)\b/;

const VENUE_SIGNAL_REGEX = /\b(venue details?|stadium|home ground|ground details?)\b/;

const PITCH_SIGNAL_REGEX = /\b(pitch report|pitch condition|pitch behave|pitch behavior)\b/;

const INJURY_SIGNAL_REGEX = /\b(injury|injured|availability|available today|ruled out|fit to play)\b/;

const GENERAL_CHAT_SIGNAL_REGEX =
  /\b(hi|hello|hey|hii|heya|yo|sup|thanks|thank you|who are you|who are u|who r u|who made you|who built you|who created you|what can you do|what can u do)\b/;

const CRICKET_DOMAIN_SIGNAL_REGEX =
  /\b(cricket|match|matches|scorecard|innings|over|overs|wicket|wickets|batter|batters|batsman|batting|bowler|bowlers|bowling|run|runs|player|players|team|teams|captain|coach|squad|lineup|playing xi|playing 11|venue|stadium|pitch|toss|odi|t20|test|ipl|icc|world cup|wc|asia cup|champions trophy|series|tournament|strike rate|economy|head to head|h2h|six|sixes|four|fours|century|fifty|fantasy|dream11|lbw|powerplay|free hit|no ball|drs|yorker|googly|doosra|maiden over|hat[- ]trick|equipment|protective gear|coaching|practice drills?)\b/;

const CRICKET_ALIAS_SIGNAL_REGEX = /\b(csk|mi|rcb|kkr|dc|srh|rr|pbks|gt|lsg|bcci|icc)\b/;

const BROKEN_PLAYER_STATS_SIGNAL_REGEX =
  /\b(stat|stats|scor|score|runs?|avg|average|sr|strike|strik|econom|wicket|wkts?|form|profile|profle|bio)\b/;

const STRICT_LIVE_ROUTE_REGEX = /\b(live|current|ongoing|today|now)\b|\blive scores?\b/;

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

  const firstBrace = source.indexOf('{');
  const lastBrace = source.lastIndexOf('}');
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    try {
      return JSON.parse(source.slice(firstBrace, lastBrace + 1));
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
  if (/\bmost six(?:es)?\b|\bbig hitters?\b|\bpower hitters?\b/.test(text)) return 'sixes';
  if (/\bmost four(?:s)?\b/.test(text)) return 'fours';
  if (
    /\bfastest\s+(?:50|fifty|100|century)\b|\bquickest\s+(?:50|fifty|100|century)\b|\bmost aggressive batting\b/.test(
      text
    )
  ) {
    return 'strike_rate';
  }
  if (/\bwickets?\b/.test(text)) return 'wickets';
  if (/\bstrike rate\b|\bsr\b/.test(text)) return 'strike_rate';
  if (/\beconomy\b/.test(text)) return 'economy';
  return 'runs';
}

function isGreeting(question = '') {
  return GENERAL_CHAT_SIGNAL_REGEX.test(String(question || '').trim()) ||
    /^(hi|hello|hey|hii|heya|yo|sup|how are you|who are you|who are u|who r u|thanks|thank you)\b/i.test(
      String(question || '').trim()
    );
}

function isPlayingXiQuestion(question = '') {
  return /\b(playing\s*(?:xi|11|eleven)|who\s+playing\s+now|who\s+is\s+playing\s+now)\b/.test(
    normalizeText(question)
  );
}

function isTeamSquadQuestion(question = '') {
  const text = normalizeText(question);
  
  // Exclude player stat queries - if "player" is followed by stat-related words, it's NOT squad query
  if (/\bplayer\b.*\b(stat|average|runs|wickets|form|profile|career|record)\b/.test(text)) {
    return false;
  }
  
  return /\b(players|squad|team list|lineup|roster|bench)\b/.test(text);
}

function isTeamInfoQuestion(question = '') {
  const text = normalizeText(question);
  if (PREDICTION_SIGNAL_REGEX.test(text) || /\bfantasy|dream11\b/.test(text)) {
    return false;
  }
  return /\b(troph(?:y|ies)|captain|coach|owner|history|founded|home ground|franchise|jersey)\b/.test(
    text
  );
}

function isRecordQuestion(question = '') {
  return /\b(highest score|highest individual score|lowest total|fastest century|fastest 100|best bowling figures|most wickets|record|records|longest six)\b/.test(
    normalizeText(question)
  );
}

function isGlossaryQuestion(question = '') {
  return /\b(meaning|define|definition|explain|what is)\b/.test(normalizeText(question));
}

function isGeneralReasoningQuestion(question = '') {
  return OPENAI_FALLBACK_SIGNAL_REGEX.test(normalizeText(question));
}

function isCricketDomainQuestion(question = '') {
  const text = normalizeText(question);
  if (!text) return false;
  return (
    CRICKET_DOMAIN_SIGNAL_REGEX.test(text) ||
    CRICKET_ALIAS_SIGNAL_REGEX.test(text) ||
    /\b(vs|versus|h2h)\b/.test(text)
  );
}

function isLikelyBrokenPlayerStatsQuestion(question = '') {
  const raw = String(question || '').trim();
  const text = normalizeText(raw);
  if (!text) return false;
  if (isGreeting(raw)) return false;
  return BROKEN_PLAYER_STATS_SIGNAL_REGEX.test(text) && raw.split(/\s+/).filter(Boolean).length <= 10;
}

function isClearlyOffTopicQuestion(question = '') {
  const raw = String(question || '').trim();
  const text = normalizeText(raw);
  if (!text) return false;
  if (isCricketDomainQuestion(raw) || isLikelyBrokenPlayerStatsQuestion(raw)) {
    return false;
  }
  return (
    isGreeting(raw) ||
    /\b(joke|weather|recipe|movie|song|email|photosynthesis|capital of|translate|python|javascript|java|algebra|history)\b/.test(
      text
    ) ||
    /^(what|who|why|how|when|where|can|could|would|should|tell|explain|write|give|define)\b/.test(text)
  );
}

function shouldForceLiveUpdateRoute(question = '') {
  const text = normalizeText(question);
  if (!text || isClearlyOffTopicQuestion(question)) return false;
  if (PREDICTION_SIGNAL_REGEX.test(text) || TEAM_COMPARISON_SIGNAL_REGEX.test(text)) {
    return false;
  }
  return STRICT_LIVE_ROUTE_REGEX.test(text) && isCricketDomainQuestion(question);
}

function detectTimeContext(question = '') {
  const text = normalizeText(question);
  if (/\b(now|live|current|ongoing|today)\b/.test(text)) return 'live';
  if (/\b(yesterday|recent|last match|last game)\b/.test(text)) return 'recent';
  if (/\b(tomorrow|upcoming|next match|next game|schedule|fixture)\b/.test(text)) return 'upcoming';
  if (/\b(career|all time|overall|historical|history|record|records)\b/.test(text)) return 'historical';
  if (YEAR_REGEX.test(question)) return 'seasonal';
  return 'general';
}

function detectSubIntent(question = '', route = {}) {
  const text = normalizeText(question);
  const action = String(route.action || '').trim();

  if (action === 'live_update') {
    if (POINTS_TABLE_SIGNAL_REGEX.test(text)) return 'points_table';
    if (/\b(schedule|fixture|upcoming|next match|tomorrow)\b/.test(text)) return 'fixture_schedule';
    if (/\b(yesterday|recent result|who won)\b/.test(text)) return 'recent_result';
    if (VENUE_SIGNAL_REGEX.test(text)) return 'venue_info';
    if (PITCH_SIGNAL_REGEX.test(text)) return 'pitch_report';
    return 'live_update';
  }
  if (action === 'player_stats' || action === 'player_season_stats') return 'player_stats';
  if (action === 'team_stats') {
    if (RANKING_SIGNAL_REGEX.test(text)) return 'team_ranking';
    return 'team_stats';
  }
  if (action === 'team_squad') return 'squad_info';
  if (action === 'playing_xi') return 'playing_xi';
  if (action === 'compare_players') return 'player_comparison';
  if (action === 'head_to_head') return 'team_comparison';
  if (action === 'record_lookup') return 'record_lookup';
  if (action === 'subjective_analysis') {
    if (/\bfantasy|dream11\b/.test(text)) return 'fantasy_tip';
    if (/\bpredict|prediction|who may win|who will win|win probability\b/.test(text)) {
      return 'match_prediction';
    }
    if (TEAM_COMPARISON_SIGNAL_REGEX.test(text)) return 'team_comparison';
    return 'strategy_analysis';
  }
  if (action === 'team_info') {
    if (VENUE_SIGNAL_REGEX.test(text)) return 'venue_info';
    if (INJURY_SIGNAL_REGEX.test(text)) return 'injury_update';
    return 'team_stats';
  }
  if (action === 'glossary') return 'terminology_explainer';
  if (action === 'general_knowledge') {
    if (RULES_SIGNAL_REGEX.test(text)) return 'rules_explainer';
    if (FORMAT_COMPARISON_SIGNAL_REGEX.test(text)) return 'format_comparison';
    if (TERMINOLOGY_SIGNAL_REGEX.test(text)) return 'terminology_explainer';
    if (EQUIPMENT_SIGNAL_REGEX.test(text)) {
      if (/\bdrill|practice|routine|coaching\b/.test(text)) return 'coaching_drill';
      return 'equipment_info';
    }
    if (HISTORY_SIGNAL_REGEX.test(text)) {
      if (/\bworld cup|wc\b/.test(text)) return 'worldcup_history';
      return 'history_trivia';
    }
    if (RANKING_SIGNAL_REGEX.test(text)) return 'team_ranking';
    if (PITCH_SIGNAL_REGEX.test(text)) return 'pitch_report';
    if (INJURY_SIGNAL_REGEX.test(text)) return 'injury_update';
    return 'general_cricket_fallback';
  }
  if (action === 'chit_chat') return 'general_cricket_fallback';
  return action || 'general_cricket_fallback';
}

function detectAnswerMode(question = '', route = {}) {
  const action = String(route.action || '').trim();
  if (action === 'live_update') return 'live';
  if (action === 'subjective_analysis') return 'analysis';
  if (action === 'general_knowledge' || action === 'glossary') return 'knowledge';
  if (String(route.sub_intent || '').trim() === 'match_prediction') return 'analysis';
  return 'fact';
}

function estimateRouteConfidence(question = '', route = {}) {
  const text = normalizeText(question);
  let score = 0.62;

  if (route.action && route.action !== 'not_supported') score += 0.08;
  if (route.player || route.player1 || route.player2 || route.team || route.team1 || route.team2) {
    score += 0.08;
  }
  if (RULES_SIGNAL_REGEX.test(text) || TERMINOLOGY_SIGNAL_REGEX.test(text)) score += 0.12;
  if (LIVE_DATA_SIGNAL_REGEX.test(text) || PREDICTION_SIGNAL_REGEX.test(text)) score += 0.1;
  if (isLikelyBrokenPlayerStatsQuestion(question)) score -= 0.05;
  if (isClearlyOffTopicQuestion(question)) score = 0.95;

  return Number(Math.max(0.45, Math.min(0.99, score)).toFixed(2));
}

function withRouteMetadata(route = {}, question = '') {
  const withIntent = {
    ...route,
    intent: String(route.intent || route.action || 'not_supported').trim() || 'not_supported'
  };
  const subIntent = String(withIntent.sub_intent || detectSubIntent(question, withIntent)).trim();
  return {
    ...withIntent,
    sub_intent: subIntent,
    time_context: String(withIntent.time_context || detectTimeContext(question)).trim(),
    answer_mode: String(withIntent.answer_mode || detectAnswerMode(question, { ...withIntent, sub_intent: subIntent })).trim(),
    confidence: Number.isFinite(Number(withIntent.confidence))
      ? Number(withIntent.confidence)
      : estimateRouteConfidence(question, { ...withIntent, sub_intent: subIntent })
  };
}

function applyPriorityRouteRules(question = '', route = {}) {
  if (shouldForceLiveUpdateRoute(question)) {
    return {
      ...route,
      action: 'live_update',
      data_sources: [DATA_SOURCES.CRICAPI_LIVE]
    };
  }
  if (isClearlyOffTopicQuestion(question)) {
    return {
      ...route,
      action: 'chit_chat',
      data_sources: [DATA_SOURCES.OPENAI_FALLBACK]
    };
  }
  return route;
}

function extractSeason(question = '') {
  const match = String(question || '').match(YEAR_REGEX);
  return match ? match[1] : '';
}

function extractMatchId(question = '') {
  const match = String(question || '').match(MATCH_ID_REGEX);
  return match ? match[1] : '';
}

function normalizeDataSource(value = '') {
  const normalized = normalizeText(value)
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

  if (
    ['cricapi_live', 'live_api', 'live', 'api', 'cricapi', 'cric_api', 'live_scores'].includes(
      normalized
    )
  ) {
    return DATA_SOURCES.CRICAPI_LIVE;
  }

  if (
    [
      'cricbuzz_stats',
      'cricbuzz',
      'player_stats',
      'player_profile',
      'deep_player_stats',
      'player_data'
    ].includes(normalized)
  ) {
    return DATA_SOURCES.CRICBUZZ_STATS;
  }

  if (
    [
      'vector_db',
      'vector',
      'vectordb',
      'chroma',
      'chroma_db',
      'chromadb',
      'archive',
      'archived',
      'historical'
    ].includes(normalized)
  ) {
    return DATA_SOURCES.VECTOR_DB;
  }

  if (
    [
      'local_knowledge',
      'knowledge',
      'knowledge_base',
      'local_data',
      'rules_data',
      'history_data'
    ].includes(normalized)
  ) {
    return DATA_SOURCES.LOCAL_KNOWLEDGE;
  }

  if (
    [
      'openai_fallback',
      'openai',
      'fallback',
      'general_llm',
      'general_reasoning',
      'synthesis_only'
    ].includes(normalized)
  ) {
    return DATA_SOURCES.OPENAI_FALLBACK;
  }

  return '';
}

function normalizeDataSources(values = []) {
  const list = Array.isArray(values)
    ? values
    : typeof values === 'string'
      ? values
          .split(',')
          .map((value) => value.trim())
          .filter(Boolean)
      : [];

  return [...new Set(list.map((value) => normalizeDataSource(value)).filter(Boolean))];
}

function inferDataSources(question = '', route = {}) {
  const text = normalizeText(question);
  const rawQuestion = String(question || '').trim();
  if (!text) {
    return [DATA_SOURCES.OPENAI_FALLBACK];
  }

  if (isClearlyOffTopicQuestion(rawQuestion) && !isCricketDomainQuestion(rawQuestion)) {
    return [DATA_SOURCES.OPENAI_FALLBACK];
  }

  const sources = [];
  const hasPlayerContext =
    Boolean(route.player || route.player1 || route.player2) ||
    /\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b/.test(rawQuestion);

  const wantsLive =
    LIVE_DATA_SIGNAL_REGEX.test(text) ||
    (route.action === 'match_summary' &&
      /\b(today|live|current|ongoing|latest|recent|yesterday|tomorrow)\b/.test(text));

  const wantsCricbuzz =
    (CRICBUZZ_SIGNAL_REGEX.test(text) && hasPlayerContext) ||
    ['player_stats', 'player_season_stats', 'compare_players'].includes(route.action);

  const wantsKnowledge =
    KNOWLEDGE_SIGNAL_REGEX.test(text) ||
    ['general_knowledge', 'glossary'].includes(route.action) ||
    ['rules_explainer', 'format_comparison', 'terminology_explainer', 'history_trivia', 'worldcup_history', 'equipment_info', 'coaching_drill', 'general_cricket_fallback'].includes(
      String(route.sub_intent || '').trim()
    );

  const wantsVector =
    VECTOR_DATA_SIGNAL_REGEX.test(text) ||
    [
      'player_stats',
      'player_season_stats',
      'team_stats',
      'team_squad',
      'playing_xi',
      'team_info',
      'match_summary',
      'compare_players',
      'head_to_head',
      'top_players',
      'record_lookup'
    ].includes(route.action);

  const wantsOpenAiFallback =
    isGreeting(question) ||
    isClearlyOffTopicQuestion(question) ||
    isGeneralReasoningQuestion(question) ||
    ['chit_chat', 'subjective_analysis', 'not_supported'].includes(route.action);

  if (wantsLive) {
    sources.push(DATA_SOURCES.CRICAPI_LIVE);
  }

  if (wantsCricbuzz) {
    sources.push(DATA_SOURCES.CRICBUZZ_STATS);
  }

  if (wantsKnowledge) {
    sources.push(DATA_SOURCES.LOCAL_KNOWLEDGE);
  }

  if (wantsVector) {
    sources.push(DATA_SOURCES.VECTOR_DB);
  }

  if (wantsOpenAiFallback && String(route.action || '').trim() === 'subjective_analysis') {
    sources.push(DATA_SOURCES.OPENAI_FALLBACK);
  }

  if (!sources.length || (wantsOpenAiFallback && !wantsLive && !wantsCricbuzz && !wantsVector && !wantsKnowledge)) {
    sources.push(DATA_SOURCES.OPENAI_FALLBACK);
  }

  const normalized = [...new Set(sources)].filter((source) => ROUTER_DATA_SOURCES.includes(source));

  return normalized;
}

function withDataSources(route = {}, question = '') {
  return withRouteMetadata({
    ...route,
    data_sources: inferDataSources(question, route)
  }, question);
}

function fallbackRoute(question = '') {
  const raw = String(question || '').trim();
  const q = normalizeText(raw);
  const season = extractSeason(raw);
  const format = detectFormat(raw);
  const vs = parseVsSides(raw);
  const buildRoute = (route = {}) => withDataSources(route, raw);

  if (!raw) {
    return buildRoute({ action: 'not_supported' });
  }

  if (isGreeting(raw)) {
    return buildRoute({ action: 'chit_chat' });
  }

  if (isClearlyOffTopicQuestion(raw)) {
    return buildRoute({ action: 'chit_chat' });
  }

  if (shouldForceLiveUpdateRoute(raw)) {
    return buildRoute({ action: 'live_update', season, format });
  }

  if (POINTS_TABLE_SIGNAL_REGEX.test(q)) {
    return buildRoute({
      action: 'general_knowledge',
      sub_intent: 'points_table',
      season,
      format
    });
  }

  if (RANKING_SIGNAL_REGEX.test(q)) {
    return buildRoute({
      action: 'general_knowledge',
      sub_intent: 'team_ranking',
      season,
      format
    });
  }

  if (PITCH_SIGNAL_REGEX.test(q)) {
    return buildRoute({
      action: 'general_knowledge',
      sub_intent: 'pitch_report',
      season,
      format
    });
  }

  if (INJURY_SIGNAL_REGEX.test(q)) {
    return buildRoute({
      action: 'general_knowledge',
      sub_intent: 'injury_update',
      season,
      format
    });
  }

  if (isRecordQuestion(raw)) {
    return buildRoute({
      action: 'record_lookup',
      metric: detectTopMetric(raw),
      season,
      format
    });
  }

  if (isTeamSquadQuestion(raw)) {
    return buildRoute({
      action: isPlayingXiQuestion(raw) ? 'playing_xi' : 'team_squad',
      team: raw,
      season,
      format
    });
  }

  if (isTeamInfoQuestion(raw)) {
    return buildRoute({
      action: 'team_info',
      team: raw,
      team1: vs?.left || '',
      team2: vs?.right || '',
      season,
      format
    });
  }

  if (FORMAT_COMPARISON_SIGNAL_REGEX.test(q)) {
    return buildRoute({
      action: 'general_knowledge',
      sub_intent: 'format_comparison',
      term: raw,
      season,
      format
    });
  }

  if (RULES_SIGNAL_REGEX.test(q) || TERMINOLOGY_SIGNAL_REGEX.test(q)) {
    return buildRoute({
      action: 'general_knowledge',
      sub_intent: RULES_SIGNAL_REGEX.test(q) ? 'rules_explainer' : 'terminology_explainer',
      term: raw,
      season,
      format
    });
  }

  if (HISTORY_SIGNAL_REGEX.test(q) || EQUIPMENT_SIGNAL_REGEX.test(q)) {
    return buildRoute({
      action: 'general_knowledge',
      sub_intent: EQUIPMENT_SIGNAL_REGEX.test(q)
        ? /\bdrill|practice|routine|coaching\b/.test(q)
          ? 'coaching_drill'
          : 'equipment_info'
        : /\bworld cup|wc\b/.test(q)
          ? 'worldcup_history'
          : 'history_trivia',
      term: raw,
      season,
      format
    });
  }

  if (isGlossaryQuestion(raw)) {
    const term =
      (q.match(/\b(strike rate|economy|average|run rate|wicket|powerplay|dls|duckworth lewis)\b/) || [])[1] ||
      '';
    if (RULES_SIGNAL_REGEX.test(q) || TERMINOLOGY_SIGNAL_REGEX.test(q) || FORMAT_COMPARISON_SIGNAL_REGEX.test(q)) {
      return buildRoute({
        action: 'general_knowledge',
        sub_intent: RULES_SIGNAL_REGEX.test(q)
          ? 'rules_explainer'
          : FORMAT_COMPARISON_SIGNAL_REGEX.test(q)
            ? 'format_comparison'
            : 'terminology_explainer',
        term: term || raw,
        season,
        format
      });
    }
    return buildRoute({
      action: term ? 'glossary' : 'subjective_analysis',
      term,
      season,
      format
    });
  }

  if (
    (/\b(top|most|highest|best|fastest|quickest)\b/.test(q) &&
      /\b(run|runs|wickets?|strike rate|sr|economy|six(?:es)?|four(?:s)?|century|fifty|50|100)\b/.test(q)) ||
    /\bmost aggressive batting\b/.test(q)
  ) {
    return buildRoute({
      action: 'top_players',
      metric: detectTopMetric(raw),
      season,
      format
    });
  }

  if (/\bhead to head\b|\bh2h\b/.test(q)) {
    return buildRoute({
      action: 'head_to_head',
      team1: vs?.left || '',
      team2: vs?.right || '',
      season,
      format
    });
  }

  if (/\bcompare\b/.test(q) || /\bvs\b|\bversus\b/.test(q)) {
    if (/\b(team|lineup|attack|captaincy|captain|overall)\b/.test(q)) {
      return buildRoute({
        action: 'subjective_analysis',
        sub_intent: 'team_comparison',
        team1: vs?.left || '',
        team2: vs?.right || '',
        season,
        format
      });
    }

    return buildRoute({
      action: 'compare_players',
      sub_intent: 'player_comparison',
      player1: vs?.left || '',
      player2: vs?.right || '',
      season,
      format
    });
  }

  if (
    PREDICTION_SIGNAL_REGEX.test(q) ||
    TEAM_COMPARISON_SIGNAL_REGEX.test(q) ||
    (vs?.left && vs?.right && /\bbetter|stronger|who is better|who better\b/.test(q))
  ) {
    const comparisonLike = Boolean(vs?.left && vs?.right);
    return buildRoute({
      action: comparisonLike && !TEAM_COMPARISON_SIGNAL_REGEX.test(q) ? 'compare_players' : 'subjective_analysis',
      sub_intent: /\bfantasy|dream11\b/.test(q)
        ? 'fantasy_tip'
        : /\bpredict|prediction|who may win|who will win|win probability\b/.test(q)
          ? 'match_prediction'
          : comparisonLike
            ? TEAM_COMPARISON_SIGNAL_REGEX.test(q)
              ? 'team_comparison'
              : 'player_comparison'
            : 'strategy_analysis',
      player1: comparisonLike ? vs?.left || '' : '',
      player2: comparisonLike && !TEAM_COMPARISON_SIGNAL_REGEX.test(q) ? vs?.right || '' : '',
      team1: TEAM_COMPARISON_SIGNAL_REGEX.test(q) ? vs?.left || '' : '',
      team2: TEAM_COMPARISON_SIGNAL_REGEX.test(q) ? vs?.right || '' : '',
      season,
      format
    });
  }

  if (
    /\b(live|current|ongoing|today|latest|schedule|scheduled match(?:es)?|fixture|fixtures|upcoming|next match|next game|tomorrow|when is|who is playing today)\b/.test(
      q
    )
  ) {
    return buildRoute({
      action: 'live_update',
      season,
      format
    });
  }

  if (/\bteam summary\b|\bteam stats?\b/.test(q)) {
    return buildRoute({
      action: 'team_stats',
      team: raw,
      season,
      format
    });
  }

  if (/\bmatch\b|\bscorecard\b|\bsummary\b|\blive\b|\bscore\b|\bschedule\b|\bfixture\b/.test(q)) {
    return buildRoute({
      action: 'match_summary',
      match_id: extractMatchId(raw),
      team1: vs?.left || '',
      team2: vs?.right || '',
      season,
      format
    });
  }

  if (/\bteam\b/.test(q)) {
    return buildRoute({ action: 'team_stats', team: raw, season, format });
  }

  if (isGeneralReasoningQuestion(raw)) {
    return buildRoute({
      action: 'subjective_analysis',
      sub_intent: /\bfantasy|dream11\b/.test(q)
        ? 'fantasy_tip'
        : /\bpredict|prediction|who may win|who will win|win probability\b/.test(q)
          ? 'match_prediction'
          : 'strategy_analysis',
      player1: vs?.left || '',
      player2: vs?.right || '',
      team1: vs?.left || '',
      team2: vs?.right || '',
      season,
      format
    });
  }

  if (isLikelyBrokenPlayerStatsQuestion(raw)) {
    return buildRoute({ action: 'player_stats', player: raw, format });
  }

  if (season) {
    return buildRoute({ action: 'player_season_stats', player: raw, season, format });
  }

  return buildRoute({ action: 'player_stats', player: raw, format });
}

function normalizeAction(action = '') {
  const raw = String(action || '').trim();
  if (!raw) return 'not_supported';
  if (
    [
      'rules_explainer',
      'format_comparison',
      'terminology_explainer',
      'history_trivia',
      'worldcup_history',
      'coaching_drill',
      'equipment_info',
      'general_cricket_fallback',
      'points_table',
      'team_ranking',
      'pitch_report',
      'injury_update'
    ].includes(raw)
  ) {
    return 'general_knowledge';
  }
  if (['fixture_schedule', 'recent_result', 'venue_info'].includes(raw)) {
    return 'live_update';
  }
  if (raw === 'player_comparison') return 'compare_players';
  if (raw === 'team_comparison') return 'subjective_analysis';
  if (['match_prediction', 'fantasy_tip', 'strategy_analysis'].includes(raw)) {
    return 'subjective_analysis';
  }
  if (raw === 'squad_info') return 'team_squad';
  if (raw === 'player_record') return 'record_lookup';
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
    min_overs: merged.min_overs || '',
    intent: String(merged.intent || merged.action || '').trim(),
    sub_intent: String(
      merged.sub_intent ||
        merged.subIntent ||
        merged.intent_name ||
        merged.intentName ||
        ''
    ).trim(),
    time_context: String(merged.time_context || merged.timeContext || '').trim(),
    answer_mode: String(merged.answer_mode || merged.answerMode || '').trim(),
    confidence: Number.isFinite(Number(merged.confidence)) ? Number(merged.confidence) : null,
    data_sources: normalizeDataSources(
      merged.data_sources ||
        merged.dataSources ||
        merged.required_data_sources ||
        merged.requiredDataSources ||
        []
    )
  };

  return withRouteMetadata(route, String(merged.question || merged.query || '').trim());
}

async function callRouterModel(messages) {
  return callLlama(messages, {
    temperature: 0,
    purpose: 'router'
  });
}

function buildSystemPrompt() {
  return [
    'You are the Omni-Router for a cricket intelligence system.',
    'Return ONLY one JSON object. No markdown. No explanation.',
    'The JSON object must always include: action and data_sources.',
    'Users will submit queries with typos, bad grammar, or slang. Infer the cricket intent mathematically. Do not fail due to bad English.',
    'Allowed actions:',
    'player_stats, player_season_stats, team_stats, team_squad, playing_xi, live_update, team_info, match_summary, compare_players, head_to_head, top_players, record_lookup, glossary, chit_chat, general_knowledge, subjective_analysis, not_supported',
    'Allowed data_sources values: CRICAPI_LIVE, CRICBUZZ_STATS, VECTOR_DB, LOCAL_KNOWLEDGE, OPENAI_FALLBACK.',
    'Use these optional keys when relevant:',
    'player, player1, player2, team, team1, team2, season, format, match_id, date, metric, term, limit, min_balls, min_overs, sub_intent, time_context, answer_mode, confidence',
    'Rules:',
    '1) data_sources must be an array of one or more values.',
    '2) Use CRICAPI_LIVE for live/current/today/yesterday/recent match, score, schedule, fixture, or upcoming-match needs.',
    '3) Use CRICBUZZ_STATS for detailed player profiles, player form, batting metrics, bowling metrics, and granular player statistics.',
    '4) Use VECTOR_DB for historical narratives, venue records, archived context, comparisons, record lookups, and long-form cricket history.',
    '5) Use LOCAL_KNOWLEDGE for cricket rules, terminology, format explanations, world cup winners, cricket history, equipment, and coaching-drill style questions.',
    '6) Use OPENAI_FALLBACK when the query is primarily conversational, analytical, opinion-based, predictive, fantasy-oriented, or otherwise needs synthesis instead of a deterministic cricket fact.',
    '7) Compound questions must include ALL relevant sources in data_sources.',
    '8) Choose the single best action for the overall answer shape, even when multiple sources are required.',
    '9) If the query is a general cricket question that does not clearly require live APIs, Cricbuzz stats, vector history, or local knowledge, use ["OPENAI_FALLBACK"].',
    '10) If the user asks for live scores, current matches, or upcoming schedules, use live_update.',
    '11) Rules, terminology, world cup winners, cricket history, and coaching/equipment questions should use general_knowledge with LOCAL_KNOWLEDGE.',
    '12) Compare two players -> compare_players with player1 and player2.',
    '13) Team-vs-team record -> head_to_head with team1 and team2.',
    '14) Rankings or leaderboards -> top_players with metric unless the user asks about points tables or ICC rankings generally, then prefer general_knowledge.',
    '15) Team squad or lineup -> team_squad, unless it explicitly asks for the playing XI, then use playing_xi.',
    '16) Team trophy, coach, captain, or franchise history -> team_info.',
    '17) Record questions like highest score or fastest century -> record_lookup.',
    '18) Basic stat term meaning like strike rate or economy -> glossary with term.',
    '19) Greetings, assistant-identity questions, and clearly non-cricket or off-topic queries -> chit_chat with ["OPENAI_FALLBACK"].',
    '20) Use subjective_analysis for predictions, fantasy suggestions, strategy talk, or comparison questions phrased as opinions.',
    'Example output for a compound query:',
    '{"action":"player_stats","player":"Virat Kohli","sub_intent":"player_stats","time_context":"live","answer_mode":"fact","confidence":0.93,"data_sources":["CRICAPI_LIVE","CRICBUZZ_STATS","VECTOR_DB"]}',
    'Example output for a rules query:',
    '{"action":"general_knowledge","sub_intent":"rules_explainer","term":"LBW","time_context":"general","answer_mode":"knowledge","confidence":0.96,"data_sources":["LOCAL_KNOWLEDGE"]}',
    'Example output for a general cricket question:',
    '{"action":"subjective_analysis","data_sources":["OPENAI_FALLBACK"]}',
    'Example output for a greeting or off-topic question:',
    '{"action":"chit_chat","data_sources":["OPENAI_FALLBACK"]}'
  ].join('\n');
}

function buildUserPrompt(question, context = {}) {
  const heuristicHint = fallbackRoute(question);

  return JSON.stringify(
    {
      question,
      context: {
        player: context.player || context.player_name || '',
        team: context.team || context.team_name || '',
        season: context.season || '',
        format: context.format || '',
        action: context.action || ''
      },
      heuristic_hint: {
        action: heuristicHint.action,
        sub_intent: heuristicHint.sub_intent || '',
        data_sources: heuristicHint.data_sources
      }
    },
    null,
    2
  );
}

async function routeQuestion(question, context = {}) {
  const heuristicRoute = fallbackRoute(question);

  const messages = [
    { role: 'system', content: buildSystemPrompt() },
    { role: 'user', content: buildUserPrompt(question, context) }
  ];

  try {
    const content = await callRouterModel(messages);
    let parsed = extractJsonFromText(content);
    let normalized = withRouteMetadata(
      applyPriorityRouteRules(question, normalizeRoute(parsed || {})),
      question
    );
    normalized.data_sources = normalized.data_sources.length
      ? normalized.data_sources
      : inferDataSources(question, normalized);
    let validation = validateAgainstSchema(ROUTER_SCHEMA, normalized);

    if (!validation.valid) {
      const retryContent = await callRouterModel([
        ...messages,
        {
          role: 'user',
          content:
            'Return valid JSON only. Include action and a non-empty data_sources array using only CRICAPI_LIVE, CRICBUZZ_STATS, VECTOR_DB, LOCAL_KNOWLEDGE, OPENAI_FALLBACK.'
        }
      ]);
      parsed = extractJsonFromText(retryContent);
      normalized = withRouteMetadata(
        applyPriorityRouteRules(question, normalizeRoute(parsed || {})),
        question
      );
      normalized.data_sources = normalized.data_sources.length
        ? normalized.data_sources
        : inferDataSources(question, normalized);
      validation = validateAgainstSchema(ROUTER_SCHEMA, normalized);
    }

    if (validation.valid && normalized.data_sources.length) {
      return normalized;
    }
  } catch (_) {
    // Fall back to local heuristics if the local router model is unavailable or returns invalid JSON.
  }

  return heuristicRoute;
}

module.exports = {
  routeQuestion,
  validateAgainstSchema,
  extractJsonFromText,
  normalizeDataSources,
  inferDataSources
};
