const SUPPORTED_ACTIONS = [
  'player_stats',
  'player_season_stats',
  'team_stats',
  'team_squad',
  'playing_xi',
  'live_update',
  'match_summary',
  'compare_players',
  'head_to_head',
  'team_info',
  'record_lookup',
  'top_players',
  'glossary',
  'chit_chat',
  'general_knowledge',
  'subjective_analysis',
  'not_supported'
];

const DATA_SOURCES = Object.freeze({
  CRICAPI_LIVE: 'CRICAPI_LIVE',
  CRICBUZZ_STATS: 'CRICBUZZ_STATS',
  VECTOR_DB: 'VECTOR_DB',
  LOCAL_KNOWLEDGE: 'LOCAL_KNOWLEDGE',
  OPENAI_FALLBACK: 'OPENAI_FALLBACK'
});

const NOT_AVAILABLE_MESSAGE = 'I can help with cricket questions only.';

const GLOSSARY = {
  strike_rate:
    'Strike rate tells how fast a batter scores. It is runs scored per 100 balls.',
  economy:
    'Economy rate tells how many runs a bowler gives per over.',
  average:
    'Batting average is total runs divided by how many times the batter got out.',
  run_rate:
    'Run rate is how many runs a team scores per over.',
  wicket:
    'A wicket means a batter is out.',
  lbw:
    'LBW means Leg Before Wicket. A batter can be given out if the ball would have hit the stumps but was blocked by the body or pad.',
  powerplay:
    'Powerplay is the fielding-restriction phase in limited-overs cricket where fewer fielders are allowed outside the inner circle.',
  free_hit:
    'A free hit is the next ball after a front-foot no-ball in limited-overs cricket. The batter cannot be out in most normal ways from that delivery.',
  drs:
    'DRS is the Decision Review System. Teams can challenge an umpire decision using ball tracking, edge detection, and other broadcast tools.',
  head_to_head:
    'Head to head means how two teams have performed against each other.'
};

const ROUTER_SCHEMA = {
  type: 'object',
  required: ['action', 'data_sources'],
  properties: {
    action: { enum: SUPPORTED_ACTIONS },
    player: { type: 'string' },
    player1: { type: 'string' },
    player2: { type: 'string' },
    team: { type: 'string' },
    team1: { type: 'string' },
    team2: { type: 'string' },
    match_id: { anyOf: [{ type: 'string' }, { type: 'number' }] },
    season: { anyOf: [{ type: 'string' }, { type: 'number' }] },
    format: { type: 'string' },
    date: { type: 'string' },
    metric: { type: 'string' },
    term: { type: 'string' },
    limit: { anyOf: [{ type: 'number' }, { type: 'string' }] },
    min_balls: { anyOf: [{ type: 'number' }, { type: 'string' }] },
    min_overs: { anyOf: [{ type: 'number' }, { type: 'string' }] },
    intent: { type: 'string' },
    sub_intent: { type: 'string' },
    time_context: { type: 'string' },
    answer_mode: { type: 'string' },
    confidence: { anyOf: [{ type: 'number' }, { type: 'string' }] },
    data_sources: { type: 'array' },
    entities: { type: 'object' }
  },
  additionalProperties: true
};

module.exports = {
  SUPPORTED_ACTIONS,
  DATA_SOURCES,
  NOT_AVAILABLE_MESSAGE,
  GLOSSARY,
  ROUTER_SCHEMA
};
