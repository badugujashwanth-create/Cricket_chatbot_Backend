const SUPPORTED_ACTIONS = [
  'player_stats',
  'player_season_stats',
  'team_stats',
  'match_summary',
  'compare_players',
  'head_to_head',
  'top_players',
  'glossary',
  'not_supported'
];

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
  head_to_head:
    'Head to head means how two teams have performed against each other.'
};

const ROUTER_SCHEMA = {
  type: 'object',
  required: ['action'],
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
    entities: { type: 'object' }
  },
  additionalProperties: true
};

module.exports = {
  SUPPORTED_ACTIONS,
  NOT_AVAILABLE_MESSAGE,
  GLOSSARY,
  ROUTER_SCHEMA
};
