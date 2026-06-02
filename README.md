# Cricket Chat Bot Backend

Express backend for the Cricket Chat Bot. It serves the frontend build, exposes archive and live cricket APIs, routes natural-language questions, combines local cricket data with external providers, maintains a SQLite and Chroma-backed archive, and pushes live match alerts through Socket.IO.

## Overview

The backend is the main intelligence layer of the app. It accepts cricket questions such as player stats, team summaries, live scores, records, glossary queries, comparisons, and subjective analysis. It then routes each question to the best available source:

- local SQLite archive
- local Chroma semantic index
- static cricket knowledge JSON files
- CricAPI live cricket data
- Cricbuzz player enrichment through RapidAPI
- ESPN fallback profile lookups
- local OpenAI-compatible LLM endpoint
- optional OpenAI fallback endpoint

The final result is normalized into one frontend-friendly response shape.

## Main Capabilities

- Serves `../frontend/dist` when the frontend has been built
- Provides health, metadata, player, team, match, and query endpoints
- Supports `POST /api/query` for natural-language cricket questions
- Supports `GET /api/query/stream` for Server-Sent Events query progress
- Uses Socket.IO to emit live score and archive-sync alerts
- Maintains a local semantic cache for repeatable query answers
- Reads local cricket knowledge from `backend/data`
- Reads and updates generated SQLite and Chroma artifacts
- Ingests completed live matches into SQLite, then refreshes affected Chroma documents
- Degrades gracefully when optional providers are missing or rate-limited

## Tech Stack

- Node.js + Express
- Socket.IO
- better-sqlite3
- ChromaDB
- Python scripts for archive rebuilds and Chroma maintenance
- CricAPI for live scores, schedules, series, and player directory data
- Cricbuzz RapidAPI for player micro-card enrichment
- Optional local `llama.cpp` or other OpenAI-compatible LLM server
- Optional OpenAI chat completions fallback

## Project Structure

```text
backend/
  server.js                         Express app, API routes, static hosting, Socket.IO
  queryService.js                   Main natural-language query orchestration
  llamaRouter.js                    Query intent routing and action selection
  llamaClient.js                    Local LLM and OpenAI-compatible client helpers
  cricApiService.js                 CricAPI and Cricbuzz provider integration
  espnService.js                    ESPN player profile fallback integration
  chromaService.js                  Chroma queries, semantic cache, document upserts
  vectorIndexService.js             Player, team, and match archive lookup helpers
  sqlStatsService.js                SQLite archive and runtime stats service
  knowledgeService.js               Static knowledge lookup from JSON files
  playerProfileService.js           Player profile metadata and Wikipedia enrichment
  playerMaster.js                   Player master data helper
  queryParser.js                    Entity parsing and route helper logic
  sessionStore.js                   In-memory query session context
  constants.js                      Shared action and data-source constants
  loadEnv.js                        Lightweight .env loader
  schema.sql                        SQLite schema reference
  data/                             Local cricket rules, records, history, terms
  scripts/                          Rebuild, Chroma, scraper, and test scripts
  workers/dailyIngestor.js          Live completed-match ingestor and alert worker
```

## Prerequisites

- Node.js 18 or newer
- npm
- Python 3.x for archive rebuild and Chroma helper scripts
- Local generated archive artifacts, or permission to rebuild them
- CricAPI key for live score, schedule, series, and player endpoints
- Cricbuzz RapidAPI key for player micro-cards
- Optional local LLM server for routed reasoning and synthesis
- Optional OpenAI API key for fallback reasoning

## Installation

```bash
cd backend
npm install
```

## Environment Setup

Copy the example file:

```powershell
cd backend
Copy-Item .env.example .env
```

Then fill in provider keys as needed.

## Environment Variables

```env
LLM_ENDPOINT=http://localhost:8080/v1/chat/completions
LLM_MODEL=local
LLM_TIMEOUT_MS=30000

OPENAI_API_KEY=
OPENAI_MODEL=gpt-4o
OPENAI_ENDPOINT=https://api.openai.com/v1/chat/completions

CRICAPI_KEY=
CRICAPI_TIMEOUT_MS=10000

CRICBUZZ_ENABLED=true
CRICBUZZ_RAPIDAPI_KEY=
CRICBUZZ_RAPIDAPI_HOST=cricbuzz-cricket.p.rapidapi.com
CRICBUZZ_BASE_URL=https://cricbuzz-cricket.p.rapidapi.com/
CRICBUZZ_TIMEOUT_MS=12000
CRICBUZZ_SEARCH_PATH=stats/v1/player/search
CRICBUZZ_PLAYER_STATS_PATH=stats/v1/player/{playerId}
CRICBUZZ_PLAYER_BIO_PATH=stats/v1/player/{playerId}/bio

CHROMA_MODE=auto
CHROMA_DB_DIR=
CHROMA_COLLECTION=cricket_semantic_index
CHROMA_PYTHON_BIN=python
CHROMA_HELPER_TIMEOUT_MS=30000
CHROMA_DEBUG=false

SEMANTIC_CACHE_COLLECTION=semantic_cache
SEMANTIC_CACHE_DISTANCE_THRESHOLD=0.05
SEMANTIC_CACHE_MIN_QUESTION_SIMILARITY=0.92
SEMANTIC_CACHE_MIN_TOKEN_OVERLAP=0.75

SQLITE_DB_PATH=

ENABLE_DAILY_INGESTOR=true
INGEST_INTERVAL_MS=3600000
INGEST_LOOKBACK_HOURS=24
INGEST_MATCH_LIMIT=10
RUN_DAILY_INGESTOR_ON_BOOT=false

ESPN_TIMEOUT_MS=12000
ESPN_CACHE_TTL_MS=3600000

NODE_ENV=production
PORT=3001
```

### Variable Notes

| Variable | Purpose |
| --- | --- |
| `PORT` | HTTP port for Express and Socket.IO. The code defaults to `3000`; `.env.example` uses `3001` for frontend dev compatibility. |
| `LLM_ENDPOINT` | Full OpenAI-compatible chat completions endpoint for a local model server. |
| `LLM_MODEL` | Model name sent to the local endpoint. |
| `LLM_TIMEOUT_MS` | Timeout for local LLM calls. |
| `OPENAI_API_KEY` | Optional cloud fallback key. Leave empty to avoid OpenAI calls. |
| `OPENAI_MODEL` | Model used by the optional OpenAI fallback. |
| `OPENAI_ENDPOINT` | Optional override for OpenAI-compatible chat completions. |
| `CRICAPI_KEY` | Required for CricAPI live endpoints. |
| `CRICAPI_TIMEOUT_MS` | Timeout for CricAPI requests. |
| `CRICBUZZ_ENABLED` | Set to `false` to disable Cricbuzz enrichment. |
| `CRICBUZZ_RAPIDAPI_KEY` | Required for Cricbuzz player micro-cards. |
| `CHROMA_MODE` | `auto`, local helper mode, or remote Chroma mode depending on config. |
| `CHROMA_DB_DIR` | Explicit path to local Chroma DB directory. Empty means auto-detect. |
| `CHROMA_COLLECTION` | Main semantic archive collection name. |
| `CHROMA_PYTHON_BIN` | Python executable used by Chroma helper scripts. |
| `SQLITE_DB_PATH` | Optional explicit path to the SQLite archive database. |
| `ENABLE_DAILY_INGESTOR` | Enables or disables background live match ingestion. |
| `RUN_DAILY_INGESTOR_ON_BOOT` | Runs one ingest attempt when the server starts. |
| `INGEST_INTERVAL_MS` | Interval for repeated live ingest checks. |
| `INGEST_LOOKBACK_HOURS` | How far back to look for completed matches. |
| `INGEST_MATCH_LIMIT` | Maximum completed matches to ingest per run. |

## Running Locally

### 1. Optional: start a local LLM server

If you use `llama.cpp`, start an OpenAI-compatible server:

```bash
llama-server -m <MODEL>.gguf --port 8080
```

The backend expects:

```text
http://localhost:8080/v1/chat/completions
```

The app can still answer many deterministic archive and knowledge questions without a configured LLM, but routed reasoning quality improves when one is available.

### 2. Start the backend

```bash
cd backend
npm start
```

With the provided `.env.example`, the backend runs at:

```text
http://localhost:3001
```

If no `.env` sets `PORT`, the code defaults to:

```text
http://localhost:3000
```

### 3. Open the app

Development mode usually runs the frontend separately:

```bash
cd frontend
npm run dev
```

Open:

```text
http://localhost:5173
```

Production-style local mode:

```bash
cd frontend
npm run build
cd ../backend
npm start
```

Then open the backend URL, for example:

```text
http://localhost:3001
```

## Startup Behavior

On startup, the server:

1. loads environment variables from `backend/.env`
2. initializes Express middleware
3. serves `../frontend/dist` if it exists
4. seeds an in-memory SQL bridge snapshot from player, team, and match archive summaries
5. starts HTTP and Socket.IO
6. starts the daily ingestor if enabled

If `../frontend/dist` does not exist, API routes still work. Non-API frontend routes return:

```text
Frontend build not found. Run the Vite build or dev server.
```

## Available Scripts

```bash
npm start
npm run test:cases
npm run test:cricapi
npm run dataset:clean
npm run chroma:build
npm run rebuild:all
npm run archive:rebuild
npm run chroma:query
```

### Script Details

| Script | Description |
| --- | --- |
| `npm start` | Starts `server.js`. |
| `npm run test:cases` | Runs local smoke coverage for routing, query responses, knowledge, archive lookup, and degraded Chroma behavior. |
| `npm run test:cricapi` | Tests CricAPI endpoints against a running backend. Uses `TEST_BASE_URL` or `http://localhost:3000`. |
| `npm run dataset:clean` | Runs `scripts/build_clean_dataset.py`. |
| `npm run chroma:build` | Rebuilds the local Chroma index with reset. |
| `npm run rebuild:all` | Alias for the full Chroma rebuild path. |
| `npm run archive:rebuild` | Runs the SQLite-first ETL pipeline in `scripts/sql_vector_pipeline.py`. |
| `npm run chroma:query` | Runs the local Chroma query helper. |

There is also `comprehensive-test.js`, which can be run directly:

```bash
node comprehensive-test.js
```

## API Reference

### Health and Metadata

#### `GET /api/status`

Returns Chroma, manifest, collection, and SQL bridge status.

Typical fields:

```json
{
  "status": "ready",
  "source": "chroma",
  "db_dir": "path-to-chroma-db",
  "collection": "cricket_semantic_index",
  "counts": {
    "documents": 0,
    "players": 0,
    "teams": 0,
    "matches": 0
  },
  "summary": {},
  "chroma_health": {},
  "sql_bridge": {}
}
```

#### `GET /api/about`

Returns `/api/status` plus dataset build dates from the Chroma manifest.

#### `GET /api/home`

Returns home dashboard data:

- archive status
- dataset summary
- top run leaders
- top wicket leaders
- top teams by win rate
- recent archived matches

#### `GET /api/options`

Returns available filter options:

- teams
- seasons
- venues

### Archive-Backed Entity Endpoints

#### `GET /api/players/search?q=&page=&limit=`

Searches local vector player profiles.

Query parameters:

- `q`: player search text
- `page`: one-based page number
- `limit`: page size, clamped by the backend

#### `GET /api/players/:id`

Returns an archive player profile with optional enriched metadata.

#### `GET /api/players/:id/summary?format=&season=`

Returns a compact player stat summary.

#### `GET /api/teams/search?q=`

Searches local team summaries.

#### `GET /api/matches?team=&season=&limit=&offset=&format=`

Returns archive match summaries. `team`, `season`, and `format` narrow the results.

#### `GET /api/matches/:id`

Returns one archive match summary.

### Natural-Language Query Endpoints

#### `POST /api/query`

Main query endpoint used by the frontend.

Request:

```json
{
  "question": "Compare Virat Kohli vs Rohit Sharma",
  "sessionId": "optional-session-id"
}
```

`query` can be used instead of `question`.

Response:

```json
{
  "type": "comparison",
  "title": "Cricket Intelligence",
  "image": "",
  "summary": "Short answer shown in the response card.",
  "stats": {},
  "extra": {
    "action": "compare_players",
    "intent": "compare_players",
    "suggestions": [],
    "insights": [],
    "detected_entities": [],
    "sources": []
  },
  "detected_entities": []
}
```

The normalized `type` is used by the frontend renderer. The more specific route is stored in `extra.action`.

Common normalized `type` values:

- `player`
- `team`
- `match`
- `comparison`
- `record`
- `playing_xi`
- `chat`

Common `extra.action` values:

- `player_stats`
- `player_season_stats`
- `team_stats`
- `team_info`
- `team_squad`
- `playing_xi`
- `live_update`
- `match_summary`
- `compare_players`
- `head_to_head`
- `top_players`
- `record_lookup`
- `glossary`
- `general_knowledge`
- `subjective_analysis`
- `chit_chat`
- `not_supported`

#### `GET /api/query/stream?question=&sessionId=`

Server-Sent Events version of the query endpoint.

Event names used by the current server:

- `status`
- `token`
- `ui_command`
- `answer`
- `error`
- `done`

Example:

```bash
curl -N "http://localhost:3001/api/query/stream?question=Virat%20Kohli%20stats"
```

### CricAPI Live Endpoints

These require `CRICAPI_KEY`.

#### `GET /api/cricapi/live-scores?limit=&offset=&includeRecent=&team=&matchType=`

Returns normalized current matches. By default, only live matches are returned. Set `includeRecent=true` to include recent completed matches from the provider.

#### `GET /api/cricapi/players/search?q=&limit=&offset=`

Searches CricAPI's player directory.

#### `GET /api/cricapi/players/:id`

Returns one CricAPI player profile.

#### `GET /api/cricapi/schedule?limit=&offset=&team=&matchType=&seriesId=&upcomingOnly=`

Returns upcoming or scheduled matches.

#### `GET /api/cricapi/series?q=&limit=&offset=`

Lists series and optionally ranks by query text.

#### `GET /api/cricapi/series/:id`

Returns one series and its matches.

### Cricbuzz Enrichment Endpoint

#### `GET /api/cricbuzz/player-card?name=`

Used by the frontend when a detected entity is clicked. It tries Cricbuzz first, then falls back to the local vector archive and profile metadata when possible.

Possible providers in the response:

- `cricbuzz`
- `fallback`

Possible fallback reasons:

- `no_cricbuzz_match`
- `cricbuzz_subscription_unavailable`
- `cricbuzz_disabled_or_unconfigured`
- `cricbuzz_unavailable`

## Socket.IO

The server creates a Socket.IO instance on the same HTTP server.

On connection, it emits:

```json
{
  "type": "socket_ready",
  "title": "Live feed connected",
  "summary": "Waiting for live cricket updates."
}
```

The daily ingestor can emit `live-score-alert` events such as:

- `live_snapshot`
- `match_ingested`

The frontend listens for `live-score-alert` and inserts useful alerts into the chat thread.

## Query Pipeline

At a high level, `POST /api/query` follows this path:

1. Validate the question.
2. Check whether semantic cache is allowed for this query.
3. If a semantic cache hit is found, normalize and return the cached response.
4. Apply session context for pronoun follow-ups when `sessionId` is present.
5. Route the question with `llamaRouter.js`.
6. Build deterministic structured context when possible.
7. Fetch selected provider context:
   - vector archive
   - CricAPI
   - Cricbuzz
   - ESPN
   - local knowledge
8. Synthesize or ground the answer.
9. Convert the result into the unified frontend response shape.
10. Save semantic cache entry for cacheable successful queries.

The cache is bypassed for many volatile or subjective queries, including live, today, latest, comparison, prediction, top, best, captain, coach, owner, trophy, and history-style questions.

## Data Sources

### Local Knowledge JSON

Files in `backend/data` cover deterministic rules and records:

```text
cricket_rules.json
cricket_terms.json
cricket_history.json
cricket_records.json
worldcup_winners.json
equipment_and_training.json
```

These are used for glossary, rules, history, records, and basic cricket knowledge.

### SQLite Archive

Generated SQLite files hold archive stats and runtime updates:

```text
cricket_archive.sqlite3
cricket_runtime.sqlite3
sqlite_manifest.json
```

The full rebuild path is SQLite-first, then Chroma semantic documents are generated from SQL aggregates.

### Chroma Semantic Index

Generated Chroma files hold semantic documents for:

- player profiles
- team summaries
- match summaries
- semantic cache entries

Main generated paths:

```text
chroma_db/
chroma_manifest.json
cleaned_dataset_manifest.json
```

## Rebuilding Data

### Clean Dataset

```bash
cd backend
npm run dataset:clean
```

### Rebuild Chroma

```bash
npm run chroma:build
```

### Rebuild SQLite Archive and Chroma Flow

```bash
npm run archive:rebuild
```

### Query Chroma Helper

```bash
npm run chroma:query
```

## Generated Artifacts

Large local artifacts are intentionally ignored by git:

```text
node_modules/
.env
chroma_db/
chroma_manifest.json
cricket_archive.sqlite3
sqlite_manifest.json
cleaned_balls_all_matches.csv
cleaned_dataset_manifest.json
__pycache__/
scripts/__pycache__/
```

Runtime SQLite files may also appear locally:

```text
cricket_runtime.sqlite3
cricket_runtime.sqlite3-shm
cricket_runtime.sqlite3-wal
```

Do not commit API keys, local databases, generated vector stores, logs, or temporary CSV artifacts.

## Testing

### Local Smoke Tests

```bash
cd backend
npm run test:cases
```

This validates route decisions and response shapes for live, archive, knowledge, history, comparison, and fallback queries.

### CricAPI Endpoint Tests

Start the backend, then run:

```bash
cd backend
npm run test:cricapi
```

If your backend uses port `3001`, set:

```powershell
$env:TEST_BASE_URL="http://localhost:3001"
npm run test:cricapi
```

### Comprehensive Local Test

```bash
cd backend
node comprehensive-test.js
```

This script checks environment loading, vector DB access, routing, query processing, knowledge files, service imports, and SQLite presence.

## Troubleshooting

### Port Already in Use

The server exits with a clear message if `PORT` is occupied. Use another port:

```powershell
$env:PORT="3002"
npm start
```

Update `frontend/.env` if the frontend dev server points to the old port.

### Frontend Build Not Found

Build the frontend:

```bash
cd frontend
npm run build
cd ../backend
npm start
```

During frontend development, use `npm run dev` in the frontend instead of relying on backend static hosting.

### CricAPI Key Missing

Live endpoints return a provider configuration error when `CRICAPI_KEY` is empty. Set the key in `backend/.env`.

### CricAPI Rate Limited or Timed Out

The query layer returns a graceful provider status when live data is unavailable. Archive and knowledge queries continue to work.

Useful checks:

```text
GET /api/cricapi/live-scores?includeRecent=true&limit=2
GET /api/status
```

### Cricbuzz Micro-Card Unavailable

Check:

- `CRICBUZZ_ENABLED=true`
- `CRICBUZZ_RAPIDAPI_KEY` is set
- RapidAPI subscription supports the configured endpoints
- `CRICBUZZ_RAPIDAPI_HOST` matches the provider

The backend attempts a local fallback profile when Cricbuzz cannot provide a match.

### Chroma Missing or Not Ready

Check:

```text
GET /api/status
```

If the DB is missing, rebuild:

```bash
cd backend
npm run chroma:build
```

If using a custom location, set:

```env
CHROMA_DB_DIR=D:\path\to\chroma_db
```

### Local LLM Not Reachable

Check the endpoint:

```text
http://localhost:8080/v1/chat/completions
```

Then verify:

```env
LLM_ENDPOINT=http://localhost:8080/v1/chat/completions
LLM_MODEL=local
LLM_TIMEOUT_MS=30000
```

For large local models, increase `LLM_TIMEOUT_MS`.

### `test:cricapi` Hits the Wrong Port

`scripts/test-cricapi.js` defaults to `http://localhost:3000`. If your server runs on `3001`, set:

```powershell
$env:TEST_BASE_URL="http://localhost:3001"
npm run test:cricapi
```

## Example Questions

- `Virat Kohli stats`
- `Virat Kohli runs in 2019`
- `MS Dhoni ODI average`
- `India team summary`
- `India vs Australia head to head`
- `Compare Rohit Sharma vs Virat Kohli`
- `Highest individual score in ODI`
- `Fastest century in ODI`
- `Who won the 2011 World Cup?`
- `What is LBW?`
- `Show recent live scores`
- `Today match schedule`
- `Who may win today match?`

## Development Notes

- `loadEnv.js` reads `backend/.env` without requiring the `dotenv` package.
- API keys should stay in `backend/.env`, never in frontend files.
- The backend response shape is intentionally stable for the frontend.
- Provider failures should be returned as structured status or fallback responses, not raw crashes.
- Large generated data files should remain local.
- Daily ingestion updates SQLite first and then refreshes only affected Chroma documents.
- Socket.IO shares the same HTTP server and port as Express.
