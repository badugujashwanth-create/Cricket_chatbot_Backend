# Cricket Chat Bot Backend

Express backend for the Cricket Chat Bot application. It serves the frontend, answers structured cricket questions from the local Chroma vector archive, enriches player and team results with Wikipedia metadata, and optionally blends live CricAPI data with an LLM-driven query router.

## What This Service Does

- Serves the static frontend from `../frontend`
- Loads player, team, and match summaries from the local Chroma vector archive
- Exposes archive search endpoints for players, teams, matches, and summaries
- Accepts natural-language cricket questions through `/api/query`
- Streams query progress and results through Server-Sent Events
- Pulls live scores, schedules, series, and player details from CricAPI
- Enriches player cards with profile metadata such as country, image, and Wikipedia links

## Key Files

- `server.js`: Express app, route registration, static hosting, and startup
- `queryService.js`: natural-language orchestration and response shaping
- `llamaRouter.js`: intent routing and action selection
- `llamaClient.js`: local OpenAI-compatible endpoint and OpenAI fallback client
- `cricApiService.js`: live CricAPI integration, normalization, and caching
- `playerProfileService.js`: player enrichment and profile lookup
- `vectorIndexService.js`: Chroma-backed player, team, and match indexing helpers
- `scripts/`: dataset rebuild, vector build, and verification scripts

## Requirements

- Node.js 18 or newer
- npm
- Python 3.x if you want to rebuild the cleaned dataset or local Chroma index
- Optional local `llama.cpp` server, or an OpenAI API key, for routed reasoning
- CricAPI key if you want live scores, schedule, or series endpoints

## Installation

```bash
cd backend
npm install
```

## Environment Variables

Create `backend/.env` from `backend/.env.example`, or export the variables in your shell.

```env
PORT=3000
LLM_ENDPOINT=http://localhost:8080/v1/chat/completions
LLM_MODEL=local
LLM_TIMEOUT_MS=30000
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4o
OPENAI_ENDPOINT=https://api.openai.com/v1/chat/completions
CRICAPI_KEY=
CRICAPI_TIMEOUT_MS=10000
```

### Variable Notes

- `PORT`: backend HTTP port. Defaults to `3000`.
- `LLM_ENDPOINT`: full OpenAI-compatible chat completions URL for a local model server.
- `LLM_MODEL`: model name sent to the local endpoint.
- `LLM_TIMEOUT_MS`: timeout for routed LLM requests.
- `OPENAI_API_KEY`: optional OpenAI key used when OpenAI is enabled instead of the local model path.
- `OPENAI_MODEL`: OpenAI chat model name.
- `OPENAI_ENDPOINT`: override for the OpenAI chat completions endpoint.
- `CRICAPI_KEY`: required for all live-data endpoints under `/api/cricapi/*`.
- `CRICAPI_TIMEOUT_MS`: timeout for external CricAPI requests.

## Running Locally

### 1. Optional: start a local `llama.cpp` server

```bash
llama-server -m <MODEL>.gguf --port 8080
```

The backend expects an OpenAI-compatible chat completions endpoint, for example:

```text
http://localhost:8080/v1/chat/completions
```

### 2. Start the backend

```bash
npm start
```

The server starts on:

```text
http://localhost:3000
```

### 3. Open the app

Because the backend statically serves the frontend, the normal local entry point is:

```text
http://localhost:3000
```

## Startup Behavior

- On boot, the server starts immediately and begins indexing the local archive in the background.
- Until the archive is ready, archive-backed endpoints can return `503` with a loading message.
- Use `GET /api/status` to track readiness.
- Once the archive status becomes `ready`, the main analytics endpoints are available.

## API Overview

## Health and app metadata

- `GET /api/status`
- `GET /api/about`
- `GET /api/home`
- `GET /api/options`

## Archive-backed entities

- `GET /api/players/search?q=&page=&limit=`
- `GET /api/players/:id`
- `GET /api/players/:id/summary?format=&season=`
- `GET /api/teams/search?q=`
- `GET /api/matches?team=&season=&venue=&limit=&offset=`
- `GET /api/matches/:id`

## Query layer

- `POST /api/query`
- `GET /api/query/stream?question=`

### `POST /api/query`

Expected request body:

```json
{
  "question": "Compare Virat Kohli vs Rohit Sharma"
}
```

Typical response shape:

```json
{
  "answer": "Short natural-language answer",
  "summary": "High-level result",
  "details": {},
  "suggestions": [],
  "data": {},
  "followups": []
}
```

The `data.type` field drives the frontend stage renderer. Common values include:

- `player_stats`
- `team_stats`
- `compare_players`
- `match_summary`
- `head_to_head`
- `top_players`
- `live_update`

### `GET /api/query/stream`

This endpoint returns Server-Sent Events. Current event names are:

- `status`
- `answer`
- `error`
- `done`

It is useful when you want progressive query feedback instead of waiting for the final JSON payload.

## Live CricAPI endpoints

- `GET /api/cricapi/live-scores?limit=&offset=&includeRecent=&team=&matchType=`
- `GET /api/cricapi/players/search?q=&limit=&offset=`
- `GET /api/cricapi/players/:id`
- `GET /api/cricapi/schedule?limit=&offset=&team=&matchType=&seriesId=&upcomingOnly=`
- `GET /api/cricapi/series?q=&limit=&offset=`
- `GET /api/cricapi/series/:id`

### CricAPI behavior

- Live provider responses are normalized before being returned.
- Responses are cached in memory for short intervals to reduce repeated external calls.
- If `CRICAPI_KEY` is missing, these endpoints return a provider configuration error.
- If CricAPI times out or rate-limits the account, the query layer degrades gracefully and exposes provider status in the response.

## Scripts

```bash
npm start
npm run test:cases
npm run test:cricapi
npm run test:name-resolution
npm run dataset:clean
npm run chroma:build
npm run rebuild:all
npm run chroma:query
```

### Script summary

- `npm start`: starts the Express server
- `npm run test:cases`: runs local archive question coverage checks
- `npm run test:cricapi`: exercises live provider integration
- `npm run test:name-resolution`: validates name matching and entity resolution
- `npm run dataset:clean`: rebuilds the cleaned local dataset
- `npm run chroma:build`: rebuilds the cleaned dataset and local Chroma index
- `npm run rebuild:all`: alias for a full dataset and Chroma rebuild
- `npm run chroma:query`: queries the local vector store helper

## Dataset and Generated Artifacts

Local rebuild scripts generate large artifacts that are intentionally ignored by git:

- `cleaned_balls_all_matches.csv`
- `cleaned_dataset_manifest.json`
- `chroma_db/`
- `chroma_manifest.json`

Those files should be rebuilt locally when needed instead of committed.

## Common Development Notes

- The first warm-up can take time because the archive is indexed in memory.
- Archive-backed endpoints wait for readiness, but live CricAPI endpoints are available independently.
- The backend assumes the frontend directory exists one level above this repo.
- `.env`, `node_modules`, generated dataset files, logs, and local vector artifacts are excluded from git.

## Example Questions

- `Virat Kohli stats`
- `Virat Kohli runs in 2019`
- `Top run scorers in 2024`
- `India team stats in ODI`
- `India vs Australia head to head`
- `Compare Rohit Sharma vs Virat Kohli`
- `Summarize the latest match`
- `Show recent live scores`

## Repository Scope

This backend repository is intended to be published separately from the frontend repository. The frontend is served locally from a sibling folder, but the git history for the backend remains independent.
