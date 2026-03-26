# Cricket Stats AI Backend

Node.js backend for the Cricket Stats AI application. It serves the frontend, loads the cricket archive dataset, answers structured stats queries, and enriches responses with live CricAPI data and optional local LLM routing.

## What This Repo Contains

- `server.js`: Express server and API routes
- `datasetStore.js`: archive loading, indexing, and local stats queries
- `queryService.js`: query routing, synthesis, and response shaping
- `cricApiService.js`: live data integration
- `playerProfileService.js`: player enrichment via CricAPI and Wikipedia
- `scripts/`: dataset rebuild and test utilities

## Requirements

- Node.js 18+
- Optional local `llama.cpp` server for routed reasoning
- CricAPI key for live data endpoints

## Install

```bash
npm install
```

## Environment

Create a `.env` file or export these variables:

```powershell
$env:LLM_ENDPOINT="http://localhost:8080/v1/chat/completions"
$env:LLM_MODEL="local"
$env:LLM_TIMEOUT_MS="30000"
$env:CRICAPI_KEY="<your-cricapi-key>"
$env:CRICAPI_TIMEOUT_MS="10000"
```

See `.env.example` for the full list.

## Run

```bash
npm start
```

Server default:

- `http://localhost:3000`

The frontend static files are served from `../frontend`.

## Main API Endpoints

### Archive data

- `GET /api/status`
- `GET /api/about`
- `GET /api/home`
- `GET /api/options`
- `GET /api/players/search?q=&page=&limit=`
- `GET /api/players/:id`
- `GET /api/players/:id/summary?format=&season=`
- `GET /api/matches?team=&season=&venue=&limit=&offset=`
- `GET /api/matches/:id`

### AI query layer

- `POST /api/query`
- `GET /api/query/stream?question=`

### Live CricAPI data

- `GET /api/cricapi/live-scores?limit=&offset=&includeRecent=&team=&matchType=`
- `GET /api/cricapi/players/search?q=&limit=&offset=`
- `GET /api/cricapi/players/:id`
- `GET /api/cricapi/schedule?limit=&offset=&team=&matchType=&seriesId=&upcomingOnly=`
- `GET /api/cricapi/series?q=&limit=&offset=`
- `GET /api/cricapi/series/:id`

## Tests

```bash
npm run test:cases
npm run test:cricapi
npm run test:name-resolution
```

## Dataset Rebuild

To rebuild the cleaned archive and Chroma index:

```bash
npm run chroma:build
```

This produces:

- `cleaned_balls_all_matches.csv`
- `cleaned_dataset_manifest.json`
- `chroma_db/`
- `chroma_manifest.json`

Generated dataset and Chroma artifacts are intentionally ignored in git and should be rebuilt locally.

## Notes

- First boot can take several minutes because the archive is indexed in memory.
- The dataset status can be checked through `GET /api/status`.
- The player detail endpoint now returns enriched player profile metadata when available.
