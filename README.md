# GGPoker Leaderboards Scraper & API

Scrapes the GGPoker Omaha Daily Leaderboard, stores results in a normalized database (SQLite locally, PostgreSQL in the cloud), and serves the data through a small HTTP API. Ships with Docker, a local DB viewer, and a remote Postgres viewer.

## Features

- Modern Selenium-based scraper (Chrome/Chromium, headless by default)
- Configurable target page and game (e.g., `PLO`)
- Normalized schema: `leaderboards`, `players` (with `country`), `update_batch`, `facts`
- Auto-detects DB backend: SQLite by default, PostgreSQL when `DATABASE_URL` is set
- Lightweight Flask API for tables, pivoted data, player history, and top players
- Docker images for continuous scraping and API service
- Helper viewers for local SQLite and remote PostgreSQL

## Quick Start (Local)

1) Install dependencies
```bash
python -m venv .venv && source .venv/bin/activate  # optional
pip install -r requirements.txt
```

2) Run the scraper once (headless by default)
```bash
python ggpoker_scraper.py
```

3) Or run continuously every 5 minutes
```bash
python ggpoker_scraper.py --interval 300
```

Environment examples
```bash
export INTERVAL=300
export DB_PATH=ggpoker_leaderboards.db
export PROMO_URL=https://ggpoker.com/promotions/omaha-daily-leaderboard/
export GAME_NAME=PLO
python ggpoker_scraper.py
```

Chrome/Chromium notes
- The project auto-installs a matching chromedriver. In Docker, we use system Chromium.
- To see the browser locally, either pass `--headless` off by setting `HEADLESS=0`, or omit `HEADLESS`.

## HTTP API

Start the API locally (uses the same DB autodetection as the scraper):
```bash
python api.py
# or production-style:
python serve.py  # runs gunicorn binding to PORT (default 8000)
```

Base URL: `http://localhost:8000`

Endpoints
- GET `/health` → `{ "status": "ok" }`
- GET `/tables` → `["PLO_...", ...]` from the `leaderboards` table
- GET `/tables/<leaderboard>/data?columns=4&limit=16`
  - Returns pivoted data: first column is `player`, followed by the last N timestamps for that leaderboard
  - Sorted by the latest column (numeric, NULLS LAST), returns up to `limit` rows
- GET `/tables/<leaderboard>/player?name=<Player+Name>`
  - Returns a single row of that player's points across all timestamps for the leaderboard
  - Includes `country`
- GET `/tables/<leaderboard>/top-players?limit=50`
  - Returns best daily standings per day aggregated across the historical updates (time cutoffs differ per backend)

Example
```bash
curl http://localhost:8000/health
curl http://localhost:8000/tables
curl "http://localhost:8000/tables/PLO___0dot01_0dot02/data?columns=4&limit=16"
curl --get "http://localhost:8000/tables/PLO___0dot01_0dot02/player" --data-urlencode "name=Some Player"
curl "http://localhost:8000/tables/PLO___0dot01_0dot02/top-players?limit=25"
```

## Database

Autodetection
- If `DATABASE_URL` (or `DATABASE_PRIVATE_URL` / `DATABASE_PUBLIC_URL` for the API) is set to a PostgreSQL URL, the app uses PostgreSQL
- Otherwise, it falls back to SQLite (file path from `SQLITE_DB_PATH` or `DB_PATH`, default `ggpoker_leaderboards.db`)

Normalized schema
```sql
leaderboards(id, name UNIQUE)
players(id, name UNIQUE, country)
update_batch(id, ts)
facts(leaderboard_id, update_id, player_id, points,
      PRIMARY KEY (leaderboard_id, player_id, update_id))
```

Viewers
- Local SQLite: `python view_database.py`
- Remote PostgreSQL (Railway, etc.):
  1) copy `env_template.txt` → `.env` and set `DATABASE_URL`
  2) `python view_remote_database.py`

## Configuration (Environment)

Scraper
- `PROMO_URL` (default: Omaha Daily Leaderboard URL)
- `GAME_NAME` (default: `PLO`)
- `INTERVAL` run cadence in seconds; `0` runs once (default: `300` in Docker, `0` locally)
- `HEADLESS` `1/true/yes` to run headless (default: headless)
- `CHROME_BIN`, `CHROME_DRIVER_BIN`, `CHROME_TYPE` (e.g., `chromium`) for custom binaries

Database
- `DATABASE_PRIVATE_URL` → preferred by the API if set
- `DATABASE_URL` → primary Postgres URL for scraper/API
- `DATABASE_PUBLIC_URL` → last fallback for the API
- `SQLITE_DB_PATH` or `DB_PATH` → SQLite file path (default: `ggpoker_leaderboards.db`)

API
- `PORT` (default: `8000`)

## Docker

We provide two services in `docker-compose.yml`:
- `scraper`: runs continuously and writes to a persistent SQLite volume at `./data`
- `api`: exposes HTTP API on port `8000`

Build & run
```bash
docker compose up -d --build
```

Persisted data
- SQLite file is mounted at `./data/ggpoker_leaderboards.db`

Environment overrides (edit `docker-compose.yml` or pass with `-e`)
- `INTERVAL` (e.g., `600`)
- `DB_PATH` (container default `/data/ggpoker_leaderboards.db`)
- `CHROME_BIN=/usr/bin/chromium`, `CHROME_TYPE=chromium`
- For the API: `DATABASE_PRIVATE_URL` / `DATABASE_URL` / `DATABASE_PUBLIC_URL`

Stop
```bash
docker compose down
```

## How the Scraper Works (High-Level)

1) Opens the promo page and locates the configured game section (e.g., `PLO`)
2) Finds the embedded leaderboard iframe and opens its URL in a new tab
3) Enumerates blind-level options from the dropdown
4) Clicks through each blind level and extracts the ranking table
5) Upserts normalized rows into the database with a new `update_batch` timestamp

## Troubleshooting

- Chromedriver/Chrome mismatch: ensure Chrome is installed locally, or run in Docker
- PostgreSQL connection: verify `DATABASE_URL` and network access
- Empty `/tables`: ensure the scraper has run and written data to the DB

## Security Notes

- Do not commit `.env`; it contains credentials
- Prefer private DB URLs (`DATABASE_PRIVATE_URL`) for internal services
