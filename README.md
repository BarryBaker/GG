# GGPoker Omaha Daily Leaderboard Scraper

A Python scraper to access the GGPoker Omaha Daily Leaderboard page and interact with the embedded iframe.

## Features

- **Visible Browser**: The scraper runs with a visible Chrome browser so you can follow along with what's happening
- **Automatic Setup**: Automatically downloads and configures ChromeDriver
- **Iframe Interaction**: Finds and interacts with the iframe containing the leaderboard data
- **Step-by-Step Process**: Clear logging of each step in the scraping process

## Setup (Local)

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Make sure you have Chrome browser installed** on your system

## Usage (Local)

Run the scraper once:
```bash
python ggpoker_scraper.py
```

Run continuously every 5 minutes:
```bash
python ggpoker_scraper.py --interval 300
```

Or via environment variables:
```bash
export INTERVAL=300
export MAX_TS_COLUMNS=100
export DB_PATH=ggpoker_leaderboards.db
python ggpoker_scraper.py
```

## What the Scraper Does

1. **Opens Chrome browser** (visible so you can follow along)
2. **Navigates to** https://ggpoker.com/promotions/omaha-daily-leaderboard/
3. **Finds the PLO section** with the iframe
4. **Clicks on the iframe** to interact with it
5. **Opens the iframe content** in a new tab for exploration
6. **Extracts blind level options** from the dropdown
7. **Clicks through each blind level** to make them active
8. **Scrapes player ranking data** for each blind level
9. **Stores data in SQLite database** with time-series tracking

## Database Structure

The scraper automatically creates and manages a SQLite database with:

- **Dynamic tables** for each game type and blind level (e.g., "PLO - $0.01/$0.02")
- **Time-series columns** for each scraping session (e.g., "ts_2025-08-12_12h42")
- **Player tracking** with points progression over time
- **Automatic table management** - creates new tables and columns as needed

## Database Schema Example

```
Table: PLO - $0.01/$0.02
player          | ts_2025-08-12_12h42 | ts_2025-08-12_12h47 | ts_2025-08-12_12h52
----------------|---------------------|---------------------|---------------------
PlayerName1     | 100                 | 120                 | 120
PlayerName2     | 200                 | 220                 | 250
PlayerName3     | 0                   | 0                   | 50
```

## Viewing the Database

Use the included viewer script:
```bash
python view_database.py
```

## Run Continuously in Docker

Build and run with Docker Compose (persists the SQLite DB under `./data`):
```bash
docker compose up -d --build
```

Environment overrides (edit `docker-compose.yml` or pass with `-e`):
- `INTERVAL` (seconds between runs, default 300)
- `MAX_TS_COLUMNS` (0=unlimited; otherwise keeps most recent N timestamp columns)
- `DB_PATH` (defaults to `/data/ggpoker_leaderboards.db` inside container)

Stop:
```bash
docker compose down
```

## Next Steps

After running the scraper:
1. Data will be automatically stored in the database
2. Each blind level gets its own table
3. Each scraping session adds a new timestamp column
4. Player progress is tracked over time
5. You can now build queries and analytics on this data

## Notes

- The scraper is designed to be exploratory - it opens the page and iframe so you can see what's available
- It will wait for you to press Enter before closing the browser
- All steps are logged to the console so you can see what's happening
