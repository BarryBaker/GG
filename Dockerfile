# syntax=docker/dockerfile:1

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System dependencies for Chromium + Chromedriver
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first for better layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Copy application source
COPY . /app

# Defaults (override via environment or compose)
ENV CHROME_BIN=/usr/bin/chromium \
    CHROME_TYPE=chromium \
    CHROME_DRIVER_BIN=/usr/bin/chromedriver \
    DB_PATH=/data/ggpoker_leaderboards.db \
    MAX_TS_COLUMNS=0 \
    INTERVAL=300

# Persistent data directory for SQLite
RUN mkdir -p /data
VOLUME ["/data"]

# Run continuously with interval
CMD ["/bin/sh", "-c", "python ggpoker_scraper.py --interval ${INTERVAL}"]
