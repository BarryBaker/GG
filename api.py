#!/usr/bin/env python3
"""
HTTP API for listing and reading tables similarly to the viewer.

Endpoints:
  GET  /health                                  → { status: ok }
  GET  /tables?prefix=PLO                       → ["PLO_...", ...]
  GET  /tables/<table>/columns                  → column metadata
  GET  /tables/<table>/data?last_columns=10&limit=10
      → first column + last N columns, sorted by the last column DESC with NULLS LAST,
        returns up to 'limit' rows (defaults: last_columns=10, limit=10)

Notes:
  - Prefers DATABASE_PRIVATE_URL, then DATABASE_URL, then DATABASE_PUBLIC_URL
  - Uses short-lived autocommit connections to avoid long-held locks
  - Sorting uses numeric parsing with NULLS LAST for robust ordering
  - CORS enabled
"""

import os
from typing import Dict, List, Tuple
from urllib.parse import urlparse

from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import sqlite3



def resolve_db_config():
    """Decide whether to use PostgreSQL or SQLite based on env vars.
    Returns a tuple (driver, conn_info), where driver is 'postgres' or 'sqlite'.
    For postgres, conn_info is a parsed urlparse result; for sqlite, it's a file path.
    """
    load_dotenv()
    
    sqlite_path = os.getenv("SQLITE_DB_PATH") or os.getenv("DB_PATH")
   
    pg_url = None
    for key in ("DATABASE_PRIVATE_URL", "DATABASE_URL", "DATABASE_PUBLIC_URL"):
        val = os.getenv(key)
        if val and (val.startswith("postgres://") or val.startswith("postgresql://")):
            pg_url = val
            break
    if sqlite_path and not pg_url:
        return ("sqlite", sqlite_path)
    if pg_url:
        return ("postgres", urlparse(pg_url))
    # default to local sqlite file if nothing provided
    return ("sqlite", os.path.abspath("gg_leaderboards.db"))


def open_connection():
    """Open a short-lived autocommit connection to the configured DB."""
    driver, info = resolve_db_config()
    if driver == "sqlite":
        conn = sqlite3.connect(info)
        conn.row_factory = sqlite3.Row
        return conn
    # postgres
    parsed = info
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=(parsed.path[1:] if parsed.path else None),
        user=parsed.username,
        password=parsed.password,
    )
    conn.autocommit = True
    return conn


def list_tables(prefix: str = "PLO") -> List[str]:
    """List leaderboard names starting with prefix (works for both PG and SQLite)."""
    like_pattern = f"{prefix}%"
    driver, _ = resolve_db_config()
    with open_connection() as conn:
        cur = conn.cursor()
        if driver == "sqlite":
            cur.execute(
                "SELECT name FROM leaderboards WHERE name LIKE ? ORDER BY name",
                (like_pattern,),
            )
            rows = cur.fetchall()
            return [row[0] if isinstance(row, tuple) else row["name"] for row in rows]
        # postgres: read from leaderboards table too
        cur.execute(
            "SELECT name FROM leaderboards WHERE name LIKE %s ORDER BY name",
            (like_pattern,),
        )
        return [r[0] for r in cur.fetchall()]


def fetch_table_data(leaderboard: str, limit: int, last_columns: int) -> Dict[str, List]:
    driver, _ = resolve_db_config()
    if driver == "sqlite":
        with open_connection() as conn:
            cur = conn.cursor()
            # Resolve leaderboard id
            cur.execute("SELECT id FROM leaderboards WHERE name = ?", (leaderboard,))
            row = cur.fetchone()
            if not row:
                return {"columns": [], "rows": []}
            lb_id = row[0]

            # Latest N distinct updates for this leaderboard (ids and timestamps)
            cur.execute(
                """
                SELECT ub.id, ub.ts
                FROM update_batch ub
                JOIN facts f ON f.update_id = ub.id
                WHERE f.leaderboard_id = ?
                GROUP BY ub.id, ub.ts
                ORDER BY ub.id DESC
                LIMIT ?
                """,
                (lb_id, last_columns),
            )
            updates = cur.fetchall()
            if not updates:
                return {"columns": ["player"], "rows": []}
            update_ids_desc = [u[0] for u in updates]
            ts_headers_desc = [u[1] for u in updates]
            update_ids = list(reversed(update_ids_desc))
            ts_headers = list(reversed(ts_headers_desc))

            placeholders_updates = ",".join(["?"] * len(update_ids))
            cur.execute(
                (
                    f"""
                    SELECT pl.name, ub.ts, f.points
                    FROM facts f
                    JOIN players pl ON pl.id = f.player_id
                    JOIN update_batch ub ON ub.id = f.update_id
                    WHERE f.leaderboard_id = ?
                      AND f.update_id IN ({placeholders_updates})
                    """
                ),
                [lb_id, *update_ids],
            )
            all_points = cur.fetchall()
            if not all_points:
                return {"columns": ["player"] + ts_headers, "rows": []}

            players = sorted({name for (name, _ts, _pts) in all_points})

            points_map: Dict[str, Dict[str, float]] = {name: {ts: 0 for ts in ts_headers} for name in players}
            for name, ts, pts in all_points:
                if ts in points_map[name]:
                    points_map[name][ts] = pts

            rows = []
            for name in players:
                row = [name] + [points_map[name][ts] for ts in ts_headers]
                rows.append(row)

            rows.sort(key=lambda r: (r[-1] if r[-1] is not None else 0), reverse=True)
            rows = rows[:limit]

            return {"columns": ["player"] + ts_headers, "rows": rows}
    else:
        # Postgres branch
        with open_connection() as conn:
            with conn.cursor() as cur:
                # Resolve leaderboard id
                cur.execute("SELECT id FROM leaderboards WHERE name = %s", (leaderboard,))
                row = cur.fetchone()
                if not row:
                    return {"columns": [], "rows": []}
                lb_id = row[0]
                # Latest N updates
                cur.execute(
                    """
                    SELECT ub.id, ub.ts
                    FROM update_batch ub
                    JOIN facts f ON f.update_id = ub.id
                    WHERE f.leaderboard_id = %s
                    GROUP BY ub.id, ub.ts
                    ORDER BY ub.id DESC
                    LIMIT %s
                    """,
                    (lb_id, last_columns),
                )
                updates = cur.fetchall()
                if not updates:
                    return {"columns": ["player"], "rows": []}
                update_ids_desc = [u[0] for u in updates]
                ts_headers_desc = [u[1] for u in updates]
                update_ids = list(reversed(update_ids_desc))
                ts_headers = list(reversed(ts_headers_desc))

                # Build IN placeholders
                in_ph = ",".join(["%s"] * len(update_ids))
                cur.execute(
                    f"""
                    SELECT pl.name, ub.ts, f.points
                    FROM facts f
                    JOIN players pl ON pl.id = f.player_id
                    JOIN update_batch ub ON ub.id = f.update_id
                    WHERE f.leaderboard_id = %s
                      AND f.update_id IN ({in_ph})
                    """,
                    tuple([lb_id] + update_ids),
                )
                all_points = cur.fetchall()
                if not all_points:
                    return {"columns": ["player"] + ts_headers, "rows": []}

                players = sorted({name for (name, _ts, _pts) in all_points})
                points_map: Dict[str, Dict[str, float]] = {name: {ts: 0 for ts in ts_headers} for name in players}
                for name, ts, pts in all_points:
                    if ts in points_map[name]:
                        points_map[name][ts] = float(pts) if pts is not None else 0

                rows = []
                for name in players:
                    row = [name] + [points_map[name][ts] for ts in ts_headers]
                    rows.append(row)

                rows.sort(key=lambda r: (r[-1] if r[-1] is not None else 0), reverse=True)
                rows = rows[:limit]

                return {"columns": ["player"] + ts_headers, "rows": rows}


def fetch_player_data(leaderboard: str, player_name: str) -> Dict[str, List]:
    """Return pivoted data for a player across ALL updates of the given leaderboard.
    Columns: [player, ts1..tsN] oldest→newest; Row: [name, points...] with 0 for missing.
    Includes 'country' of the player.
    """
    driver, _ = resolve_db_config()
    if driver == "sqlite":
        with open_connection() as conn:
            cur = conn.cursor()
            # Resolve leaderboard id
            cur.execute("SELECT id FROM leaderboards WHERE name = ?", (leaderboard,))
            row = cur.fetchone()
            if not row:
                return {"columns": ["player"], "rows": [[player_name]], "country": None}
            lb_id = row[0]

            # Fetch country
            cur.execute("SELECT country FROM players WHERE name = ?", (player_name,))
            crow = cur.fetchone()
            country = crow[0] if crow and crow[0] is not None else None

            # Get all update ids and timestamps for this leaderboard (only those that appear in facts)
            cur.execute(
                """
                SELECT ub.id, ub.ts
                FROM update_batch ub
                JOIN facts f ON f.update_id = ub.id
                WHERE f.leaderboard_id = ?
                GROUP BY ub.id, ub.ts
                ORDER BY ub.id ASC
                """,
                (lb_id,),
            )
            updates = cur.fetchall()
            if not updates:
                return {"columns": ["player"], "rows": [[player_name]], "country": country}
            update_ids = [u[0] for u in updates]
            ts_headers = [u[1] for u in updates]

            # Fetch player's points for those updates
            placeholders = ",".join(["?"] * len(update_ids))
            cur.execute(
                (
                    f"""
                    SELECT f.update_id, f.points
                    FROM facts f
                    JOIN players pl ON pl.id = f.player_id
                    WHERE f.leaderboard_id = ? AND pl.name = ? AND f.update_id IN ({placeholders})
                    """
                ),
                [lb_id, player_name, *update_ids],
            )
            pts_by_update = {uid: 0 for uid in update_ids}
            for uid, pts in cur.fetchall():
                pts_by_update[uid] = pts

            points_row = [pts_by_update[uid] for uid in update_ids]
            return {"columns": ["player"] + ts_headers, "rows": [[player_name] + points_row], "country": country}
    else:
        with open_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM leaderboards WHERE name = %s", (leaderboard,))
                row = cur.fetchone()
                if not row:
                    return {"columns": ["player"], "rows": [[player_name]], "country": None}
                lb_id = row[0]
                # Fetch country
                cur.execute("SELECT country FROM players WHERE name = %s", (player_name,))
                crow = cur.fetchone()
                country = crow[0] if crow and crow[0] is not None else None

                cur.execute(
                    """
                    SELECT ub.id, ub.ts
                    FROM update_batch ub
                    JOIN facts f ON f.update_id = ub.id
                    WHERE f.leaderboard_id = %s
                    GROUP BY ub.id, ub.ts
                    ORDER BY ub.id ASC
                    """,
                    (lb_id,),
                )
                updates = cur.fetchall()
                if not updates:
                    return {"columns": ["player"], "rows": [[player_name]], "country": country}
                update_ids = [u[0] for u in updates]
                ts_headers = [u[1] for u in updates]
                in_ph = ",".join(["%s"] * len(update_ids))
                cur.execute(
                    f"""
                    SELECT f.update_id, f.points
                    FROM facts f
                    JOIN players pl ON pl.id = f.player_id
                    WHERE f.leaderboard_id = %s AND pl.name = %s AND f.update_id IN ({in_ph})
                    """,
                    tuple([lb_id, player_name] + update_ids),
                )
                pts_by_update = {uid: 0 for uid in update_ids}
                for uid, pts in cur.fetchall():
                    pts_by_update[uid] = float(pts) if pts is not None else 0
                points_row = [pts_by_update[uid] for uid in update_ids]
                return {"columns": ["player"] + ts_headers, "rows": [[player_name] + points_row], "country": country}


def fetch_top_players(leaderboard: str, limit: int = 50) -> List[str]:
    driver, _ = resolve_db_config()
    if driver == "sqlite":
        with open_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM leaderboards WHERE name = ?", (leaderboard,))
            row = cur.fetchone()
            if not row:
                return []
            lb_id = row[0]
            cur.execute(
                """
                WITH daily AS (
                  SELECT date(ub.ts) AS d, MAX(ub.id) AS update_id
                  FROM update_batch ub
                  JOIN facts f ON f.update_id = ub.id
                  WHERE f.leaderboard_id = ?
                    AND time(ub.ts) < '21:00:00'
                  GROUP BY date(ub.ts)
                )
                SELECT pl.name, SUM(f.points) AS total
                FROM facts f
                JOIN daily d     ON d.update_id = f.update_id
                JOIN players pl  ON pl.id = f.player_id
                WHERE f.leaderboard_id = ?
                GROUP BY pl.name
                ORDER BY total DESC
                LIMIT ?
                """,
                (lb_id, lb_id, limit),
            )
            rows = cur.fetchall()
            return [r[0] for r in rows]
    else:
        with open_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM leaderboards WHERE name = %s", (leaderboard,))
                row = cur.fetchone()
                if not row:
                    return []
                lb_id = row[0]
                cur.execute(
                    """
                    WITH daily AS (
                      SELECT date_trunc('day', ub.ts) AS d, MAX(ub.id) AS update_id
                      FROM update_batch ub
                      JOIN facts f ON f.update_id = ub.id
                      WHERE f.leaderboard_id = %s
                        AND (ub.ts::time) < TIME '8:00:00'
                      GROUP BY date_trunc('day', ub.ts)
                    )
                    SELECT pl.name, SUM(f.points) AS total
                    FROM facts f
                    JOIN daily d    ON d.update_id = f.update_id
                    JOIN players pl ON pl.id = f.player_id
                    WHERE f.leaderboard_id = %s
                    GROUP BY pl.name
                    ORDER BY total DESC
                    LIMIT %s
                    """,
                    (lb_id, lb_id, limit),
                )
                rows = cur.fetchall()
                return [r[0] for r in rows]



app = Flask(__name__)
CORS(app)


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/tables")
def api_tables():
    try:
        prefix = request.args.get("prefix", "PLO")
        return jsonify(list_tables(prefix=prefix))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/tables/<leaderboard>/data")
def api_table_data(leaderboard: str):
    
    try: 
        # Defaults: last_columns=10, limit=15
        limit = int(request.args.get("limit", 15))
        last_cols = int(request.args.get("columns", 10))
        
        payload = fetch_table_data(leaderboard, limit=limit, last_columns=last_cols)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/tables/<table>/player")
def api_table_player(table: str):
    try:
        name = request.args.get("name")
        if not name:
            return jsonify({"error": "Missing required query parameter 'name'"}), 400

        payload = fetch_player_data(table, player_name=name)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/tables/<table>/top-players")
def api_top_players(table: str):
    try:
        limit = int(request.args.get("limit", 50))
        player_names = fetch_top_players(table, limit=limit)
        return jsonify(player_names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/tables")
def tables():
    try:
        return 'tables here'
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    debug_env = (os.getenv("FLASK_DEBUG") or os.getenv("DEBUG") or "").lower()
    debug = debug_env in ("1", "true", "yes", "on", "dev", "development")
    app.run(host="0.0.0.0", port=port, debug=debug)


