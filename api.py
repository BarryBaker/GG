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


def resolve_database_url() -> str:
    """Pick the most appropriate database URL from environment variables."""
    load_dotenv()
    for key in ("DATABASE_PRIVATE_URL", "DATABASE_URL", "DATABASE_PUBLIC_URL"):
        value = os.getenv(key)
        if value and (value.startswith("postgres://") or value.startswith("postgresql://")):
            return value
    raise RuntimeError(
        "No PostgreSQL connection string found. Set DATABASE_PRIVATE_URL or DATABASE_URL or DATABASE_PUBLIC_URL."
    )


def open_connection():
    """Open a short-lived autocommit connection to Postgres."""
    db_url = resolve_database_url()
    parsed = urlparse(db_url)
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=(parsed.path[1:] if parsed.path else None),
        user=parsed.username,
        password=parsed.password,
    )
    # Keep read operations from holding transactions/locks
    conn.autocommit = True
    return conn


def list_tables(prefix: str = "PLO") -> List[str]:
    """List public tables whose names start with the given prefix (default 'PLO')."""
    like_pattern = f"{prefix}%"
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name LIKE %s
                ORDER BY table_name
                """,
                (like_pattern,),
            )
            return [r[0] for r in cur.fetchall()]


def list_columns(table_name: str) -> List[Dict[str, str]]:
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable, column_default, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            rows = cur.fetchall()
            return [
                {
                    "name": r[0],
                    "data_type": r[1],
                    "is_nullable": r[2],
                    "default": r[3],
                    "position": r[4],
                }
                for r in rows
            ]


def fetch_table_data(table_name: str, limit: int, last_columns: int) -> Dict[str, List]:
    # Determine columns
    columns_info = list_columns(table_name)
    column_names = [c["name"] for c in columns_info]
    if not column_names:
        return {"columns": [], "rows": []}

    # Build selection: first column + last N columns (unique, ordered)
    selected: List[str] = []
    selected.append(column_names[0])
    tail = column_names[-last_columns:] if len(column_names) > last_columns else column_names[1:]
    for c in tail:
        if c not in selected:
            selected.append(c)

    # Order by the last selected column; try numeric parsing with NULLS LAST
    order_col = selected[-1]

    with open_connection() as conn:
        with conn.cursor() as cur:
            # Build safe SQL with identifiers
            select_idents = [sql.Identifier(c) for c in selected]
            tbl_ident = sql.Identifier(table_name)
            order_ident = sql.Identifier(order_col)

            # ORDER BY: robust numeric sort; non-numeric treated as NULLs → placed last
            cleaned = sql.SQL("regexp_replace({col}, '[^0-9.-]', '', 'g')").format(col=order_ident)
            numeric_when = sql.SQL("({clean}) ~ '^-?\\d*\\.?\\d+$'").format(clean=cleaned)
            # Yes, this code takes into account that the ordering column values are numbers represented as text.
            # It tries to parse the column as a number for sorting, otherwise treats as NULL (sorted last).
            order_expr = sql.SQL(
                "CASE WHEN {when} THEN CAST({clean} AS double precision) ELSE NULL END DESC NULLS LAST"
            ).format(when=numeric_when, clean=cleaned)

            query = sql.SQL("SELECT {cols} FROM {tbl} ORDER BY {ord} LIMIT %s").format(
                cols=sql.SQL(", ").join(select_idents),
                tbl=tbl_ident,
                ord=order_expr,
            )

            cur.execute(query, (limit,))
            rows = cur.fetchall()

    return {"columns": selected, "rows": rows}


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


@app.get("/tables/<table>/columns")
def api_table_columns(table: str):
    try:
        return jsonify(list_columns(table))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/tables/<table>/data")
def api_table_data(table: str):
    try:
        # Defaults: last_columns=10, limit=10
        limit = int(request.args.get("limit", 15))
        last_cols = int(request.args.get("last_columns", 10))
        payload = fetch_table_data(table, limit=limit, last_columns=last_cols)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def fetch_player_data(table_name: str, player_name: str) -> Dict[str, List]:
    """Return all columns for rows matching the player in the first column."""
    # Determine columns and first-column name (assumed to be the player identifier)
    columns_info = list_columns(table_name)
    column_names = [c["name"] for c in columns_info]
    if not column_names:
        return {"columns": [], "rows": []}

    first_col = column_names[0]

    with open_connection() as conn:
        with conn.cursor() as cur:
            select_idents = [sql.Identifier(c) for c in column_names]
            tbl_ident = sql.Identifier(table_name)
            first_ident = sql.Identifier(first_col)

            query = sql.SQL("SELECT {cols} FROM {tbl} WHERE {first} = %s").format(
                cols=sql.SQL(", ").join(select_idents),
                tbl=tbl_ident,
                first=first_ident,
            )

            cur.execute(query, (player_name,))
            rows = cur.fetchall()

    return {"columns": column_names, "rows": rows}


def fetch_top_players(table_name: str, limit: int = 50) -> List[str]:
    """Return list of top player names sorted by sum of all numeric columns (excluding first column)."""
    # Get column info
    columns_info = list_columns(table_name)
    column_names = [c["name"] for c in columns_info]
    if not column_names:
        return []

    first_col = column_names[0]
    # Skip first column (player name) and only sum numeric columns
    numeric_cols = column_names[1:]

    with open_connection() as conn:
        with conn.cursor() as cur:
            # Build SUM expression for all numeric columns
            sum_parts = []
            for col in numeric_cols:
                # Try to cast to numeric, treat non-numeric as 0
                sum_parts.append(
                    sql.SQL("COALESCE(CAST(NULLIF(regexp_replace({col}, '[^0-9.-]', '', 'g'), '') AS double precision), 0)")
                    .format(col=sql.Identifier(col))
                )
            
            # Build the query: SELECT player, SUM(all_numeric_cols) as total
            tbl_ident = sql.Identifier(table_name)
            first_ident = sql.Identifier(first_col)
            
            query = sql.SQL("""
                SELECT {player_col}, SUM({sum_expr}) as total
                FROM {tbl} 
                GROUP BY {player_col}
                ORDER BY total DESC 
                LIMIT %s
            """).format(
                player_col=first_ident,
                sum_expr=sql.SQL(" + ").join(sum_parts),
                tbl=tbl_ident
            )
            
            cur.execute(query, (limit,))
            rows = cur.fetchall()
            
            # Return just the player names
            return [row[0] for row in rows]


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


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)


