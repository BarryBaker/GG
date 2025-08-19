#!/usr/bin/env python3
"""
Simple HTTP API to expose PostgreSQL tables for a frontend application.

Endpoints:
  GET  /health                         → { status: ok }
  GET  /tables                         → ["table_a", "table_b", ...]
  GET  /tables/<table>/columns         → [{ name, data_type, is_nullable, default, position }, ...]
  GET  /tables/<table>/data            → { columns: [...], rows: [...] }
      Query params:
        - limit: int (default 50)
        - last_columns: int (default 10) → first column + last N columns (no duplicates)

Notes:
  - Prefers DATABASE_PRIVATE_URL, then DATABASE_URL, then DATABASE_PUBLIC_URL
  - Avoids holding long transactions (autocommit connection per request)
  - Uses NULLS LAST ordering on the last selected column, parsing numbers from strings
  - CORS enabled for browser clients
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


def list_tables() -> List[str]:
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
                """
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

            # ORDER BY: remove non-numeric chars, treat empty as NULL, cast to double, DESC NULLS LAST
            order_expr = sql.SQL(
                "CAST(NULLIF(regexp_replace({col}, '[^0-9.-]', '', 'g'), '') AS double precision) DESC NULLS LAST"
            ).format(col=order_ident)

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
        return jsonify(list_tables())
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
        limit = int(request.args.get("limit", 50))
        last_cols = int(request.args.get("last_columns", 10))
        payload = fetch_table_data(table, limit=limit, last_columns=last_cols)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)


