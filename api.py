#!/usr/bin/env python3
"""
FastAPI backend to expose a simple API and serve the Vue frontend.

Endpoints:
- GET /api/tables: list public tables
- GET /api/preview: per-table preview of first column + last 10 columns, sorted by last column numerically desc (NULLS LAST), top 10 rows
- GET /api/last_update: timestamp marker to detect updates (based on leaderboard_tables.last_updated if present; otherwise current time)

Static:
- Serves compiled Vue app from frontend/dist
"""

import os
import sys
from typing import List, Dict, Any
from urllib.parse import urlparse

import psycopg2
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles


def get_pg_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url or not (database_url.startswith("postgres://") or database_url.startswith("postgresql://")):
        raise RuntimeError("DATABASE_URL must be set to a PostgreSQL connection string")
    url = urlparse(database_url)
    conn = psycopg2.connect(
        host=url.hostname,
        port=url.port or 5432,
        database=url.path[1:] if url.path else "railway",
        user=url.username,
        password=url.password,
    )
    return conn


def list_public_tables(connection) -> List[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
        """
    )
    tables = [r[0] for r in cursor.fetchall()]
    cursor.close()
    return tables


def get_columns_for_table(connection, table_name: str) -> List[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    cols = [r[0] for r in cursor.fetchall()]
    cursor.close()
    return cols


def build_preview_for_table(connection, table_name: str, last_columns_count: int = 10, row_limit: int = 10) -> Dict[str, Any]:
    column_names = get_columns_for_table(connection, table_name)
    if not column_names:
        return {"name": table_name, "columns": [], "rows": []}

    # determine selected columns: first + last N (deduped)
    selected_columns: List[str] = []
    selected_columns.append(column_names[0])
    if len(column_names) > 1:
        tail = column_names[-last_columns_count:] if len(column_names) > last_columns_count else column_names[1:]
        for c in tail:
            if c not in selected_columns:
                selected_columns.append(c)

    # order by the true last column numerically, regardless of selection
    order_column = column_names[-1]

    select_clause = ", ".join([f'"{c}"' for c in selected_columns])
    # strip non-numeric chars then cast
    query = (
        f"SELECT {select_clause} FROM \"{table_name}\" "
        f"ORDER BY CAST(NULLIF(regexp_replace(\"{order_column}\", '[^0-9.-]', '', 'g'), '') AS double precision) DESC NULLS LAST "
        f"LIMIT %s"
    )

    cursor = connection.cursor()
    cursor.execute(query, (row_limit,))
    rows = cursor.fetchall()
    cursor.close()

    # Convert to JSON-serializable (leave as-is; psycopg returns Python types already)
    serializable_rows: List[List[Any]] = []
    for row in rows:
        serializable_rows.append([None if v is None else v for v in row])

    return {"name": table_name, "columns": selected_columns, "rows": serializable_rows}


app = FastAPI()

# Basic CORS (same-origin by default; allow all for simplicity during dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/tables")
def api_tables():
    try:
        conn = get_pg_connection()
        tables = list_public_tables(conn)
        conn.close()
        return {"tables": tables}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/preview")
def api_preview():
    try:
        conn = get_pg_connection()
        tables = list_public_tables(conn)
        previews: List[Dict[str, Any]] = []
        for t in tables:
            try:
                previews.append(build_preview_for_table(conn, t, last_columns_count=10, row_limit=10))
            except Exception:
                # Skip problematic tables but keep others
                continue
        conn.close()
        return {"previews": previews}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/last_update")
def api_last_update():
    """Return a marker that changes when table data changes.

    Strategy: for each public table, compute MAX(numeric(last_column)) and hash the tuple.
    This avoids relying on a separate tracking table.
    """
    try:
        import hashlib
        conn = get_pg_connection()
        tables = list_public_tables(conn)
        stats: List[str] = []
        for t in tables:
            try:
                cols = get_columns_for_table(conn, t)
                if not cols:
                    continue
                last_col = cols[-1]
                cur = conn.cursor()
                cur.execute(
                    f"SELECT MAX(CAST(NULLIF(regexp_replace(\"{last_col}\", '[^0-9.-]', '', 'g'), '') AS double precision)) FROM \"{t}\""
                )
                max_val = cur.fetchone()[0]
                cur.close()
                stats.append(f"{t}:{max_val}")
            except Exception:
                continue
        conn.close()
        if not stats:
            return {"last_update": "0"}
        digest = hashlib.sha1("|".join(sorted(stats)).encode()).hexdigest()
        return {"last_update": digest}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# Serve static frontend if built
frontend_dist_path = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.isdir(frontend_dist_path):
    app.mount("/", StaticFiles(directory=frontend_dist_path, html=True), name="static")


