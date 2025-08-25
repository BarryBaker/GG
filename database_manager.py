#!/usr/bin/env python3
"""
Hybrid Database Manager for GGPoker Leaderboard Tracking
Uses SQLite locally, PostgreSQL in cloud
"""

import os
import datetime
import re
from typing import List, Tuple, Optional

class DatabaseManager:
    def __init__(self, db_path: str = None):
        """
        Initialize database manager - auto-detects environment
        """
        # Check if we're in cloud (Railway/Render) or local
        self.database_url = os.getenv('DATABASE_URL')
        
        if self.database_url and (self.database_url.startswith('postgres://') or self.database_url.startswith('postgresql://')):
            # Cloud environment - use PostgreSQL
            self.use_postgres = True
            self.init_postgres()
            print("�� Using PostgreSQL (cloud)")
        else:
            # Local environment - use SQLite
            self.use_postgres = False
            self.db_path = db_path or os.getenv("DB_PATH", "ggpoker_leaderboards.db")
            self.init_sqlite()
            print("💻 Using SQLite (local)")
        
    
    def init_postgres(self):
        """Initialize PostgreSQL connection"""
        try:
            import psycopg2
            from urllib.parse import urlparse
            
            # Parse the DATABASE_URL
            url = urlparse(self.database_url)
            
            # Connect to PostgreSQL
            self.connection = psycopg2.connect(
                host=url.hostname,
                port=url.port or 5432,
                database=url.path[1:],  # Remove leading slash
                user=url.username,
                password=url.password
            )
            
            # Create tables if they don't exist
            self.create_postgres_tables()
            
        except ImportError:
            print("❌ psycopg2 not installed. Install with: pip install psycopg2-binary")
            raise
        except Exception as e:
            print(f"❌ Error connecting to PostgreSQL: {e}")
            raise
    
    def init_sqlite(self):
        """Initialize SQLite connection (your existing code)"""
        try:
            import sqlite3
            self.connection = sqlite3.connect(self.db_path)
            self.connection.execute("PRAGMA foreign_keys = ON")
            self.create_sqlite_tables()
        except Exception as e:
            print(f"❌ Error initializing SQLite: {e}")
            raise
    
    def create_postgres_tables(self):
        """Create PostgreSQL tables"""
        try:
            cursor = self.connection.cursor()
            
            

            # Normalized schema (new)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leaderboards (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    country TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS update_batch (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS facts (
                    leaderboard_id INT NOT NULL REFERENCES leaderboards(id) ON DELETE CASCADE,
                    update_id BIGINT NOT NULL REFERENCES update_batch(id) ON DELETE CASCADE,
                    player_id INT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                    points NUMERIC NOT NULL,
                    PRIMARY KEY (leaderboard_id, player_id, update_id)
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_lb_update ON facts (leaderboard_id, update_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_lb_player_update ON facts (leaderboard_id, player_id, update_id)")
            
            self.connection.commit()
            print("✅ PostgreSQL tables created")
            
        except Exception as e:
            print(f"❌ Error creating PostgreSQL tables: {e}")
            raise
    
    def create_sqlite_tables(self):
        """Create SQLite tables (your existing code)"""
        try:
            cursor = self.connection.cursor()
          
            
            # Normalized schema (new)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leaderboards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    country TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS update_batch (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS facts (
                    leaderboard_id INTEGER NOT NULL,
                    update_id INTEGER NOT NULL,
                    player_id INTEGER NOT NULL,
                    points REAL NOT NULL,
                    PRIMARY KEY (leaderboard_id, player_id, update_id),
                    FOREIGN KEY (leaderboard_id) REFERENCES leaderboards(id) ON DELETE CASCADE,
                    FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE,
                    FOREIGN KEY (update_id) REFERENCES update_batch(id) ON DELETE CASCADE
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_lb_update ON facts (leaderboard_id, update_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_facts_lb_player_update ON facts (leaderboard_id, player_id, update_id)")
            
            self.connection.commit()
            print("✅ SQLite tables created")
        except Exception as e:
            print(f"❌ Error creating SQLite tables: {e}")
            raise

    # ---- Normalized helpers (generic wrappers) ----
    def get_or_create_leaderboard_id(self, name: str) -> int:
        if self.use_postgres:
            return self._get_or_create_leaderboard_id_pg(name)
        # SQLite path (existing behavior)
        cursor = self.connection.cursor()
        cursor.execute("SELECT id FROM leaderboards WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute("INSERT INTO leaderboards (name) VALUES (?)", (name,))
        self.connection.commit()
        return cursor.lastrowid

    def get_or_create_player_id(self, name: str, country: Optional[str]) -> int:
        if self.use_postgres:
            return self._get_or_create_player_id_pg(name, country)
        # SQLite path
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, country FROM players WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row:
            player_id, existing_country = row
            if (not existing_country) and country:
                cursor.execute("UPDATE players SET country = ? WHERE id = ?", (country, player_id))
                self.connection.commit()
            return player_id
        cursor.execute("INSERT INTO players (name, country) VALUES (?, ?)", (name, country))
        self.connection.commit()
        return cursor.lastrowid

    def create_update_batch(self, ts: str) -> int:
        if self.use_postgres:
            return self._create_update_batch_pg(ts)
        # SQLite path
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO update_batch (ts) VALUES (?)", (ts,))
        self.connection.commit()
        return cursor.lastrowid

    def insert_fact(self, leaderboard_id: int, update_id: int, player_id: int, points: float):
        if self.use_postgres:
            return self._insert_fact_pg(leaderboard_id, update_id, player_id, points)
        # SQLite path
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO facts (leaderboard_id, update_id, player_id, points)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(leaderboard_id, player_id, update_id)
            DO UPDATE SET points = excluded.points
            """,
            (leaderboard_id, update_id, player_id, points),
        )
        self.connection.commit()
    
    def _get_or_create_leaderboard_id_pg(self, name: str) -> int:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO leaderboards (name)
            VALUES (%s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (name,),
        )
        lb_id = cursor.fetchone()[0]
        self.connection.commit()
        return lb_id

    def _get_or_create_player_id_pg(self, name: str, country: Optional[str]) -> int:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO players (name, country)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET country = COALESCE(players.country, EXCLUDED.country)
            RETURNING id
            """,
            (name, country),
        )
        player_id = cursor.fetchone()[0]
        self.connection.commit()
        return player_id

    def _create_update_batch_pg(self, ts: str) -> int:
        cursor = self.connection.cursor()
        cursor.execute(
            "INSERT INTO update_batch (ts) VALUES (%s) RETURNING id",
            (ts,),
        )
        update_id = cursor.fetchone()[0]
        self.connection.commit()
        return update_id

    def _insert_fact_pg(self, leaderboard_id: int, update_id: int, player_id: int, points: float):
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO facts (leaderboard_id, update_id, player_id, points)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (leaderboard_id, player_id, update_id)
            DO UPDATE SET points = EXCLUDED.points
            """,
            (leaderboard_id, update_id, player_id, points),
        )
        self.connection.commit()
    
    def get_leaderboard_table_name(self, game: str, blind_level: str) -> str:
        """Get or create leaderboard table for game/blind level"""
        table_name = f"{game} - {blind_level}"
        return self._sanitize_table_name(table_name)
        
        
    
   
    # --- Name sanitizers (restored) ---
    def _sanitize_table_name(self, name: str) -> str:
        """Sanitize table name for SQLite/PostgreSQL compatibility"""
        sanitized = re.sub(r'[^a-zA-Z0-9_\s-]', '', name)
        sanitized = sanitized.replace(' ', '_').replace('-', '_')
        sanitized = sanitized.replace('$', 'dollar').replace('.', 'dot')
        return sanitized

    def _sanitize_column_name(self, timestamp: str) -> str:
        """Sanitize timestamp for use as a column name"""
        sanitized = timestamp.replace(' ', '_').replace(':', 'h').replace('-', '_')
        sanitized = f"ts_{sanitized}"
        return sanitized

    # --- Timestamp pruning helpers ---
    def _get_sqlite_timestamp_columns(self, table_name: str) -> List[str]:
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        all_columns = [col[1] for col in cursor.fetchall()]
        ts_columns = [c for c in all_columns if c.startswith("ts_")]
        ts_columns.sort()
        return ts_columns

    def _prune_sqlite_timestamp_columns(self, table_name: str, max_columns: int):
        try:
            ts_columns = self._get_sqlite_timestamp_columns(table_name)
            if len(ts_columns) <= max_columns:
                return
            to_drop = len(ts_columns) - max_columns
            columns_to_drop = ts_columns[:to_drop]
            cursor = self.connection.cursor()
            for col in columns_to_drop:
                try:
                    cursor.execute(f"ALTER TABLE \"{table_name}\" DROP COLUMN \"{col}\"")
                    print(f"    🗑️ Dropped old column: {col}")
                except Exception as drop_err:
                    print(f"    ⚠️ Could not drop column {col}: {drop_err}")
                    break
            self.connection.commit()
        except Exception as e:
            print(f"    ⚠️ Error during pruning timestamp columns (SQLite): {e}")

    def _get_postgres_timestamp_columns(self, table_name: str) -> List[str]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s AND column_name LIKE 'ts_%'
            ORDER BY column_name ASC
            """,
            (table_name,),
        )
        rows = cursor.fetchall()
        return [r[0] for r in rows]

    def _prune_postgres_timestamp_columns(self, table_name: str, max_columns: int):
        try:
            ts_columns = self._get_postgres_timestamp_columns(table_name)
            if len(ts_columns) <= max_columns:
                return
            to_drop = len(ts_columns) - max_columns
            columns_to_drop = ts_columns[:to_drop]
            cursor = self.connection.cursor()
            for col in columns_to_drop:
                try:
                    cursor.execute(f"ALTER TABLE \"{table_name}\" DROP COLUMN \"{col}\"")
                    print(f"    🗑️ Dropped old column: {col}")
                except Exception as drop_err:
                    print(f"    ⚠️ Could not drop column {col}: {drop_err}")
                    break
            self.connection.commit()
        except Exception as e:
            print(f"    ⚠️ Error during pruning timestamp columns (PostgreSQL): {e}")
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("🔒 Database connection closed")



