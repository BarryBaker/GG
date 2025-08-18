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
        
        # Optional cap on timestamp columns
        try:
            self.max_ts_columns = int(os.getenv("MAX_TS_COLUMNS", "0"))
        except ValueError:
            self.max_ts_columns = 0
    
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
            
            # Create leaderboard_tables tracking table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leaderboard_tables (
                    table_name TEXT PRIMARY KEY,
                    game_type TEXT NOT NULL,
                    blind_level TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.connection.commit()
            print("✅ PostgreSQL tables created")
            
        except Exception as e:
            print(f"❌ Error creating PostgreSQL tables: {e}")
            raise
    
    def create_sqlite_tables(self):
        """Create SQLite tables (your existing code)"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leaderboard_tables (
                    table_name TEXT PRIMARY KEY,
                    game_type TEXT NOT NULL,
                    blind_level TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.connection.commit()
            print("✅ SQLite tables created")
        except Exception as e:
            print(f"❌ Error creating SQLite tables: {e}")
            raise
    
    def get_or_create_leaderboard_table(self, game: str, blind_level: str) -> str:
        """Get or create leaderboard table for game/blind level"""
        table_name = f"{game} - {blind_level}"
        safe_table_name = self._sanitize_table_name(table_name)
        
        try:
            if self.use_postgres:
                return self._get_or_create_postgres_table(safe_table_name, game, blind_level)
            else:
                return self._get_or_create_sqlite_table(safe_table_name, game, blind_level)
        except Exception as e:
            print(f"❌ Error getting/creating table: {e}")
            raise
    
    def _get_or_create_postgres_table(self, table_name: str, game: str, blind_level: str) -> str:
        """PostgreSQL table creation"""
        cursor = self.connection.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name = %s
        """, (table_name,))
        
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # Create new table
            cursor.execute(f"""
                CREATE TABLE "{table_name}" (
                    player TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            print(f"  🆕 Created new PostgreSQL table: {table_name}")
        else:
            print(f"  📋 Using existing PostgreSQL table: {table_name}")
        
        # Update tracking record
        cursor.execute("""
            INSERT INTO leaderboard_tables (table_name, game_type, blind_level, last_updated) 
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) DO UPDATE SET last_updated = CURRENT_TIMESTAMP
        """, (table_name, game, blind_level))
        
        self.connection.commit()
        return table_name
    
    def _get_or_create_sqlite_table(self, table_name: str, game: str, blind_level: str) -> str:
        """SQLite table creation (your existing code)"""
        cursor = self.connection.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (table_name,))
        
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # Create new table
            cursor.execute(f"""
                CREATE TABLE "{table_name}" (
                    player TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            print(f"  �� Created new SQLite table: {table_name}")
        else:
            print(f"  �� Using existing SQLite table: {table_name}")
        
        # Update tracking record
        cursor.execute("""
            INSERT OR REPLACE INTO leaderboard_tables 
            (table_name, game_type, blind_level, last_updated) 
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, (table_name, game, blind_level))
        
        self.connection.commit()
        return table_name
    
    def add_timestamp_column(self, table_name: str, timestamp: str) -> bool:
        """Add timestamp column to table"""
        try:
            safe_column_name = self._sanitize_column_name(timestamp)
            
            if self.use_postgres:
                return self._add_postgres_timestamp_column(table_name, safe_column_name)
            else:
                return self._add_sqlite_timestamp_column(table_name, safe_column_name)
        except Exception as e:
            print(f"❌ Error adding timestamp column: {e}")
            return False
    
    def _add_postgres_timestamp_column(self, table_name: str, column_name: str) -> bool:
        """Add timestamp column to PostgreSQL table"""
        cursor = self.connection.cursor()
        
        # Check if column exists
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = %s AND column_name = %s
        """, (table_name, column_name))
        
        column_exists = cursor.fetchone() is not None
        
        if column_exists:
            print(f"    ⚠️ Column {column_name} already exists in {table_name}")
            return True
        
        # Add new column
        cursor.execute(f"""
            ALTER TABLE "{table_name}" 
            ADD COLUMN "{column_name}" TEXT DEFAULT '0'
        """)
        
        self.connection.commit()
        print(f"    ✅ Added new PostgreSQL column: {column_name}")
        
        # Prune old columns if needed
        if self.max_ts_columns and self.max_ts_columns > 0:
            self._prune_postgres_timestamp_columns(table_name, self.max_ts_columns)
        
        return True
    
    def _add_sqlite_timestamp_column(self, table_name: str, column_name: str) -> bool:
        """Add timestamp column to SQLite table (your existing code)"""
        cursor = self.connection.cursor()
        
        # Check if column exists
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        columns = [col[1] for col in cursor.fetchall()]
        
        if column_name in columns:
            print(f"    ⚠️ Column {column_name} already exists in {table_name}")
            return True
        
        # Add new column
        cursor.execute(f"""
            ALTER TABLE "{table_name}" 
            ADD COLUMN "{column_name}" TEXT DEFAULT '0'
        """)
        
        self.connection.commit()
        print(f"    ✅ Added new SQLite column: {column_name}")
        
        # Prune old columns if needed
        if self.max_ts_columns and self.max_ts_columns > 0:
            self._prune_sqlite_timestamp_columns(table_name, self.max_ts_columns)
        
        return True
    
    def update_player_points(self, table_name: str, player: str, timestamp: str, points: str):
        """Update player points for timestamp"""
        try:
            safe_column_name = self._sanitize_column_name(timestamp)
            
            if self.use_postgres:
                self._update_postgres_player_points(table_name, player, safe_column_name, points)
            else:
                
                self._update_sqlite_player_points(table_name, player, safe_column_name, points)
        except Exception as e:
            print(f"❌ Error updating player points: {e}")
    
    def _update_postgres_player_points(self, table_name: str, player: str, column_name: str, points: str):
        """Update player points in PostgreSQL"""
        cursor = self.connection.cursor()
        
        # Check if player exists
        cursor.execute(f"""
            SELECT player FROM "{table_name}" WHERE player = %s
        """, (player,))
        
        player_exists = cursor.fetchone() is not None
        
        if player_exists:
            # Update existing player
            cursor.execute(f"""
                UPDATE "{table_name}" 
                SET "{column_name}" = %s 
                WHERE player = %s
            """, (points, player))
        else:
            # Create new player record
            cursor.execute(f"""
                INSERT INTO "{table_name}" (player, "{column_name}")
                VALUES (%s, %s)
            """, (player, points))
        
        self.connection.commit()
    
    def _update_sqlite_player_points(self, table_name: str, player: str, column_name: str, points: str):
        """Update player points in SQLite (your existing code)"""
        cursor = self.connection.cursor()
        
        # Check if player exists
        cursor.execute(f"""
            SELECT player FROM "{table_name}" WHERE player = ?
        """, (player,))
        
        player_exists = cursor.fetchone() is not None
        
        if player_exists:
            # Update existing player
          
            cursor.execute(f"""
                UPDATE "{table_name}" 
                SET "{column_name}" = ? 
                WHERE player = ?
            """, (points, player))
           
        else:
            # Create new player record
            cursor.execute(f"""
                INSERT INTO "{table_name}" (player, "{column_name}")
                VALUES (?, ?)
            """, (player, points))
        
        self.connection.commit()
    
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