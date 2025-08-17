#!/usr/bin/env python3
"""
Database Manager for GGPoker Leaderboard Tracking
Handles dynamic table creation and player progress tracking
"""

import sqlite3
import datetime
import re
from typing import List, Tuple, Optional
import os

class DatabaseManager:
    def __init__(self, db_path: str = None):
        """
        Initialize the database manager
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        # Allow overriding via environment variable for cloud/container deployments
        # Env var: DB_PATH (fallback to local file if not provided)
        self.db_path = db_path or os.getenv("DB_PATH", "ggpoker_leaderboards.db")
        # Optional cap on number of timestamp columns per table
        # Env var: MAX_TS_COLUMNS (0 or missing means unlimited)
        try:
            self.max_ts_columns = int(os.getenv("MAX_TS_COLUMNS", "0"))
        except ValueError:
            self.max_ts_columns = 0
        self.connection = None
        self.init_database()
    
    def init_database(self):
        """Initialize the database and create necessary tables"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.execute("PRAGMA foreign_keys = ON")
            
            # Create a table to track all our dynamic tables
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS leaderboard_tables (
                    table_name TEXT PRIMARY KEY,
                    game_type TEXT NOT NULL,
                    blind_level TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.connection.commit()
            print(f"✅ Database initialized: {self.db_path}")
            
        except Exception as e:
            print(f"❌ Error initializing database: {e}")
            raise
    
    def get_or_create_leaderboard_table(self, game: str, blind_level: str) -> str:
        """
        Get or create a leaderboard table for a specific game and blind level
        
        Args:
            game (str): Game type (e.g., 'PLO')
            blind_level (str): Blind level (e.g., '$0.01/$0.02')
            
        Returns:
            str: Table name
        """
        # Create a safe table name
        table_name = f"{game} - {blind_level}"
        safe_table_name = self._sanitize_table_name(table_name)
        
        try:
            # Check if table exists
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (safe_table_name,))
            
            table_exists = cursor.fetchone() is not None
            
            if not table_exists:
                # Create new table
                self._create_leaderboard_table(safe_table_name, game, blind_level)
                print(f"  🆕 Created new table: {safe_table_name}")
            else:
                print(f"  📋 Using existing table: {safe_table_name}")
            
            # Update or insert table tracking record
            cursor.execute("""
                INSERT OR REPLACE INTO leaderboard_tables 
                (table_name, game_type, blind_level, last_updated) 
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (safe_table_name, game, blind_level))
            
            self.connection.commit()
            return safe_table_name
            
        except Exception as e:
            print(f"❌ Error getting/creating table: {e}")
            raise
    
    def _create_leaderboard_table(self, table_name: str, game: str, blind_level: str):
        """Create a new leaderboard table with basic structure"""
        try:
            # Create table with player column and initial timestamp column
            cursor = self.connection.cursor()
            cursor.execute(f"""
                CREATE TABLE "{table_name}" (
                    player TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.connection.commit()
            
        except Exception as e:
            print(f"❌ Error creating table {table_name}: {e}")
            raise
    
    def add_timestamp_column(self, table_name: str, timestamp: str) -> bool:
        """
        Add a new timestamp column to an existing table
        
        Args:
            table_name (str): Name of the table
            timestamp (str): Timestamp string to use as column name
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Sanitize the timestamp for use as a column name
            safe_column_name = self._sanitize_column_name(timestamp)
            
            cursor = self.connection.cursor()
            
            # Check if column already exists
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns = [col[1] for col in cursor.fetchall()]
            
            if safe_column_name in columns:
                print(f"    ⚠️ Column {safe_column_name} already exists in {table_name}")
                # Even if it exists, enforce pruning if enabled
                if self.max_ts_columns and self.max_ts_columns > 0:
                    self._prune_old_timestamp_columns(table_name, self.max_ts_columns)
                return True
            
            # Add new column
            cursor.execute(f"""
                ALTER TABLE "{table_name}" 
                ADD COLUMN "{safe_column_name}" TEXT DEFAULT '0'
            """)
            
            self.connection.commit()
            print(f"    ✅ Added new column: {safe_column_name}")

            # Optionally prune old timestamp columns if we exceed the limit
            if self.max_ts_columns and self.max_ts_columns > 0:
                self._prune_old_timestamp_columns(table_name, self.max_ts_columns)
            return True
            
        except Exception as e:
            print(f"❌ Error adding timestamp column: {e}")
            return False
    
    def update_player_points(self, table_name: str, player: str, timestamp: str, points: str):
        """
        Update or create player record with points for a specific timestamp
        
        Args:
            table_name (str): Name of the table
            player (str): Player name
            timestamp (str): Timestamp string
            points (str): Points value
        """
        try:
            safe_column_name = self._sanitize_column_name(timestamp)
            
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
                    SET "{safe_column_name}" = ? 
                    WHERE player = ?
                """, (points, player))
                # print(f"      📝 Updated {player}: {points}")
            else:
                # Create new player record
                cursor.execute(f"""
                    INSERT INTO "{table_name}" (player, "{safe_column_name}")
                    VALUES (?, ?)
                """, (player, points))
                # print(f"      🆕 Created {player}: {points}")
            
            self.connection.commit()
            
        except Exception as e:
            print(f"❌ Error updating player points: {e}")
    
    def _sanitize_table_name(self, name: str) -> str:
        """Sanitize table name for SQLite compatibility"""
        # Remove or replace problematic characters
        sanitized = re.sub(r'[^a-zA-Z0-9_\s-]', '', name)
        sanitized = sanitized.replace(' ', '_').replace('-', '_')
        sanitized = sanitized.replace('$', 'dollar').replace('.', 'dot')
        return sanitized
    
    def _sanitize_column_name(self, timestamp: str) -> str:
        """Sanitize timestamp for use as column name"""
        # Convert timestamp to safe column name
        sanitized = timestamp.replace(' ', '_').replace(':', 'h').replace('-', '_')
        sanitized = f"ts_{sanitized}"
        return sanitized

    def _get_timestamp_columns(self, table_name: str) -> List[str]:
        """Return a list of timestamp column names (those starting with 'ts_')."""
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        all_columns = [col[1] for col in cursor.fetchall()]
        ts_columns = [c for c in all_columns if c.startswith("ts_")]
        # Sort by lexical order which matches chronological order with our naming scheme
        ts_columns.sort()
        return ts_columns

    def _prune_old_timestamp_columns(self, table_name: str, max_columns: int):
        """
        Ensure the number of timestamp columns does not exceed max_columns.
        Drops the oldest columns first. Best-effort: relies on SQLite >= 3.35 for DROP COLUMN.
        """
        try:
            ts_columns = self._get_timestamp_columns(table_name)
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
                    # If DROP COLUMN is not supported, log and stop pruning to avoid destructive migrations
                    print(f"    ⚠️ Could not drop column {col}: {drop_err}")
                    break
            self.connection.commit()
        except Exception as e:
            print(f"    ⚠️ Error during pruning timestamp columns: {e}")
    
    def get_table_info(self, table_name: str) -> Optional[List[Tuple]]:
        """Get information about a specific table"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            return cursor.fetchall()
        except Exception as e:
            print(f"❌ Error getting table info: {e}")
            return None
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            print("🔒 Database connection closed")

