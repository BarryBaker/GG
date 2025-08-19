#!/usr/bin/env python3
"""
Remote PostgreSQL Database Viewer for GGPoker Leaderboards
Connects to Railway PostgreSQL and displays tables with last 10 columns
"""

import os
import sys
from typing import List, Tuple, Optional
from datetime import datetime
import psycopg2
from urllib.parse import urlparse
from tabulate import tabulate
from dotenv import load_dotenv

class RemoteDatabaseViewer:
    def __init__(self):
        """Initialize connection to remote PostgreSQL database"""
        # Load environment variables from .env file
        load_dotenv()
        
        self.database_url = os.getenv('DATABASE_URL')
        
        if not self.database_url:
            print("❌ DATABASE_URL environment variable not found!")
            print("Please set your Railway PostgreSQL connection string in DATABASE_URL")
            sys.exit(1)
            
        if not (self.database_url.startswith('postgres://') or self.database_url.startswith('postgresql://')):
            print("❌ DATABASE_URL must be a PostgreSQL connection string!")
            sys.exit(1)
            
        self.connect_to_database()
    
    def connect_to_database(self):
        """Establish connection to remote PostgreSQL database"""
        try:
            # Parse the DATABASE_URL
            url = urlparse(self.database_url)
            
            # Debug: Print parsed URL components
            print(f"🔍 Parsed URL components:")
            print(f"   Scheme: {url.scheme}")
            print(f"   Username: {url.username}")
            print(f"   Password: {'*' * len(url.password) if url.password else 'None'}")
            print(f"   Hostname: {url.hostname}")
            print(f"   Port: {url.port}")
            print(f"   Path: {url.path}")
            print(f"   Database name: {url.path[1:] if url.path else 'railway'}")
            print()
            
            # Connect to PostgreSQL
            self.connection = psycopg2.connect(
                host=url.hostname,
                port=url.port or 5432,
                database=url.path[1:] if url.path else 'railway',  # Remove leading slash or default to 'railway'
                user=url.username,
                password=url.password
            )
            
            print(f"✅ Connected to PostgreSQL database: {url.path[1:] if url.path else 'railway'}")
            print(f"📍 Host: {url.hostname}:{url.port or 5432}")
            print(f"👤 User: {url.username}")
            print()
            
        except ImportError:
            print("❌ psycopg2 not installed. Install with: pip install psycopg2-binary")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error connecting to PostgreSQL: {e}")
            sys.exit(1)
    
    def get_all_tables(self) -> List[str]:
        """Get all table names from the database"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return tables
            
        except Exception as e:
            print(f"❌ Error getting tables: {e}")
            return []
    
    def get_table_structure(self, table_name: str) -> List[Tuple]:
        """Get table structure with column information"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = cursor.fetchall()
            cursor.close()
            return columns
            
        except Exception as e:
            print(f"❌ Error getting structure for table {table_name}: {e}")
            return []
    
    def get_table_data(self, table_name: str, limit: int = 5) -> Tuple[List[str], List[List]]:
        """Get sample data from table (last 10 rows)"""
        try:
            cursor = self.connection.cursor()
            
            # First get the column names
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            column_names = [row[0] for row in cursor.fetchall()]
          
            if not column_names:
                return [], []
            
            # Get the last 10 rows (assuming there's some ordering column)
            # Try to find a timestamp or ID column for ordering
            # Use the last column (assumed to be a ts column with the latest timestamp) as the order column,
            # and order by its numeric value (even if stored as string)
            order_column = None
            if column_names:
                order_column = column_names[-1]

            # Determine which columns to select: first column and last 4 columns (no duplicates)
            selected_columns = []
            if column_names:
                selected_columns.append(column_names[0])
                # Add last 4 columns, skipping the first if it's already included
                num_last_columns = 5  # You can set this to any number before this statement
                last_cols = column_names[-num_last_columns:] if len(column_names) > num_last_columns else column_names[1:]
                for col in last_cols:
                    if col not in selected_columns:
                        selected_columns.append(col)
           
            # Build the SELECT clause
            select_clause = ", ".join([f'"{col}"' for col in selected_columns])

            if order_column:
                # Remove commas before casting to REAL for correct numeric sorting (e.g., "1,181.00" -> 1181.00)
                query = f"SELECT {select_clause} FROM \"{table_name}\" ORDER BY CAST(REPLACE(\"{order_column}\", ',', '') AS REAL) DESC LIMIT %s"
            else:
                # If no obvious ordering column, just get last 10 rows
                query = f"SELECT {select_clause} FROM \"{table_name}\" LIMIT %s"
            
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
            cursor.close()
            
            return selected_columns, rows
            
        except Exception as e:
            print(f"❌ Error getting data from table {table_name}: {e}")
            return [], []
    
    def display_table_info(self, table_name: str):
        """Display comprehensive information about a table"""
        print(f"\n{'='*80}")
        print(f"📊 TABLE: {table_name.upper()}")
        print(f"{'='*80}")
        
        # Get table structure
        columns = self.get_table_structure(table_name)
        
        if not columns:
            print("❌ No columns found or error occurred")
            return
        
        print(f"\n📋 TABLE STRUCTURE ({len(columns)} columns):")
        print("-" * 80)
        
        # Display column information
        column_info = []
        for col in columns:
            column_info.append([
                col[0],  # column_name
                col[1],  # data_type
                "NULL" if col[2] == "YES" else "NOT NULL",
                str(col[3]) if col[3] else "No default",
                col[4]   # ordinal_position
            ])
        
        print(tabulate(column_info, 
                      headers=["Column Name", "Data Type", "Nullable", "Default", "Position"],
                      tablefmt="grid"))
        
        # Get and display sample data (last 10 rows)
        print(f"\n📊 SAMPLE DATA (Last {min(10, len(columns))} rows):")
        print("-" * 80)
        
        column_names, rows = self.get_table_data(table_name, 10)
        
        if rows:
            # Format the data for display
            formatted_rows = []
            for row in rows:
                formatted_row = []
                for i, value in enumerate(row):
                    if value is None:
                        formatted_row.append("NULL")
                    elif isinstance(value, datetime):
                        formatted_row.append(value.strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        # Truncate long values for display
                        str_value = str(value)
                        if len(str_value) > 30:
                            str_value = str_value[:27] + "..."
                        formatted_row.append(str_value)
                formatted_rows.append(formatted_row)
            
            print(tabulate(formatted_rows, headers=column_names, tablefmt="grid"))
        else:
            print("ℹ️  No data found in table")
        
        print(f"\n📈 TABLE STATS:")
        print(f"   • Total columns: {len(columns)}")
        print(f"   • Sample rows shown: {len(rows) if rows else 0}")
        
        # Try to get row count
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\"")
            row_count = cursor.fetchone()[0]
            print(f"   • Total rows: {row_count:,}")
            cursor.close()
        except:
            print("   • Total rows: Unable to determine")
    
    def display_database_overview(self):
        """Display overview of all tables in the database"""
        tables = self.get_all_tables()
        
        if not tables:
            print("❌ No tables found in database")
            return
        
        print(f"\n🗄️  DATABASE OVERVIEW")
        print(f"{'='*80}")
        print(f"📊 Total tables found: {len(tables)}")
        print()
        
        # Display table summary
        table_summary = []
        for table_name in tables:
            
            try:
                cursor = self.connection.cursor()
                
                cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\"")
                row_count = cursor.fetchone()[0]
                cursor.close()
                
                # Get column count
                columns = self.get_table_structure(table_name)
                column_count = len(columns)
                
                table_summary.append([
                    table_name,
                    f"{row_count:,}",
                    column_count,
                    "✅" if row_count > 0 else "⚠️"
                ])
                
            except Exception as e:
                table_summary.append([table_name, "Error", "Error", "❌"])
        
        print(tabulate(table_summary, 
                      headers=["Table Name", "Row Count", "Columns", "Status"],
                      tablefmt="grid"))
    
    def run_interactive_viewer(self):
        """Run interactive database viewer"""
        while True:
            print(f"\n{'='*80}")
            print("🔍 REMOTE POSTGRESQL DATABASE VIEWER")
            print(f"{'='*80}")
            print("1. Show database overview")
            print("2. View specific table")
            print("3. List all tables")
            print("4. Exit")
            print("-" * 80)
            
            choice = input("Select an option (1-4): ").strip()
            
            if choice == "1":
                self.display_database_overview()
                
            elif choice == "2":
                tables = self.get_all_tables()
                if tables:
                    print(f"\nAvailable tables:")
                    for i, table in enumerate(tables, 1):
                        print(f"  {i}. {table}")
                    
                    try:
                        table_choice = input(f"\nEnter table number (1-{len(tables)}) or table name: ").strip()
                        
                        if table_choice.isdigit():
                            idx = int(table_choice) - 1
                            if 0 <= idx < len(tables):
                                self.display_table_info(tables[idx])
                            else:
                                print("❌ Invalid table number")
                        else:
                            if table_choice in tables:
                                self.display_table_info(table_choice)
                            else:
                                print("❌ Table not found")
                    except ValueError:
                        print("❌ Invalid input")
                else:
                    print("❌ No tables found")
                    
            elif choice == "3":
                tables = self.get_all_tables()
                if tables:
                    print(f"\n📋 ALL TABLES ({len(tables)}):")
                    for i, table in enumerate(tables, 1):
                        print(f"  {i:2d}. {table}")
                else:
                    print("❌ No tables found")
                    
            elif choice == "4":
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please select 1-4.")
    
    def close_connection(self):
        """Close database connection"""
        if hasattr(self, 'connection'):
            self.connection.close()
            print("🔌 Database connection closed")

def main():
    """Main function to run the remote database viewer"""
    try:
        print("🚀 Connecting to remote PostgreSQL database...")
        viewer = RemoteDatabaseViewer()
        
        # Show database overview first
        viewer.display_database_overview()
        
        # Run interactive viewer
        viewer.run_interactive_viewer()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    finally:
        if 'viewer' in locals():
            viewer.close_connection()

if __name__ == "__main__":
    main()
