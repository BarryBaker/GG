#!/usr/bin/env python3
"""
Simple script to view the GGPoker leaderboard database contents
"""

import sqlite3
from tabulate import tabulate

def view_database(db_path="ggpoker_leaderboards.db"):
    """View the database contents"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print(f"📊 Database: {db_path}")
        print(f"📋 Found {len(tables)} tables:\n")
        
        for table in tables:
            table_name = table[0]
            print(f"🔍 Table: {table_name}")
            
            # Get table info
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns = cursor.fetchall()
            
            print(f"  📝 Columns: {len(columns)}")
            for col in columns:
                
                cid, col_name, col_type, not_null, default_val, pk = col
                print(f"    - {col_name} ({col_type})")
            
            # Get sample data
            try:
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                all_columns = [col[1] for col in cursor.fetchall()]
                if len(all_columns) > 5:
                    display_columns = [all_columns[0]] + all_columns[-5:]
                else:
                    display_columns = all_columns
                col_str = ", ".join([f'"{col}"' for col in display_columns])
                cursor.execute(f"SELECT {col_str} FROM '{table_name}' LIMIT 5")
                rows = cursor.fetchall()
                # INSERT_YOUR_CODE
                # Sort by the last column in descending order
                if display_columns:
                    last_col = display_columns[-1]
                    cursor.execute(f"SELECT {col_str} FROM '{table_name}' ORDER BY CAST(\"{last_col}\" AS REAL) DESC LIMIT 10")
                    rows = cursor.fetchall()
                if rows:
                    print(f"  📊 Sample data ({len(rows)} rows):")
                    # Get column names for display
                    col_names = [col[1] for col in columns]
                    
                    # Format the data for tabulate
                    formatted_rows = []
                    for row in rows:
                        formatted_row = []
                        for i, val in enumerate(row):
                            if val is None:
                                formatted_row.append("NULL")
                            else:
                                formatted_row.append(str(val))
                        formatted_rows.append(formatted_row)
                    
                    print(tabulate(formatted_rows, headers=col_names, tablefmt="grid"))
                else:
                    print("  📊 No data in table")
                    
            except Exception as e:
                print(f"  ❌ Error reading table data: {e}")
            
            print()  # Empty line between tables
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error viewing database: {e}")

if __name__ == "__main__":
    view_database()

