# Remote PostgreSQL Database Viewer

This tool connects to your Railway PostgreSQL database and provides an interactive way to explore your GGPoker leaderboard data.

## Features

- 🔌 **Remote Connection**: Connects directly to your Railway PostgreSQL database
- 📊 **Table Overview**: Shows all tables with row counts and column counts
- 🗂️ **Structure Analysis**: Displays detailed table structure (column names, types, constraints)
- 📈 **Sample Data**: Shows the last 10 rows from each table (ordered by most recent)
- 🎯 **Smart Ordering**: Automatically detects timestamp/ID columns for proper ordering
- 🖥️ **Interactive Interface**: Easy-to-use menu system for exploring your database

## Setup

1. **Install Dependencies** (if not already installed):
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Environment Variable**:
   - Copy `env_template.txt` to `.env`
   - Add your Railway PostgreSQL connection string:
   ```bash
   DATABASE_URL=postgresql://username:password@hostname:port/database_name
   ```

3. **Get Your Railway Database URL**:
   - Go to your Railway project dashboard
   - Click on your PostgreSQL service
   - Go to "Connect" tab
   - Copy the "Postgres Connection URL"

## Usage

Run the remote database viewer:
```bash
python view_remote_database.py
```

### Menu Options

1. **Show Database Overview**: Displays summary of all tables
2. **View Specific Table**: Explore individual table structure and data
3. **List All Tables**: Simple list of all available tables
4. **Exit**: Close the application

### What You'll See

- **Table Structure**: Column names, data types, nullability, defaults, and positions
- **Sample Data**: Last 10 rows from each table (ordered by most recent timestamp/ID)
- **Statistics**: Row counts, column counts, and table status
- **Formatted Output**: Clean, tabulated display using the `tabulate` library

## Example Output

```
🗄️  DATABASE OVERVIEW
================================================================================
📊 Total tables found: 3

┌─────────────────────┬────────────┬──────────┬────────┐
│ Table Name          │ Row Count  │ Columns  │ Status │
├─────────────────────┼────────────┼──────────┼────────┤
│ leaderboard_tables  │ 15         │ 5        │ ✅     │
│ player_stats        │ 1,247      │ 8        │ ✅     │
│ tournament_results  │ 892        │ 12       │ ✅     │
└─────────────────────┴────────────┴──────────┴────────┘
```

## Troubleshooting

- **Connection Error**: Verify your `DATABASE_URL` is correct and Railway service is running
- **Missing psycopg2**: Run `pip install psycopg2-binary`
- **Permission Issues**: Ensure your Railway database user has proper access rights

## Security Notes

- Never commit your `.env` file to version control
- The `DATABASE_URL` contains sensitive credentials
- Use Railway's built-in security features for production deployments
