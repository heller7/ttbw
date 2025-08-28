# TTBW Database System

This document explains the new SQLite database system for tracking TTBW player changes over time.

## Overview

The database system automatically tracks all changes to player records, including:
- Club changes
- District changes
- Name changes
- Age class updates
- Any other modifications

## Database Structure

### Tables

1. **`current_players`** - Current active player records
   - `interne_lizenznr` (Primary Key) - Internal license number
   - `first_name`, `last_name` - Player names
   - `club`, `district`, `region` - Club and location information
   - `birth_year`, `age_class` - Age-related information
   - `gender` - Player gender
   - `qttr` - QTTR rating
   - `verband` - Federation (default: TTBW)
   - `created_at`, `updated_at` - Timestamps

2. **`player_history`** - Complete history of all changes
   - All player fields from current_players
   - `change_type` - Type of change (INSERT, UPDATE, DELETE)
   - `changed_at` - When the change occurred
   - `previous_club`, `previous_district` - Previous values for tracking changes

### Indexes

- Name-based search: `idx_current_players_name`
- Club-based search: `idx_current_players_club`
- History lookup: `idx_history_lizenznr`

## Usage

### Basic Database Operations

```python
from ttbw_database import TTBWDatabase

# Initialize database with config file
db = TTBWDatabase("ttbw_players.db", "config.yaml")

# Or use default config file name
db = TTBWDatabase("ttbw_players.db")  # defaults to "config.yaml"

# Load players from CSV
players_loaded = db.load_players_from_csv("Spielberechtigungen.csv")

# Find a player
player_id = db.find_player_by_name_and_club("John", "Doe", "TTC Club")

# Get player history
history = db.get_player_history("NU1234567")

# Get all current players
all_players = db.get_all_current_players()

# Get database statistics
stats = db.get_database_stats()
```

### Integration with Main Script

The main `ttbw_compute_ranking.py` script now automatically:
1. Loads players from CSV into the database
2. Tracks all changes over time
3. Uses the database for player matching (including historical club changes)
4. Shows database statistics after processing
5. Generates comprehensive CSV reports

### CSV Reports Generated

The system now generates multiple CSV reports:

1. **Regional Reports** (`region1.csv`, `region2.csv`, etc.):
   - Only age-eligible players with tournament results
   - Sorted by points within each competition

2. **All Players Report** (`all_players.csv`):
   - Complete list of all players across all regions
   - Includes tournament participation count and total points
   - Age-ineligible players marked with asterisk (*)

3. **Unmatched Players Report** (`unmatched_players.csv`):
   - Players who didn't participate in any tournaments
   - Shows reason (too old vs. no participation)

4. **Tournament Unmatched Report** (`tournament_unmatched_players.csv`):
   - Detailed list of tournament results that couldn't be matched
   - Includes possible reasons for matching failures
   - **Club Region Check**: Identifies when clubs are not part of considered regions
   - **Smart Reason Analysis**: Provides specific reasons for matching failures

5. **Fuzzy Matches Report** (`fuzzy_matches.csv`):
   - List of all players matched using fuzzy name matching
   - Shows tournament vs. database name variations
   - Tracks common name variants like "Marc" vs "Mark"
   - Includes old club information for club change tracking
   - Shows both historical and current club data

### Change Tracking

The system automatically detects and records:
- **New players**: INSERT records in history
- **Updated players**: UPDATE records with previous values
- **Club changes**: Previous club stored in history
- **District changes**: Previous district stored in history

### Age Filtering

The system applies age filtering only during tournament result processing:
- **CSV Loading**: ALL players are loaded into the database regardless of age
- **Tournament Processing**: Only age-eligible players are matched and processed
- **Database Reuse**: Same database can be used with different age class configurations
- **Statistics**: Shows count of players eligible vs. too old for tournaments
- **Logging**: Debug logs show which players are filtered out during tournament processing

## Benefits

1. **Persistent Storage**: Player data persists between runs
2. **Change History**: Complete audit trail of all modifications
3. **Better Matching**: Can find players even if they changed clubs
4. **Data Integrity**: Centralized data management
5. **Performance**: Indexed queries for fast lookups
6. **Flexible Age Filtering**: Database stores all players, age filtering applied during tournament processing

## File Structure

- `ttbw_database.py` - Database module
- `ttbw_compute_ranking.py` - Main ranking computation script with database integration
- `test_database.py` - Test script for database functionality
- `ttbw_players.db` - SQLite database file (created automatically)

## Testing

Run the test script to verify database functionality:

```bash
python test_database.py
```

This will:
1. Create a test database
2. Load players from CSV
3. Demonstrate change tracking
4. Show database statistics

## Database Inspection

You can inspect the database using SQLite tools:

```bash
# View database structure
sqlite3 ttbw_players.db ".schema"

# View current players
sqlite3 ttbw_players.db "SELECT * FROM current_players LIMIT 5;"

# View recent changes
sqlite3 ttbw_players.db "SELECT * FROM player_history ORDER BY changed_at DESC LIMIT 10;"

# View club changes
sqlite3 ttbw_players.db "SELECT first_name, last_name, previous_club, club, changed_at FROM player_history WHERE change_type='UPDATE' AND previous_club != club ORDER BY changed_at DESC LIMIT 10;"
```

## Migration from Old System

The new system is backward compatible:
1. Old CSV processing still works
2. New database functionality is added transparently
3. Existing output files are generated as before
4. Database is created automatically on first run

## Database Reuse with Different Configs

The system is designed to work with the same database across different configurations:

1. **Load all players** from CSV into database (regardless of age)
2. **Process tournaments** with different age class configurations
3. **Same database** can be used for:
   - Youth tournaments (filtering by age)
   - Senior tournaments (different age classes)
   - Mixed age tournaments
4. **Age filtering** is applied dynamically based on current config
5. **No data loss** when switching between configurations

### Fuzzy Name Matching

The system now includes intelligent fuzzy matching for common name variations:

- **Common Variants**: Handles variations like "Marc" vs "Mark", "Mike" vs "Michael", "Jenny" vs "Jennifer"
- **Automatic Detection**: Automatically detects when fuzzy matching is used
- **Comprehensive Logging**: All fuzzy matches are logged and reported
- **Quality Control**: Fuzzy matches are clearly identified in reports for review

**Supported Name Variations:**
- Marc ↔ Mark
- Luis ↔ Louis
- Michael ↔ Mike ↔ Michal  
- Christopher ↔ Chris ↔ Kristopher
- Nicholas ↔ Nick ↔ Niklas
- Alexander ↔ Alex
- Daniel ↔ Dan ↔ Danny
- Matthew ↔ Matt ↔ Matthias
- Andrew ↔ Andy ↔ Andreas
- Jennifer ↔ Jenny ↔ Jen
- Elizabeth ↔ Liz ↔ Lizzy ↔ Beth
- Katherine ↔ Kate ↔ Kathy ↔ Katie
- Margaret ↔ Maggie ↔ Meg ↔ Peggy

**Encoding Variations Handled:**
- D´Elia ↔ D?Elia ↔ D'Elia ↔ Delia (smart quotes, question marks, apostrophes)
- Löwe ↔ Loewe (umlauts to standard characters)
- Ö ↔ Oe, Ü ↔ Ue, Ä ↔ Ae, ß ↔ ss

This feature helps resolve common data entry inconsistencies between tournament results and player databases.

### Historical Data Matching

The system automatically searches historical player data when current records don't match:

- **CSV Updates**: When `spielberechtigungen.csv` is updated with new information (e.g., name changes, club changes)
- **History Preservation**: Old information is preserved in `player_history` table
- **Automatic Fallback**: If a player isn't found in current records, the system searches historical data
- **Name Change Resolution**: Handles cases like "Frieda Löwe" → "Frieda Richter" automatically

**Example**: Tournament shows "Frieda Löwe" but CSV was updated to "Frieda Richter"
1. Current database has "Frieda Richter" 
2. History table contains "Frieda Löwe" entry
3. System automatically matches tournament "Frieda Löwe" to historical record
4. Player is successfully processed using current information

## Configuration

The database system automatically reads configuration from `config.yaml`:

### Age Classes
```yaml
age_classes:
  2006: 19
  2007: 19
  2008: 19
  2009: 19
  2010: 15
  2011: 15
  2012: 13
  2013: 13
  2014: 11
```

### Districts and Regions
```yaml
districts:
  Heilbronn:
    region: 1
    short_name: "HN"
  Ludwigsburg:
    region: 1
    short_name: "LB"
  Hochschwarzwald:
    region: 5
    short_name: "HS"
  # ... more districts
```

The system automatically:
- Maps birth years to age classes based on config
- Maps district names to region numbers based on config
- Falls back to sensible defaults if config is missing
- Supports partial district name matching for flexibility

## Troubleshooting

### Common Issues

1. **Database locked**: Ensure no other processes are using the database
2. **Permission errors**: Check file permissions in the working directory
3. **CSV encoding issues**: The system uses Latin-1 encoding by default

### Logging

The database system includes comprehensive logging:
- Player additions/updates
- Change tracking
- Error conditions
- Performance statistics

Check the console output for detailed information during operation.
