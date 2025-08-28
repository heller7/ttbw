"""
Core database management for the TTBW system.
"""

import sqlite3
import logging
from typing import Dict, Any
from config.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages core database operations and initialization."""
    
    def __init__(self, db_path: str = "ttbw_players.db", config_file: str = "config.yaml"):
        self.db_path = db_path
        self.config = ConfigManager.load_config(config_file)
        self.init_database()
    
    def init_database(self) -> None:
        """Initialize the database with required tables."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Create current players table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS current_players (
                    interne_lizenznr TEXT PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    club TEXT NOT NULL,
                    gender TEXT NOT NULL,
                    district TEXT NOT NULL,
                    birth_year INTEGER NOT NULL,
                    age_class INTEGER NOT NULL,
                    region INTEGER NOT NULL,
                    qttr INTEGER,
                    club_number TEXT,
                    verband TEXT DEFAULT 'TTBW',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create player history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS player_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    interne_lizenznr TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    club TEXT NOT NULL,
                    gender TEXT NOT NULL,
                    district TEXT NOT NULL,
                    birth_year INTEGER NOT NULL,
                    age_class INTEGER NOT NULL,
                    region INTEGER NOT NULL,
                    qttr INTEGER,
                    club_number TEXT,
                    verband TEXT DEFAULT 'TTBW',
                    change_type TEXT NOT NULL,
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    previous_club TEXT,
                    previous_district TEXT
                )
            """)

            # Create fuzzy matches table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fuzzy_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tournament_name TEXT NOT NULL,
                    tournament_first TEXT NOT NULL,
                    tournament_last TEXT NOT NULL,
                    tournament_club TEXT NOT NULL,
                    db_first TEXT NOT NULL,
                    db_last TEXT NOT NULL,
                    db_club TEXT NOT NULL,
                    old_club TEXT,
                    current_club TEXT,
                    match_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            logger.info("Database initialized successfully")
    
    def add_unique_constraint_to_history(self) -> None:
        """Add a unique constraint to the player_history table to prevent future duplicates."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            try:
                # Add unique constraint on the combination of fields that should be unique
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_history 
                    ON player_history(
                        interne_lizenznr, first_name, last_name, club, gender, district,
                        birth_year, age_class, region, COALESCE(qttr, ''), COALESCE(club_number, ''),
                        verband, change_type, COALESCE(previous_club, ''), COALESCE(previous_district, '')
                    )
                """)
                
                conn.commit()
                logger.info("Added unique constraint to player_history table")
                
            except Exception as e:
                logger.warning(f"Could not add unique constraint: {e}")
                # If constraint creation fails, we'll still have duplicate prevention in the code
    
    def get_connection(self):
        """Get a database connection."""
        return sqlite3.connect(self.db_path)
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get basic database statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Count current players
            cursor.execute("SELECT COUNT(*) FROM current_players")
            current_players = cursor.fetchone()[0]
            
            # Count history records
            cursor.execute("SELECT COUNT(*) FROM player_history")
            history_records = cursor.fetchone()[0]
            
            # Count fuzzy matches
            cursor.execute("SELECT COUNT(*) FROM fuzzy_matches")
            fuzzy_matches = cursor.fetchone()[0]
            
            return {
                'current_players': current_players,
                'history_records': history_records,
                'fuzzy_matches': fuzzy_matches
            }
