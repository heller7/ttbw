#!/usr/bin/env python3
"""
TTBW Database Module

This module handles SQLite database operations for tracking player changes over time.
It maintains current player records and historical changes for audit purposes.
"""

import sqlite3
import pandas as pd
import yaml
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class PlayerRecord:
    """Database record for a player."""
    interne_lizenznr: str
    first_name: str
    last_name: str
    club: str
    gender: str
    district: str
    birth_year: int
    age_class: int
    region: int
    qttr: Optional[int] = None
    club_number: Optional[str] = None
    verband: str = "TTBW"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TTBWDatabase:
    """SQLite database manager for TTBW player data."""

    def __init__(self, db_path: str = "ttbw_players.db", config_file: str = "config.yaml"):
        self.db_path = db_path
        self.config = self._load_config(config_file)
        self.init_database()

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Configuration file '{config_file}' not found. Using default configuration.")
            return self._get_default_config()
        except yaml.YAMLError as e:
            print(f"Error parsing configuration file: {e}. Using default configuration.")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration if config file is not available."""
        return {
            'default_birth_year': 2014,
            'age_classes': {
                2006: 19, 2007: 19, 2008: 19, 2009: 19,
                2010: 15, 2011: 15, 2012: 13, 2013: 13, 2014: 11
            },
            'districts': {
                'Hochschwarzwald': {'region': 1, 'short_name': 'HS'},
                'Ulm': {'region': 2, 'short_name': 'UL'},
                'Donau': {'region': 3, 'short_name': 'DO'},
                'Ludwigsburg': {'region': 4, 'short_name': 'LB'},
                'Stuttgart': {'region': 5, 'short_name': 'ST'}
            }
        }

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

            # Create historical changes table
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
                    change_type TEXT NOT NULL,  -- 'INSERT', 'UPDATE', 'DELETE'
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    previous_club TEXT,
                    previous_district TEXT
                )
            """)

            # Create indexes for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_current_players_name 
                ON current_players(last_name, first_name)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_current_players_club 
                ON current_players(club)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_history_lizenznr 
                ON player_history(interne_lizenznr)
            """)

            conn.commit()
            logger.info("Database initialized successfully")
            
            # Add unique constraint to history table to prevent duplicates
            self.add_unique_constraint_to_history()

    def load_players_from_csv(self, csv_file: str) -> int:
        """
        Load players from CSV file and update database.
        Returns the number of players processed.
        """
        try:
            df = pd.read_csv(csv_file, delimiter=';', encoding='latin1')
            logger.info(f"Loaded CSV with {len(df)} rows")

            players_processed = 0
            for index, row in df.iterrows():
                if self._process_csv_row(row):
                    players_processed += 1

            logger.info(f"Processed {players_processed} players from CSV")
            return players_processed

        except Exception as e:
            logger.error(f"Error loading CSV file: {e}")
            return 0

    def _process_csv_row(self, row: pd.Series) -> bool:
        """Process a single CSV row and update database."""
        try:
            # Extract values from the row
            verband = row.get('Verband', '')
            district = row.get('Region', '')
            club = row.get('VereinName', '')
            club_number = row.get('VereinNr', '')
            title = row.get('Anrede', '')
            last_name = row.get('Nachname', '')
            first_name = row.get('Vorname', '')
            birth_date = row.get('Geburtsdatum', '')
            interne_lizenznr = row.get('InterneNr', '')
            
            # Skip if essential fields are missing
            if pd.isna(last_name) or pd.isna(first_name) or pd.isna(interne_lizenznr) or pd.isna(birth_date):
                return False
            
            # Skip if not TTBW
            if verband != 'TTBW':
                return False
            
            # Extract birth year from birth date (assuming format DD.MM.YYYY)
            try:
                if isinstance(birth_date, str) and '.' in birth_date:
                    birth_year = int(birth_date.split('.')[-1])
                else:
                    birth_year = int(birth_date)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse birth date '{birth_date}' for player {first_name} {last_name}")
                return False
            
            # Note: We load ALL players into the database, regardless of age
            # Age filtering is applied later during tournament result processing
            
            # Determine age class
            age_class = self._calculate_age_class(birth_year)
            
            # Determine gender
            gender = "Jungen" if title == "Herr" else "Mädchen"
            
            # Create player record
            player_record = PlayerRecord(
                interne_lizenznr=str(interne_lizenznr),
                first_name=str(first_name),
                last_name=str(last_name),
                club=str(club),
                gender=gender,
                district=str(district),
                birth_year=birth_year,
                age_class=age_class,
                region=self._get_region_from_district(district),
                club_number=str(club_number) if not pd.isna(club_number) else None,
                verband=verband
            )
            
            # Update database
            self._update_player_in_database(player_record)
            return True
            
        except Exception as e:
            logger.error(f"Error processing row {row.get('InterneNr', 'unknown')}: {e}")
            return False
    
    def _is_player_age_eligible(self, birth_year: int) -> bool:
        """Check if player's birth year is within the eligible age range for tournament processing."""
        age_classes = self.config.get('age_classes', {})
        if not age_classes:
            return True  # If no age classes defined, accept all players
        
        # Find the oldest birth year that has an age class defined
        oldest_eligible_birth_year = min(age_classes.keys())
        
        # Player is eligible if their birth year is >= oldest eligible birth year
        # (i.e., not older than the oldest defined age class)
        return birth_year >= oldest_eligible_birth_year
    
    def _calculate_age_class(self, birth_year: int) -> int:
        """Calculate age class based on birth year from config."""
        age_class_mapping = self.config.get('age_classes', {})
        default_age_class = age_class_mapping.get(self.config.get('default_birth_year', 2014), 11)
        return age_class_mapping.get(birth_year, default_age_class)

    def _get_region_from_district(self, district: str) -> int:
        """Get region number from district name from config."""
        if district is None:
            return 1  # Default fallback for None
        
        districts_config = self.config.get('districts', {})

        # Try to find the district in the config
        for district_name, district_info in districts_config.items():
            if district_name.lower() == district.lower():
                return district_info.get('region', 1)

        # If no exact match, try partial matching
        for district_name, district_info in districts_config.items():
            if (district_name.lower() in district.lower() or
                    district.lower() in district_name.lower()):
                return district_info.get('region', 1)

        # Default to region 1 if no match found
        return 1

    def _update_player_in_database(self, player_record: PlayerRecord) -> None:
        """Update player record in database, tracking changes."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Check if player exists
            cursor.execute("""
                SELECT * FROM current_players WHERE interne_lizenznr = ?
            """, (player_record.interne_lizenznr,))

            existing_player = cursor.fetchone()

            if existing_player:
                # Player exists, check for changes
                if self._has_changes(existing_player, player_record):
                    # Record the change in history
                    self._record_change(cursor, existing_player, player_record, 'UPDATE')

                    # Update current record
                    cursor.execute("""
                        UPDATE current_players SET
                            first_name = ?, last_name = ?, club = ?, gender = ?,
                            district = ?, birth_year = ?, age_class = ?, region = ?,
                            qttr = ?, club_number = ?, verband = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE interne_lizenznr = ?
                    """, (
                        player_record.first_name, player_record.last_name, player_record.club,
                        player_record.gender, player_record.district, player_record.birth_year,
                        player_record.age_class, player_record.region, player_record.qttr,
                        player_record.club_number, player_record.verband,
                        player_record.interne_lizenznr
                    ))
                    logger.info(f"Updated player {player_record.first_name} {player_record.last_name}")
                else:
                    logger.debug(f"No changes for player {player_record.first_name} {player_record.last_name}")
            else:
                # New player
                cursor.execute("""
                    INSERT INTO current_players (
                        interne_lizenznr, first_name, last_name, club, gender, district,
                        birth_year, age_class, region, qttr, club_number, verband
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    player_record.interne_lizenznr, player_record.first_name, player_record.last_name,
                    player_record.club, player_record.gender, player_record.district,
                    player_record.birth_year, player_record.age_class, player_record.region,
                    player_record.qttr, player_record.club_number, player_record.verband
                ))

                # Record the insertion
                self._record_change(cursor, None, player_record, 'INSERT')
                logger.info(f"Added new player {player_record.first_name} {player_record.last_name}")

            conn.commit()

    def _has_changes(self, existing_player: Tuple, new_record: PlayerRecord) -> bool:
        """Check if there are changes between existing and new player record."""
        # existing_player is a tuple from database query
        # Compare relevant fields (excluding timestamps and auto-increment fields)
        return (
                existing_player[1] != new_record.first_name or  # first_name
                existing_player[2] != new_record.last_name or  # last_name
                existing_player[3] != new_record.club or  # club
                existing_player[4] != new_record.gender or  # gender
                existing_player[5] != new_record.district or  # district
                existing_player[6] != new_record.birth_year or  # birth_year
                existing_player[7] != new_record.age_class or  # age_class
                existing_player[8] != new_record.region or  # region
                existing_player[9] != new_record.qttr or  # qttr
                existing_player[10] != new_record.club_number  # club_number
        )

    def _record_change(self, cursor: sqlite3.Cursor, old_record: Optional[Tuple],
                       new_record: PlayerRecord, change_type: str) -> None:
        """Record a change in the history table."""
        previous_club = old_record[3] if old_record else None
        previous_district = old_record[5] if old_record else None

        # Check if this exact change already exists to prevent duplicates
        cursor.execute("""
            SELECT COUNT(*) FROM player_history 
            WHERE interne_lizenznr = ? 
            AND first_name = ? 
            AND last_name = ? 
            AND club = ? 
            AND gender = ? 
            AND district = ? 
            AND birth_year = ? 
            AND age_class = ? 
            AND region = ? 
            AND COALESCE(qttr, '') = COALESCE(?, '')
            AND COALESCE(club_number, '') = COALESCE(?, '')
            AND verband = ? 
            AND change_type = ? 
            AND COALESCE(previous_club, '') = COALESCE(?, '')
            AND COALESCE(previous_district, '') = COALESCE(?, '')
        """, (
            new_record.interne_lizenznr, new_record.first_name, new_record.last_name,
            new_record.club, new_record.gender, new_record.district,
            new_record.birth_year, new_record.age_class, new_record.region,
            new_record.qttr, new_record.club_number, new_record.verband,
            change_type, previous_club, previous_district
        ))

        if cursor.fetchone()[0] == 0:
            # Only insert if this change doesn't already exist
            cursor.execute("""
                INSERT INTO player_history (
                    interne_lizenznr, first_name, last_name, club, gender, district,
                    birth_year, age_class, region, qttr, club_number, verband,
                    change_type, previous_club, previous_district
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                new_record.interne_lizenznr, new_record.first_name, new_record.last_name,
                new_record.club, new_record.gender, new_record.district,
                new_record.birth_year, new_record.age_class, new_record.region,
                new_record.qttr, new_record.club_number, new_record.verband,
                change_type, previous_club, previous_district
            ))
        else:
            logger.debug(f"Skipping duplicate change record for {new_record.first_name} {new_record.last_name}")

    def _get_name_variants(self, name: str) -> List[str]:
        """Get common name variants for fuzzy matching."""
        if name is None:
            return []
        name = name.lower().strip()
        variants = [name]  # Always include the original name
        
        # Common name variations
        name_variants = {
            'marc': ['mark'],
            'mark': ['marc'],
            'luis': ['louis'],
            'louis': ['luis'],
            'mukherjee': ['mukherjee'],  # Keep as is for now
            'd´elia': ['d?elia', 'd\'elia', 'delia'],  # Handle encoding variations
            'd?elia': ['d´elia', 'd\'elia', 'delia'],
            'd\'elia': ['d´elia', 'd?elia', 'delia'],
            'delia': ['d´elia', 'd?elia', 'd\'elia'],
            'kleiss': ['kleiss'],  # Keep as is for now
            'löwe': ['löwe', 'loewe'],  # Handle umlaut variations
            'loewe': ['löwe'],
            'titus': ['titus'],  # Keep as is for now
            'kleiss': ['kleiß'],  # Keep as is for now
            'kleis': ['kleiß'],  # Keep as is for now
            'kleiß': ['kleiss', 'kleis']  # Keep as is for now
        }
        
        if name in name_variants:
            variants.extend(name_variants[name])
        
        # Add encoding-normalized variants
        normalized_name = self._normalize_encoding(name)
        if normalized_name != name and normalized_name not in variants:
            variants.append(normalized_name)
        
        return variants

    def _normalize_encoding(self, name: str) -> str:
        """Normalize common encoding variations in names."""
        # Handle common encoding issues
        encoding_variants = {
            'd´elia': 'delia',      # Smart quote to regular apostrophe
            'd?elia': 'delia',      # Question mark to regular apostrophe
            'd\'elia': 'delia',     # Regular apostrophe
            'd´': 'd\'',            # Smart quote to regular apostrophe
            'd?': 'd\'',            # Question mark to regular apostrophe
            'löwe': 'loewe',        # Umlaut to oe
            'ö': 'oe',              # Umlaut to oe
            'ü': 'ue',              # Umlaut to ue
            'ä': 'ae',              # Umlaut to ae
            'ß': 'ss'               # Sharp s to ss
        }
        
        normalized = name
        for variant, standard in encoding_variants.items():
            normalized = normalized.replace(variant, standard)
        
        return normalized

    def find_player_by_name_and_club(self, first_name: str, last_name: str,
                                     club: str, club_number: Optional[str] = None) -> Optional[str]:
        """
        Find a player by name and club information.
        Returns the interne_lizenznr if found, None otherwise.
        Only returns players who are age-eligible.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Try to find by exact name and club match (with age eligibility check)
            cursor.execute("""
                SELECT interne_lizenznr, birth_year FROM current_players 
                WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                AND LOWER(TRIM(club)) = LOWER(TRIM(?))
            """, (first_name, last_name, club))

            result = cursor.fetchone()
            if result:
                player_id, birth_year = result
                # Check age eligibility before returning
                if self._is_player_age_eligible(birth_year):
                    return player_id
                else:
                    logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")

            # If club number is provided, try matching by name and club number
            if club_number:
                # First try matching by name and club number
                cursor.execute("""
                    SELECT interne_lizenznr, birth_year FROM current_players 
                    WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                    AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                    AND club_number = ?
                """, (first_name, last_name, club_number))

                result = cursor.fetchone()
                if result:
                    player_id, birth_year = result
                    # Check age eligibility before returning
                    if self._is_player_age_eligible(birth_year):
                        return player_id
                    else:
                        logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")
                
                # If club number looks like a license ID, try matching by license ID
                if len(club_number) >= 8:  # License IDs are typically 8+ characters
                    cursor.execute("""
                        SELECT interne_lizenznr, first_name, last_name, club, birth_year FROM current_players 
                        WHERE interne_lizenznr = ?
                    """, (club_number,))

                    result = cursor.fetchone()
                    if result:
                        player_id, db_first_name, db_last_name, club_name, birth_year = result
                        # Check age eligibility before returning
                        if self._is_player_age_eligible(birth_year):
                            logger.info(f"LICENSE ID MATCH: Found player by license ID {club_number}: {db_first_name} {db_last_name} (tournament: {first_name} {last_name})")
                            # Log the fuzzy match for reporting
                            self._log_fuzzy_match(
                                tournament_name="",  # We don't have tournament name in this context
                                db_name=f"{db_first_name} {db_last_name}",
                                tournament_club=club,
                                db_club=club_name,
                                tournament_first=first_name,
                                tournament_last=last_name,
                                db_first=db_first_name,
                                db_last=last_name
                            )
                            return player_id
                        else:
                            logger.debug(f"Player {db_first_name} {db_last_name} (birth year {birth_year}) is too old for age classes")

            # Try fuzzy matching by name only (in case club has changed)
            cursor.execute("""
                SELECT interne_lizenznr, club, birth_year FROM current_players 
                WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
            """, (first_name, last_name))

            results = cursor.fetchall()
            if len(results) == 1:
                player_id, club_name, birth_year = results[0]
                # Check age eligibility before returning
                if self._is_player_age_eligible(birth_year):
                    return player_id
                else:
                    logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")
            elif len(results) > 1:
                # Multiple matches, find the first age-eligible one
                for player_id, club_name, birth_year in results:
                    if self._is_player_age_eligible(birth_year):
                        logger.debug(f"Multiple players found for {first_name} {last_name}, using age-eligible one: {player_id}")
                        return player_id
                
                # If no age-eligible players found, log warning
                logger.warning(f"Multiple players found for {first_name} {last_name}, but none are age-eligible: {results}")

            # Try fuzzy name matching with common variants
            first_name_variants = self._get_name_variants(first_name)
            last_name_variants = self._get_name_variants(last_name)
            
            # Try matching with exact names but fuzzy club matching (for club name variations)
            cursor.execute("""
                SELECT interne_lizenznr, club, birth_year FROM current_players 
                WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
            """, (first_name, last_name))

            results = cursor.fetchall()
            if len(results) == 1:
                player_id, club_name, birth_year = results[0]
                # Check age eligibility before returning
                if self._is_player_age_eligible(birth_year):
                    logger.info(f"FUZZY CLUB MATCH: Tournament club '{club}' matched to DB club '{club_name}' for {first_name} {last_name}")
                    # Log the fuzzy match for reporting
                    self._log_fuzzy_match(
                        tournament_name="",  # We don't have tournament name in this context
                        db_name=f"{first_name} {last_name}",
                        tournament_club=club,
                        db_club=club_name,
                        tournament_first=first_name,
                        tournament_last=last_name,
                        db_first=first_name,
                        db_last=last_name
                    )
                    return player_id
                else:
                    logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")
            elif len(results) > 1:
                # Multiple matches, find the first age-eligible one
                for player_id, club_name, birth_year in results:
                    if self._is_player_age_eligible(birth_year):
                        logger.info(f"FUZZY CLUB MATCH: Tournament club '{club}' matched to DB club '{club_name}' for {first_name} {last_name} (multiple matches, using age-eligible one)")
                        # Log the fuzzy match for reporting
                        self._log_fuzzy_match(
                            tournament_name="",  # We don't have tournament name in this context
                            db_name=f"{first_name} {last_name}",
                            tournament_club=club,
                            db_club=club_name,
                            tournament_first=first_name,
                            tournament_last=last_name,
                            db_first=first_name,
                            db_last=last_name
                        )
                        return player_id
            
            # Try matching with first name variants
            for variant in first_name_variants:
                if variant != first_name.lower().strip():  # Skip the original name (already tried)
                    cursor.execute("""
                        SELECT interne_lizenznr, club, birth_year, first_name FROM current_players 
                        WHERE LOWER(TRIM(first_name)) = ? 
                        AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                        AND LOWER(TRIM(club)) = LOWER(TRIM(?))
                    """, (variant, last_name, club))

                    result = cursor.fetchone()
                    if result:
                        player_id, club_name, birth_year, db_first_name = result
                        # Check age eligibility before returning
                        if self._is_player_age_eligible(birth_year):
                            logger.info(f"FUZZY MATCH: Tournament '{first_name}' matched to DB '{db_first_name}' for {first_name} {last_name} from {club}")
                            # Log the fuzzy match for reporting
                            self._log_fuzzy_match(
                                tournament_name="",  # We don't have tournament name in this context
                                db_name=f"{db_first_name} {last_name}",
                                tournament_club=club,
                                db_club=club_name,
                                tournament_first=first_name,
                                tournament_last=last_name,
                                db_first=db_first_name,
                                db_last=last_name
                            )
                            return player_id
                        else:
                            logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")

            # Try matching with last name variants
            for variant in last_name_variants:
                if variant != last_name.lower().strip():  # Skip the original name (already tried)
                    cursor.execute("""
                        SELECT interne_lizenznr, club, birth_year, last_name FROM current_players 
                        WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                        AND LOWER(TRIM(last_name)) = ?
                        AND LOWER(TRIM(club)) = LOWER(TRIM(?))
                    """, (first_name, variant, club))

                    result = cursor.fetchone()
                    if result:
                        player_id, club_name, birth_year, db_last_name = result
                        # Check age eligibility before returning
                        if self._is_player_age_eligible(birth_year):
                            logger.info(f"FUZZY MATCH: Tournament '{last_name}' matched to DB '{db_last_name}' for {first_name} {last_name} from {club}")
                            # Log the fuzzy match for reporting
                            self._log_fuzzy_match(
                                tournament_name="",  # We don't have tournament name in this context
                                db_name=f"{first_name} {db_last_name}",
                                tournament_club=club,
                                db_club=club_name,
                                tournament_first=first_name,
                                tournament_last=last_name,
                                db_first=first_name,
                                db_last=db_last_name
                            )
                            return player_id
                        else:
                            logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")

            # Try fuzzy matching by name variants only (in case club has changed)
            for variant in first_name_variants:
                if variant != first_name.lower().strip():  # Skip the original name (already tried)
                    cursor.execute("""
                        SELECT interne_lizenznr, club, birth_year, first_name FROM current_players 
                        WHERE LOWER(TRIM(first_name)) = ? 
                        AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                    """, (variant, last_name))

                    results = cursor.fetchall()
                    if len(results) == 1:
                        player_id, club_name, birth_year, db_first_name = results[0]
                        # Check age eligibility before returning
                        if self._is_player_age_eligible(birth_year):
                            logger.info(f"FUZZY MATCH: Tournament '{first_name}' matched to DB '{db_first_name}' for {first_name} {last_name} (club: tournament={club}, DB={club_name})")
                            # Log the fuzzy match for reporting
                            self._log_fuzzy_match(
                                tournament_name="",  # We don't have tournament name in this context
                                db_name=f"{db_first_name} {last_name}",
                                tournament_club=club,
                                db_club=club_name,
                                tournament_first=first_name,
                                tournament_last=last_name,
                                db_first=db_first_name,
                                db_last=last_name
                            )
                            return player_id
                        else:
                            logger.debug(f"Player {first_name} {last_name} (birth year {birth_year}) is too old for age classes")
                    elif len(results) > 1:
                        # Multiple matches, find the first age-eligible one
                        for player_id, club_name, birth_year, db_first_name in results:
                            if self._is_player_age_eligible(birth_year):
                                logger.info(f"FUZZY MATCH: Tournament '{first_name}' matched to DB '{db_first_name}' for {first_name} {last_name} (multiple matches, using age-eligible one)")
                                # Log the fuzzy match for reporting
                                self._log_fuzzy_match(
                                    tournament_name="",  # We don't have tournament name in this context
                                    db_name=f"{db_first_name} {last_name}",
                                    tournament_club=club,
                                    db_club=club_name,
                                    tournament_first=first_name,
                                    tournament_last=last_name,
                                    db_first=db_first_name,
                                    db_last=last_name
                                )
                                return player_id

            # Try to find by license ID if club number is provided and looks like a license ID
            if club_number and len(club_number) >= 8:  # License IDs are typically 8+ characters
                cursor.execute("""
                    SELECT interne_lizenznr, first_name, last_name, club, birth_year FROM current_players 
                    WHERE interne_lizenznr = ?
                """, (club_number,))

                result = cursor.fetchone()
                if result:
                    player_id, db_first_name, db_last_name, club_name, birth_year = result
                    # Check age eligibility before returning
                    if self._is_player_age_eligible(birth_year):
                        logger.info(f"LICENSE ID MATCH: Found player by license ID {club_number}: {db_first_name} {db_last_name} (tournament: {first_name} {last_name})")
                        # Log the fuzzy match for reporting
                        self._log_fuzzy_match(
                            tournament_name="",  # We don't have tournament name in this context
                            db_name=f"{db_first_name} {db_last_name}",
                            tournament_club=club,
                            db_club=club_name,
                            tournament_first=first_name,
                            tournament_last=last_name,
                            db_first=db_first_name,
                            db_last=db_last_name
                        )
                        return player_id
                    else:
                        logger.debug(f"Player {db_first_name} {db_last_name} (birth year {birth_year}) is too old for age classes")

            # If no match found in current_players, search the history table
            # This handles cases where CSV was updated and old names are in history
            cursor.execute("""
                SELECT interne_lizenznr, first_name, last_name, club, birth_year, gender, district, age_class, region
                FROM player_history 
                WHERE LOWER(TRIM(first_name)) = LOWER(TRIM(?)) 
                AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                AND LOWER(TRIM(club)) = LOWER(TRIM(?))
                ORDER BY changed_at DESC
                LIMIT 1
            """, (first_name, last_name, club))

            result = cursor.fetchone()
            if result:
                player_id, db_first_name, db_last_name, club_name, birth_year, gender, district, age_class, region = result
                # Check age eligibility before returning
                if self._is_player_age_eligible(birth_year):
                    logger.info(f"HISTORY MATCH: Found player in history: {db_first_name} {db_last_name} (tournament: {first_name} {last_name})")
                    # Log the fuzzy match for reporting
                    self._log_fuzzy_match(
                        tournament_name="",  # We don't have tournament name in this context
                        db_name=f"{db_first_name} {db_last_name}",
                        tournament_club=club,
                        db_club=club_name,
                        tournament_first=first_name,
                        tournament_last=last_name,
                        db_first=db_first_name,
                        db_last=db_last_name
                    )
                    return player_id
                else:
                    logger.debug(f"Player {db_first_name} {db_last_name} (birth year {birth_year}) is too old for age classes")

            # If no match found in current_players, search the history table with fuzzy name matching
            # This handles cases where CSV was updated and old names are in history
            for variant in first_name_variants:
                if variant != first_name.lower().strip():  # Skip the original name (already tried)
                    cursor.execute("""
                        SELECT interne_lizenznr, first_name, last_name, club, birth_year, gender, district, age_class, region
                        FROM player_history 
                        WHERE LOWER(TRIM(first_name)) = ? 
                        AND LOWER(TRIM(last_name)) = LOWER(TRIM(?))
                        AND LOWER(TRIM(club)) = LOWER(TRIM(?))
                        ORDER BY changed_at DESC
                        LIMIT 1
                    """, (variant, last_name, club))

                    result = cursor.fetchone()
                    if result:
                        player_id, db_first_name, db_last_name, club_name, birth_year, gender, district, age_class, region = result
                        # Check age eligibility before returning
                        if self._is_player_age_eligible(birth_year):
                            logger.info(f"HISTORY FUZZY MATCH: Tournament '{first_name}' matched to history '{db_first_name}' for {first_name} {last_name} from {club}")
                            # Log the fuzzy match for reporting
                            self._log_fuzzy_match(
                                tournament_name="",  # We don't have tournament name in this context
                                db_name=f"{db_first_name} {db_last_name}",
                                tournament_club=club,
                                db_club=club_name,
                                tournament_first=first_name,
                                tournament_last=last_name,
                                db_first=db_first_name,
                                db_last=last_name
                            )
                            return player_id
                        else:
                            logger.debug(f"Player {db_first_name} {db_last_name} (birth year {birth_year}) is too old for age classes")

            # Check if the club exists in the database at all
            cursor.execute("""
                SELECT COUNT(*) FROM current_players WHERE LOWER(TRIM(club)) = LOWER(TRIM(?))
            """, (club,))
            
            club_exists = cursor.fetchone()[0] > 0
            
            if not club_exists:
                logger.warning(f"CLUB NOT FOUND: Club '{club}' is not in the database - likely not part of considered regions")
                return None

            return None

    def club_exists(self, club_name: str) -> bool:
        """Check if a club exists in the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM current_players WHERE LOWER(TRIM(club)) = LOWER(TRIM(?))
            """, (club_name,))
            return cursor.fetchone()[0] > 0

    def cleanup_duplicate_history(self) -> int:
        """Remove duplicate rows from the player_history table. Returns number of duplicates removed."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create a temporary table with unique records
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_unique_history AS
                SELECT MIN(id) as id, interne_lizenznr, first_name, last_name, club, gender, district,
                       birth_year, age_class, region, qttr, club_number, verband, change_type,
                       changed_at, previous_club, previous_district
                FROM player_history
                GROUP BY interne_lizenznr, first_name, last_name, club, gender, district,
                         birth_year, age_class, region, COALESCE(qttr, ''), COALESCE(club_number, ''),
                         verband, change_type, COALESCE(previous_club, ''), COALESCE(previous_district, '')
            """)
            
            # Count duplicates
            cursor.execute("SELECT COUNT(*) FROM player_history")
            total_before = cursor.fetchone()[0]
            
            # Replace the original table with deduplicated data
            cursor.execute("DELETE FROM player_history")
            cursor.execute("""
                INSERT INTO player_history 
                SELECT * FROM temp_unique_history
            """)
            
            # Drop temporary table
            cursor.execute("DROP TABLE temp_unique_history")
            
            # Count after cleanup
            cursor.execute("SELECT COUNT(*) FROM player_history")
            total_after = cursor.fetchone()[0]
            
            duplicates_removed = total_before - total_after
            conn.commit()
            
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicate history records")
            else:
                logger.info("No duplicate history records found")
            
            return duplicates_removed

    def _get_connection(self):
        """Get a database connection."""
        return sqlite3.connect(self.db_path)

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

    def get_fuzzy_matches_summary(self) -> List[Dict[str, str]]:
        """Get a summary of all fuzzy matches that occurred during processing."""
        # This will be populated during fuzzy matching operations
        if not hasattr(self, '_fuzzy_matches'):
            self._fuzzy_matches = []
        return self._fuzzy_matches

    def _log_fuzzy_match(self, tournament_name: str, db_name: str, tournament_club: str, db_club: str, 
                         tournament_first: str, tournament_last: str, db_first: str, db_last: str,
                         old_club: Optional[str] = None, current_club: Optional[str] = None) -> None:
        """Log a fuzzy match for reporting purposes."""
        if not hasattr(self, '_fuzzy_matches'):
            self._fuzzy_matches = []
        
        self._fuzzy_matches.append({
            'tournament_name': tournament_name,
            'db_name': db_name,
            'tournament_club': tournament_club,
            'db_club': db_club,
            'tournament_first': tournament_first,
            'tournament_last': tournament_last,
            'db_first': db_first,
            'db_last': db_last,
            'old_club': old_club,
            'current_club': current_club
        })

    def get_player_history(self, interne_lizenznr: str) -> List[Dict]:
        """Get complete history for a player."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT * FROM player_history 
                WHERE interne_lizenznr = ? 
                ORDER BY changed_at DESC
            """, (interne_lizenznr,))

            columns = [description[0] for description in cursor.description]
            history = []

            for row in cursor.fetchall():
                history.append(dict(zip(columns, row)))

            return history

    def get_all_current_players(self) -> List[PlayerRecord]:
        """Get all current players from database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM current_players")

            players = []
            for row in cursor.fetchall():
                player = PlayerRecord(
                    interne_lizenznr=row[0],
                    first_name=row[1],
                    last_name=row[2],
                    club=row[3],
                    gender=row[4],
                    district=row[5],
                    birth_year=row[6],
                    age_class=row[7],
                    region=row[8],
                    qttr=row[9],
                    club_number=row[10],
                    verband=row[11],
                    created_at=row[12],
                    updated_at=row[13]
                )
                players.append(player)

            return players

    def get_player_by_lizenznr(self, interne_lizenznr: str) -> Optional[PlayerRecord]:
        """Get a specific player by their internal license number."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM current_players WHERE interne_lizenznr = ?", (interne_lizenznr,))
            row = cursor.fetchone()

            if row:
                return PlayerRecord(
                    interne_lizenznr=row[0],
                    first_name=row[1],
                    last_name=row[2],
                    club=row[3],
                    gender=row[4],
                    district=row[5],
                    birth_year=row[6],
                    age_class=row[7],
                    region=row[8],
                    qttr=row[9],
                    club_number=row[10],
                    verband=row[11],
                    created_at=row[12],
                    updated_at=row[13]
                )

            return None

    def get_database_stats(self) -> Dict[str, int]:
        """Get database statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM current_players")
            current_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM player_history")
            history_count = cursor.fetchone()[0]
            
            # Get age eligibility statistics for tournament processing
            age_classes = self.config.get('age_classes', {})
            if age_classes:
                oldest_eligible_birth_year = min(age_classes.keys())
                cursor.execute("SELECT COUNT(*) FROM current_players WHERE birth_year < ?", (oldest_eligible_birth_year,))
                too_old_for_tournaments_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM current_players WHERE birth_year >= ?", (oldest_eligible_birth_year,))
                eligible_for_tournaments_count = cursor.fetchone()[0]
            else:
                too_old_for_tournaments_count = 0
                eligible_for_tournaments_count = current_count
                oldest_eligible_birth_year = None
            
            return {
                'current_players': current_count,
                'history_records': history_count,
                'too_old_for_tournaments': too_old_for_tournaments_count,
                'eligible_for_tournaments': eligible_for_tournaments_count,
                'oldest_eligible_birth_year': oldest_eligible_birth_year
            }
